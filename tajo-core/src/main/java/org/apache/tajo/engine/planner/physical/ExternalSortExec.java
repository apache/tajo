/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.physical;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.PhysicalPlanningException;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.RawFile;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.offheap.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

/**
 * This external sort algorithm can be characterized by the followings:
 *
 * <ul>
 *   <li>in-memory sort if input data size fits a sort buffer</li>
 *   <li>k-way merge sort if input data size exceeds the size of sort buffer</li>
 *   <li>parallel merge</li>
 *   <li>final merge avoidance</li>
 *   <li>Unbalance merge if needed</li>
 * </ul>
 */
public class ExternalSortExec extends SortExec {
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(ExternalSortExec.class);

  private SortNode plan;
  private final TableMeta meta;
  /** the defaultFanout of external sort */
  private final int defaultFanout;
  /** It's the size of in-memory table. If memory consumption exceeds it, store the memory table into a disk. */
  private long sortBufferBytesNum;
  /** the number of available cores */
  private final int allocatedCoreNum;
  /** If there are available multiple cores, it tries parallel merge. */
  private ExecutorService executorService;
  /** used for in-memory sort of each chunk. */
  private OffHeapRowBlock tupleBlock;
  private List<Tuple> sortedTuples;
  /** temporal dir */
  private final Path sortTmpDir;
  /** It enables round-robin disks allocation */
  private final LocalDirAllocator localDirAllocator;
  /** local file system */
  private final RawLocalFileSystem localFS;
  /** final output files which are used for cleaning */
  private List<Path> finalOutputFiles = null;
  /** for directly merging sorted inputs */
  private List<Path> mergedInputPaths = null;

  ///////////////////////////////////////////////////
  // transient variables
  ///////////////////////////////////////////////////
  /** already sorted or not */
  private boolean sorted = false;
  /** a flag to point whether sorted data resides in memory or not */
  private boolean memoryResident = true;
  /** the final result */
  private Scanner result;
  /** total bytes of input data */
  private long sortAndStoredBytes;

  private ExternalSortExec(final TaskAttemptContext context, final AbstractStorageManager sm, final SortNode plan)
      throws PhysicalPlanningException {
    super(context, plan.getInSchema(), plan.getOutSchema(), null, plan.getSortKeys());

    this.plan = plan;
    this.meta = CatalogUtil.newTableMeta(StoreType.DIRECTRAW);

    this.defaultFanout = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT);
    if (defaultFanout < 2) {
      throw new PhysicalPlanningException(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT.varname + " cannot be lower than 2");
    }
    // TODO - sort buffer and core num should be changed to use the allocated container resource.
    this.sortBufferBytesNum = context.getQueryContext().getLong(SessionVars.EXTSORT_BUFFER_SIZE) * StorageUnit.MB;
    this.allocatedCoreNum = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_THREAD_NUM);
    this.executorService = Executors.newFixedThreadPool(this.allocatedCoreNum);
    this.tupleBlock = new OffHeapRowBlock(inSchema, new FixedSizeLimitSpec(sortBufferBytesNum));

    this.sortTmpDir = getExecutorTmpDir();
    localDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    localFS = new RawLocalFileSystem();
  }

  public ExternalSortExec(final TaskAttemptContext context,
                          final AbstractStorageManager sm, final SortNode plan,
                          final CatalogProtos.FragmentProto[] fragments) throws PhysicalPlanningException {
    this(context, sm, plan);

    mergedInputPaths = TUtil.newList();
    for (CatalogProtos.FragmentProto proto : fragments) {
      FileFragment fragment = FragmentConvertor.convert(FileFragment.class, proto);
      mergedInputPaths.add(fragment.getPath());
    }
  }

  public ExternalSortExec(final TaskAttemptContext context,
                          final AbstractStorageManager sm, final SortNode plan, final PhysicalExec child)
      throws IOException {
    this(context, sm, plan);
    setChild(child);
  }

  @VisibleForTesting
  public void setSortBufferBytesNum(int sortBufferBytesNum) {
    this.sortBufferBytesNum = sortBufferBytesNum;
  }

  public void init() throws IOException {
    inputStats = new TableStats();
    super.init();
  }

  public SortNode getPlan() {
    return this.plan;
  }

  public static List<Tuple> sortTuples(OffHeapRowBlock sortBuffer, Comparator<Tuple> comparator) {
    List<Tuple> tupleList = Lists.newArrayList();
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    OffHeapRowBlockReader reader = new OffHeapRowBlockReader(sortBuffer);
    while(reader.next(zcTuple)) {
      tupleList.add(zcTuple);
      zcTuple = new ZeroCopyTuple();
    }
    Collections.sort(tupleList, comparator);
    return tupleList;
  }

  /**
   * Sort a tuple block and store them into a chunk file
   */
  private Path sortAndStoreChunk(int chunkId, OffHeapRowBlock sortBuffer)
      throws IOException {
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.DIRECTRAW);
    int rowNum = sortBuffer.rows();

    long sortStart = System.currentTimeMillis();
    List<Tuple> tupleList = sortTuples(sortBuffer, getComparator());
    long sortEnd = System.currentTimeMillis();

    long chunkWriteStart = System.currentTimeMillis();
    Path outputPath = getChunkPathForWrite(0, chunkId);
    final DirectRawFileWriter appender = new DirectRawFileWriter(context.getConf(), inSchema, meta, outputPath);
    appender.init();
    for (Tuple t : tupleList) {
      appender.addTuple(t);
    }
    appender.close();

    long chunkWriteEnd = System.currentTimeMillis();
    info(LOG, "Chunk #" + chunkId + " sort and written (" +
        FileUtil.humanReadableByteCount(appender.getOffset(), false) + " bytes, " + rowNum + " rows, " +
        ", sort time: " + (sortEnd - sortStart) + " msec, " +
        "write time: " + (chunkWriteEnd - chunkWriteStart) + " msec)");
    return outputPath;
  }

  /**
   * It divides all tuples into a number of chunks, then sort for each chunk.
   *
   * @return All paths of chunks
   * @throws java.io.IOException
   */
  private List<Path> sortAndStoreAllChunks() throws IOException {
    Tuple tuple;
    List<Path> chunkPaths = TUtil.newList();

    int chunkId = 0;
    long runStartTime = System.currentTimeMillis();
    while ((tuple = child.next()) != null) { // partition sort start
      tupleBlock.addTuple(tuple);

      if (tupleBlock.usedMem() > sortBufferBytesNum) {
        long runEndTime = System.currentTimeMillis();
        info(LOG, chunkId + " run loading time: " + (runEndTime - runStartTime) + " msec");
        runStartTime = runEndTime;

        info(LOG, "Memory consumption exceeds " + sortBufferBytesNum + " bytes");
        memoryResident = false;

        chunkPaths.add(sortAndStoreChunk(chunkId, tupleBlock));
        tupleBlock.clear();
        chunkId++;

        // When the volume of sorting data once exceed the size of sort buffer,
        // the total progress of this external sort is divided into two parts.
        // In contrast, if the data fits in memory, the progress is only one part.
        //
        // When the progress is divided into two parts, the first part sorts tuples on memory and stores them
        // into a chunk. The second part merges stored chunks into fewer chunks, and it continues until the number
        // of merged chunks is fewer than the default fanout.
        //
        // The fact that the code reach here means that the first chunk has been just stored.
        // That is, the progress was divided into two parts.
        // So, it multiply the progress of the children operator and 0.5f.
        progress = child.getProgress() * 0.5f;
      }
    }

    if (tupleBlock.rows() >= 0) { // if there are at least one or more input tuples
      if (!memoryResident) { // check if data exceeds a sort buffer. If so, it store the remain data into a chunk.
        if (tupleBlock.rows() > 0) {
          long start = System.currentTimeMillis();
          int rowNum = tupleBlock.rows();
          chunkPaths.add(sortAndStoreChunk(chunkId, tupleBlock));
          long end = System.currentTimeMillis();
          info(LOG, "Last Chunk #" + chunkId + " " + rowNum + " rows written (" + (end - start) + " msec)");
        }
      } else { // this case means that all data does not exceed a sort buffer
        sortedTuples = sortTuples(tupleBlock, getComparator());
      }
    }

    // get total loaded (or stored) bytes and total row numbers
    TableStats childTableStats = child.getInputStats();
    if (childTableStats != null) {
      sortAndStoredBytes = childTableStats.getNumBytes();
    }
    return chunkPaths;
  }

  /**
   * Get a local path from all temporal paths in round-robin manner.
   */
  private synchronized Path getChunkPathForWrite(int level, int chunkId) throws IOException {
    return localDirAllocator.getLocalPathForWrite(sortTmpDir + "/" + level +"_" + chunkId, context.getConf());
  }

  @Override
  public Tuple next() throws IOException {

    if (!sorted) { // if not sorted, first sort all data

      // if input files are given, it starts merging directly.
      if (mergedInputPaths != null) {
        try {
          this.result = externalMergeAndSort(mergedInputPaths);
        } catch (Exception e) {
          throw new PhysicalPlanningException(e);
        }
      } else {
        // Try to sort all data, and store them as multiple chunks if memory exceeds
        long startTimeOfChunkSplit = System.currentTimeMillis();
        List<Path> chunks = sortAndStoreAllChunks();
        long endTimeOfChunkSplit = System.currentTimeMillis();
        info(LOG, "Chunks creation time: " + (endTimeOfChunkSplit - startTimeOfChunkSplit) + " msec");

        if (memoryResident) { // if all sorted data reside in a main-memory table.
          this.result = new MemTableScanner(sortedTuples, sortAndStoredBytes);
        } else { // if input data exceeds main-memory at least once

          try {
            this.result = externalMergeAndSort(chunks);
          } catch (Exception e) {
            throw new PhysicalPlanningException(e);
          }

        }
      }

      sorted = true;
      result.init();

      // if loaded and sorted, we assume that it proceeds the half of one entire external sort operation.
      progress = 0.5f;
    }

    return result.next();
  }

  private int calculateFanout(int remainInputChunks, int intputNum, int outputNum, int startIdx) {
    int computedFanout = Math.min(remainInputChunks, defaultFanout);

    // Why should we detect an opportunity for unbalanced merge?
    //
    // Assume that a fanout is given by 8 and there are 10 chunks.
    // If we firstly merge 3 chunks into one chunk, there remain only 8 chunks.
    // Then, we can just finish the merge phase even though we don't complete merge phase on all chunks.
    if (checkIfCanBeUnbalancedMerged(intputNum - (startIdx + computedFanout), outputNum + 1)) {
      int candidateFanout = computedFanout;
      while(checkIfCanBeUnbalancedMerged(intputNum - (startIdx + candidateFanout), outputNum + 1)) {
        candidateFanout--;
      }
      int beforeFanout = computedFanout;
      if (computedFanout > candidateFanout + 1) {
        computedFanout = candidateFanout + 1;
        info(LOG, "Fanout reduced for unbalanced merge: " + beforeFanout + " -> " + computedFanout);
      }
    }

    return computedFanout;
  }

  private Scanner externalMergeAndSort(List<Path> chunks)
      throws IOException, ExecutionException, InterruptedException {
    int level = 0;
    final List<Path> inputFiles = TUtil.newList(chunks);
    final List<Path> outputFiles = TUtil.newList();
    int remainRun = inputFiles.size();
    int chunksSize = chunks.size();

    long mergeStart = System.currentTimeMillis();

    // continue until the remain runs are larger than defaultFanout
    while (remainRun > defaultFanout) {

      // reset outChunkId
      int remainInputRuns = inputFiles.size();
      int outChunkId = 0;
      int outputFileNum = 0;
      List<Future> futures = TUtil.newList();
      // the number of files being merged in threads.
      List<Integer> numberOfMergingFiles = TUtil.newList();

      for (int startIdx = 0; startIdx < inputFiles.size();) {

        // calculate proper fanout
        int fanout = calculateFanout(remainInputRuns, inputFiles.size(), outputFileNum, startIdx);
        // how many files are merged in ith thread?
        numberOfMergingFiles.add(fanout);
        // launch a merger runner
        futures.add(executorService.submit(
            new KWayMergerCaller(level, outChunkId++, inputFiles, startIdx, fanout, false)));
        outputFileNum++;

        startIdx += fanout;
        remainInputRuns = inputFiles.size() - startIdx;

        // If unbalanced merge is available, it finishes the merge phase earlier.
        if (checkIfCanBeUnbalancedMerged(remainInputRuns, outputFileNum)) {
          info(LOG, "Unbalanced merge possibility detected: number of remain input (" + remainInputRuns
              + ") and output files (" + outputFileNum + ") <= " + defaultFanout);

          List<Path> switched = TUtil.newList();
          // switch the remain inputs to the next outputs
          for (int j = startIdx; j < inputFiles.size(); j++) {
            switched.add(inputFiles.get(j));
          }
          inputFiles.removeAll(switched);
          outputFiles.addAll(switched);

          break;
        }
      }

      // wait for all sort runners
      int finishedMerger = 0;
      int index = 0;
      for (Future<Path> future : futures) {
        outputFiles.add(future.get());
        // Getting the number of merged files
        finishedMerger += numberOfMergingFiles.get(index++);
        // progress = (# number of merged files / total number of files) * 0.5;
        progress = ((float)finishedMerger/(float)chunksSize) * 0.5f;
      }

      // delete merged intermediate files
      for (Path path : inputFiles) {
        localFS.delete(path, true);
      }
      info(LOG, inputFiles.size() + " merged intermediate files deleted");

      // switch input files to output files, and then clear outputFiles
      inputFiles.clear();
      inputFiles.addAll(outputFiles);
      remainRun = inputFiles.size();
      outputFiles.clear();
      level++;
    }

    long mergeEnd = System.currentTimeMillis();
    info(LOG, "Total merge time: " + (mergeEnd - mergeStart) + " msec");

    // final result
    finalOutputFiles = inputFiles;

    result = createFinalMerger(inputFiles);
    return result;
  }

  /**
   * Merge Thread
   */
  private class KWayMergerCaller implements Callable<Path> {
    final int level;
    final int nextRunId;
    final List<Path> inputFiles;
    final int startIdx;
    final int mergeFanout;
    final boolean updateInputStats;

    public KWayMergerCaller(final int level, final int nextRunId, final List<Path> inputFiles,
                            final int startIdx, final int mergeFanout, final boolean updateInputStats) {
      this.level = level;
      this.nextRunId = nextRunId;
      this.inputFiles = inputFiles;
      this.startIdx = startIdx;
      this.mergeFanout = mergeFanout;
      this.updateInputStats = updateInputStats;
    }

    @Override
    public Path call() throws Exception {
      final Path outputPath = getChunkPathForWrite(level + 1, nextRunId);
      info(LOG, mergeFanout + " files are being merged to an output file " + outputPath.getName());
      long mergeStartTime = System.currentTimeMillis();
      final DirectRawFileWriter output = new DirectRawFileWriter(context.getConf(), inSchema, meta, outputPath);
      output.init();
      final Scanner merger = createKWayMerger(inputFiles, startIdx, mergeFanout);
      merger.init();
      Tuple mergeTuple;
      while((mergeTuple = merger.next()) != null) {
        output.addTuple(mergeTuple);
      }
      merger.close();
      output.close();
      long mergeEndTime = System.currentTimeMillis();
      info(LOG, outputPath.getName() + " is written to a disk. ("
          + FileUtil.humanReadableByteCount(output.getOffset(), false)
          + " bytes, " + (mergeEndTime - mergeStartTime) + " msec)");
      return outputPath;
    }
  }

  /**
   * It checks if unbalanced merge is possible.
   */
  private boolean checkIfCanBeUnbalancedMerged(int remainInputNum, int outputNum) {
    return (remainInputNum + outputNum) <= defaultFanout;
  }

  /**
   * Create a merged file scanner or k-way merge scanner.
   */
  private Scanner createFinalMerger(List<Path> inputs) throws IOException {
    if (inputs.size() == 1) {
      this.result = getFileScanner(inputs.get(0));
    } else {
      this.result = createKWayMerger(inputs, 0, inputs.size());
    }
    return result;
  }

  private Scanner getFileScanner(Path path) throws IOException {
    if (mergedInputPaths != null) {
      return new RawFile.RawFileScanner(context.getConf(), plan.getInSchema(), meta, path);
    } else {
      return new DirectRawFileScanner(context.getConf(), plan.getInSchema(), meta, path);
    }
  }

  private Scanner createKWayMerger(List<Path> inputs, final int startChunkId, final int num) throws IOException {
    final Scanner [] sources = new Scanner[num];
    for (int i = 0; i < num; i++) {
      sources[i] = getFileScanner(inputs.get(startChunkId + i));
    }

    return createKWayMergerInternal(sources, 0, num);
  }

  private Scanner createKWayMergerInternal(final Scanner [] sources, final int startIdx, final int num)
      throws IOException {
    if (num > 1) {
      final int mid = (int) Math.ceil((float)num / 2);
      return new PairWiseMerger(inSchema,
          createKWayMergerInternal(sources, startIdx, mid),
          createKWayMergerInternal(sources, startIdx + mid, num - mid), getComparator());
    } else {
      return sources[startIdx];
    }
  }

  @Override
  public void close() throws IOException {
    if (result != null) {
      result.close();
      try {
        inputStats = (TableStats)result.getInputStats().clone();
      } catch (CloneNotSupportedException e) {
        LOG.warn(e.getMessage());
      }
      result = null;
    }

    if (finalOutputFiles != null) {
      for (Path path : finalOutputFiles) {
        localFS.delete(path, true);
      }
    }

    if(tupleBlock != null){
      tupleBlock.free();
      tupleBlock = null;
    }

    if(executorService != null){
      executorService.shutdown();
      executorService = null;
    }

    plan = null;
    super.close();
  }

  @Override
  public void rescan() throws IOException {
    if (result != null) {
      result.reset();
    } else {
      super.rescan();
    }
    progress = 0.5f;
  }

  @Override
  public float getProgress() {
    if (result != null) {
      return progress + result.getProgress() * 0.5f;
    } else {
      return progress;
    }
  }

  @Override
  public TableStats getInputStats() {
    if (result != null) {
      return result.getInputStats();
    } else {
      return inputStats;
    }
  }
}
