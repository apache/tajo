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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.PhysicalPlanningException;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.tajo.storage.RawFile.RawFileAppender;
import static org.apache.tajo.storage.RawFile.RawFileScanner;

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
  /** The prefix of fragment name for intermediate */
  private static final String INTERMEDIATE_FILE_PREFIX = "@interFile_";

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
  private TupleList inMemoryTable;
  /** temporal dir */
  private final Path sortTmpDir;
  /** It enables round-robin disks allocation */
  private final LocalDirAllocator localDirAllocator;
  /** local file system */
  private final RawLocalFileSystem localFS;
  /** final output files which are used for cleaning */
  private List<FileFragment> finalOutputFiles = null;
  /** for directly merging sorted inputs */
  private List<FileFragment> mergedInputFragments = null;

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

  private ExternalSortExec(final TaskAttemptContext context, final SortNode plan)
      throws PhysicalPlanningException {
    super(context, plan.getInSchema(), plan.getOutSchema(), null, plan.getSortKeys());

    this.plan = plan;
    this.meta = CatalogUtil.newTableMeta("ROWFILE");

    this.defaultFanout = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT);
    if (defaultFanout < 2) {
      throw new PhysicalPlanningException(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT.varname + " cannot be lower than 2");
    }
    // TODO - sort buffer and core num should be changed to use the allocated container resource.
    this.sortBufferBytesNum = context.getQueryContext().getLong(SessionVars.EXTSORT_BUFFER_SIZE) * StorageUnit.MB;
    this.allocatedCoreNum = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_THREAD_NUM);
    this.executorService = Executors.newFixedThreadPool(this.allocatedCoreNum);
    this.inMemoryTable = new TupleList(100000);

    this.sortTmpDir = getExecutorTmpDir();
    localDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    localFS = new RawLocalFileSystem();
  }

  public ExternalSortExec(final TaskAttemptContext context,final SortNode plan,
                          final CatalogProtos.FragmentProto[] fragments) throws PhysicalPlanningException {
    this(context, plan);

    mergedInputFragments = TUtil.newList();
    for (CatalogProtos.FragmentProto proto : fragments) {
      FileFragment fragment = FragmentConvertor.convert(FileFragment.class, proto);
      mergedInputFragments.add(fragment);
    }
  }

  public ExternalSortExec(final TaskAttemptContext context, final SortNode plan, final PhysicalExec child)
      throws IOException {
    this(context, plan);
    setChild(child);
  }

  public void init() throws IOException {
    inputStats = new TableStats();
    super.init();
  }

  public SortNode getPlan() {
    return this.plan;
  }

  /**
   * Sort a tuple block and store them into a chunk file
   */
  private Path sortAndStoreChunk(int chunkId, TupleList tupleBlock)
      throws IOException {
    TableMeta meta = CatalogUtil.newTableMeta("RAW");
    int rowNum = tupleBlock.size();

    long sortStart = System.currentTimeMillis();
    Iterable<Tuple> sorted = getSorter(tupleBlock).sort();
    long sortEnd = System.currentTimeMillis();

    long chunkWriteStart = System.currentTimeMillis();
    Path outputPath = getChunkPathForWrite(0, chunkId);
    final RawFileAppender appender = new RawFileAppender(context.getConf(), null, inSchema, meta, outputPath);
    appender.init();
    for (Tuple t : sorted) {
      appender.addTuple(t);
    }
    appender.close();
    tupleBlock.clear();
    long chunkWriteEnd = System.currentTimeMillis();


    info(LOG, "Chunk #" + chunkId + " sort and written (" +
        FileUtil.humanReadableByteCount(appender.getOffset(), false) + " bytes, " + rowNum + " rows, " +
        "sort time: " + (sortEnd - sortStart) + " msec, " +
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
    long memoryConsumption = 0;
    List<Path> chunkPaths = TUtil.newList();

    int chunkId = 0;
    long runStartTime = System.currentTimeMillis();
    while (!context.isStopped() && (tuple = child.next()) != null) { // partition sort start
      inMemoryTable.add(tuple);
      memoryConsumption += MemoryUtil.calculateMemorySize(tuple);

      if (memoryConsumption > sortBufferBytesNum) {
        long runEndTime = System.currentTimeMillis();
        info(LOG, chunkId + " run loading time: " + (runEndTime - runStartTime) + " msec");
        runStartTime = runEndTime;

        info(LOG, "Memory consumption exceeds " + sortBufferBytesNum + " bytes");
        memoryResident = false;

        chunkPaths.add(sortAndStoreChunk(chunkId, inMemoryTable));

        memoryConsumption = 0;
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

    if (!memoryResident && !inMemoryTable.isEmpty()) { // if there are at least one or more input tuples
      // check if data exceeds a sort buffer. If so, it store the remain data into a chunk.
      long start = System.currentTimeMillis();
      int rowNum = inMemoryTable.size();
      chunkPaths.add(sortAndStoreChunk(chunkId, inMemoryTable));
      long end = System.currentTimeMillis();
      info(LOG, "Last Chunk #" + chunkId + " " + rowNum + " rows written (" + (end - start) + " msec)");
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
      if (mergedInputFragments != null) {
        try {
          this.result = externalMergeAndSort(mergedInputFragments);
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
          TupleSorter sorter = getSorter(inMemoryTable);
          result = new MemTableScanner(sorter.sort(), inMemoryTable.size(), sortAndStoredBytes);
        } else { // if input data exceeds main-memory at least once

          try {
            List<FileFragment> fragments = TUtil.newList();
            for (Path chunk : chunks) {
              FileFragment frag = new FileFragment("", chunk, 0,
                  new File(localFS.makeQualified(chunk).toUri()).length());
              fragments.add(frag);
            }

            this.result = externalMergeAndSort(fragments);
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

  private int calculateFanout(int remainInputChunks, int inputNum, int outputNum, int startIdx) {
    int computedFanout = Math.min(remainInputChunks, defaultFanout);

    // Why should we detect an opportunity for unbalanced merge?
    //
    // Assume that a fanout is given by 8 and there are 10 chunks.
    // If we firstly merge 3 chunks into one chunk, there remain only 8 chunks.
    // Then, we can just finish the merge phase even though we don't complete merge phase on all chunks.
    if (checkIfCanBeUnbalancedMerged(inputNum - (startIdx + computedFanout), outputNum + 1)) {
      int candidateFanout = computedFanout;
      while (checkIfCanBeUnbalancedMerged(inputNum - (startIdx + candidateFanout), outputNum + 1)) {
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

  private Scanner externalMergeAndSort(List<FileFragment> chunks)
      throws IOException, ExecutionException, InterruptedException {
    int level = 0;
    final List<FileFragment> inputFiles = TUtil.newList(chunks);
    final List<FileFragment> outputFiles = TUtil.newList();
    int remainRun = inputFiles.size();
    int chunksSize = chunks.size();

    long mergeStart = System.currentTimeMillis();

    // continue until the remain runs are larger than defaultFanout
    while (remainRun > defaultFanout) {

      // reset outChunkId
      int remainInputRuns = inputFiles.size();
      int outChunkId = 0;
      int outputFileNum = 0;
      List<Future<FileFragment>> futures = TUtil.newList();
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

          List<FileFragment> switched = TUtil.newList();
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
      for (Future<FileFragment> future : futures) {
        outputFiles.add(future.get());
        // Getting the number of merged files
        finishedMerger += numberOfMergingFiles.get(index++);
        // progress = (# number of merged files / total number of files) * 0.5;
        progress = ((float)finishedMerger/(float)chunksSize) * 0.5f;
      }

      /*
       * delete merged intermediate files
       * 
       * There may be 4 different types of file fragments in the list inputFiles
       * + A: a fragment created from fetched data from a remote host. By default, this fragment represents
       * a whole physical file (i.e., startOffset == 0 and length == length of physical file)
       * + B1: a fragment created from a local file (pseudo-fetched data from local host) in which the fragment
       * represents the whole physical file (i.e., startOffset == 0 AND length == length of physical file)
       * + B2: a fragment created from a local file (pseudo-fetched data from local host) in which the fragment
       * represents only a part of the physical file (i.e., startOffset > 0 OR length != length of physical file)
       * + C: a fragment created from merging some fragments of the above types. When this fragment is created,
       * its startOffset is set to 0 and its length is set to the length of the physical file, automatically
       * 
       * Fragments of types A, B1, and B2 are inputs of ExternalSortExec. Among them, only B2-type fragments will
       * possibly be used by another task in the future. Thus, ideally, all fragments of types A, B1, and C can be
       * deleted at this point. However, for the ease of future code maintenance, we delete only type-C fragments here
       */
      int numDeletedFiles = 0;
      for (FileFragment frag : inputFiles) {
        if (frag.getTableName().contains(INTERMEDIATE_FILE_PREFIX)) {
          localFS.delete(frag.getPath(), true);
          numDeletedFiles++;
          LOG.info("Delete merged intermediate file: " + frag);
        }
      }
      info(LOG, numDeletedFiles + " merged intermediate files deleted");

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
  private class KWayMergerCaller implements Callable<FileFragment> {
    final int level;
    final int nextRunId;
    final List<FileFragment> inputFiles;
    final int startIdx;
    final int mergeFanout;
    final boolean updateInputStats;

    public KWayMergerCaller(final int level, final int nextRunId, final List<FileFragment> inputFiles,
                            final int startIdx, final int mergeFanout, final boolean updateInputStats) {
      this.level = level;
      this.nextRunId = nextRunId;
      this.inputFiles = inputFiles;
      this.startIdx = startIdx;
      this.mergeFanout = mergeFanout;
      this.updateInputStats = updateInputStats;
    }

    @Override
    public FileFragment call() throws Exception {
      final Path outputPath = getChunkPathForWrite(level + 1, nextRunId);
      info(LOG, mergeFanout + " files are being merged to an output file " + outputPath.getName());
      long mergeStartTime = System.currentTimeMillis();
      final RawFileAppender output = new RawFileAppender(context.getConf(), null, inSchema, meta, outputPath);
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
      File f = new File(localFS.makeQualified(outputPath).toUri());
      FileFragment frag = new FileFragment(INTERMEDIATE_FILE_PREFIX + outputPath.getName(), outputPath, 0, f.length());
      return frag;
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
  private Scanner createFinalMerger(List<FileFragment> inputs) throws IOException {
    if (inputs.size() == 1) {
      this.result = getFileScanner(inputs.get(0));
    } else {
      this.result = createKWayMerger(inputs, 0, inputs.size());
    }
    return result;
  }

  private Scanner getFileScanner(FileFragment frag) throws IOException {
    return new RawFileScanner(context.getConf(), plan.getInSchema(), meta, frag);
  }

  private Scanner createKWayMerger(List<FileFragment> inputs, final int startChunkId, final int num) throws IOException {
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
      Scanner left = createKWayMergerInternal(sources, startIdx, mid);
      Scanner right = createKWayMergerInternal(sources, startIdx + mid, num - mid);
      if (ComparableVector.isVectorizable(sortSpecs)) {
        return new VectorComparePairWiseMerger(inSchema, left, right, comparator);
      }
      return new PairWiseMerger(inSchema, left, right, comparator);
    } else {
      return sources[startIdx];
    }
  }

  private static class MemTableScanner extends AbstractScanner {
    final Iterable<Tuple> iterable;
    final long sortAndStoredBytes;
    final int totalRecords;

    Iterator<Tuple> iterator;
    // for input stats
    float scannerProgress;
    int numRecords;
    TableStats scannerTableStats;

    public MemTableScanner(Iterable<Tuple> iterable, int length, long inBytes) {
      this.iterable = iterable;
      this.totalRecords = length;
      this.sortAndStoredBytes = inBytes;
    }

    @Override
    public void init() throws IOException {
      iterator = iterable.iterator();

      scannerProgress = 0.0f;
      numRecords = 0;

      // it will be returned as the final stats
      scannerTableStats = new TableStats();
      scannerTableStats.setNumBytes(sortAndStoredBytes);
      scannerTableStats.setReadBytes(sortAndStoredBytes);
      scannerTableStats.setNumRows(totalRecords);
    }

    @Override
    public Tuple next() throws IOException {
      if (iterator.hasNext()) {
        numRecords++;
        return iterator.next();
      } else {
        return null;
      }
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      iterator = null;
      scannerProgress = 1.0f;
    }

    @Override
    public float getProgress() {
      if (iterator != null && numRecords > 0) {
        return (float)numRecords / (float)totalRecords;

      } else { // if an input is empty
        return scannerProgress;
      }
    }

    @Override
    public TableStats getInputStats() {
      return scannerTableStats;
    }
  }

  enum State {
    NEW,
    INITED,
    CLOSED
  }

  private static class VectorComparePairWiseMerger extends PairWiseMerger {

    private ComparableVector comparable;

    public VectorComparePairWiseMerger(Schema schema, Scanner leftScanner, Scanner rightScanner,
                                       BaseTupleComparator comparator) throws IOException {
      super(schema, leftScanner, rightScanner, null);
      comparable = new ComparableVector(2, comparator.getSortSpecs(), comparator.getSortKeyIds());
    }

    @Override
    protected Tuple prepare(int index, Tuple tuple) {
      if (tuple != null) {
        comparable.set(index, tuple);
      }
      return tuple;
    }

    @Override
    protected int compare() {
      return comparable.compare(0, 1);
    }
  }

  /**
   * Two-way merger scanner that reads two input sources and outputs one output tuples sorted in some order.
   */
  private static class PairWiseMerger extends AbstractScanner {

    protected final Schema schema;
    protected final Comparator<Tuple> comparator;

    protected final Scanner leftScan;
    protected final Scanner rightScan;

    private Tuple leftTuple;
    private Tuple rightTuple;

    private final Tuple outTuple;

    private float mergerProgress;
    private TableStats mergerInputStats;

    private State state = State.NEW;

    public PairWiseMerger(Schema schema, Scanner leftScanner, Scanner rightScanner, Comparator<Tuple> comparator)
        throws IOException {
      this.schema = schema;
      this.leftScan = leftScanner;
      this.rightScan = rightScanner;
      this.comparator = comparator;
      this.outTuple = new VTuple(schema.size());
    }

    private void setState(State state) {
      this.state = state;
    }

    @Override
    public void init() throws IOException {
      if (state == State.NEW) {
        leftScan.init();
        rightScan.init();

        prepareTuplesForFirstComparison();

        mergerInputStats = new TableStats();
        mergerProgress = 0.0f;

        setState(State.INITED);
      } else {
        throw new IllegalStateException("Illegal State: init() is not allowed in " + state.name());
      }
    }

    private void prepareTuplesForFirstComparison() throws IOException {
      leftTuple = prepare(0, leftScan.next());
      rightTuple = prepare(1, rightScan.next());
    }

    protected Tuple prepare(int index, Tuple tuple) {
      return tuple;
    }

    protected int compare() {
      return comparator.compare(leftTuple, rightTuple);
    }

    @Override
    public Tuple next() throws IOException {
      if (leftTuple == null && rightTuple == null) {
        return null;
      }
      if (rightTuple == null || (leftTuple != null && compare() < 0)) {
        outTuple.put(leftTuple.getValues());
        leftTuple = prepare(0, leftScan.next());
        return outTuple;
      }
      outTuple.put(rightTuple.getValues());
      rightTuple = prepare(1, rightScan.next());
      return outTuple;
    }

    @Override
    public void reset() throws IOException {
      if (state == State.INITED) {
        leftScan.reset();
        rightScan.reset();

        leftTuple = null;
        rightTuple = null;

        prepareTuplesForFirstComparison();
      } else {
        throw new IllegalStateException("Illegal State: init() is not allowed in " + state.name());
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.cleanup(LOG, leftScan, rightScan);
      getInputStats();
      mergerProgress = 1.0f;
      leftTuple = null;
      rightTuple = null;
      setState(State.CLOSED);
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public float getProgress() {
      if (leftScan == null) {
        return mergerProgress;
      }
      return leftScan.getProgress() * 0.5f + rightScan.getProgress() * 0.5f;
    }

    @Override
    public TableStats getInputStats() {
      if (leftScan == null) {
        return mergerInputStats;
      }
      TableStats leftInputStats = leftScan.getInputStats();
      if (mergerInputStats == null) {
        mergerInputStats = new TableStats();
      }
      mergerInputStats.setNumBytes(0);
      mergerInputStats.setReadBytes(0);
      mergerInputStats.setNumRows(0);

      if (leftInputStats != null) {
        mergerInputStats.setNumBytes(leftInputStats.getNumBytes());
        mergerInputStats.setReadBytes(leftInputStats.getReadBytes());
        mergerInputStats.setNumRows(leftInputStats.getNumRows());
      }

      TableStats rightInputStats = rightScan.getInputStats();
      if (rightInputStats != null) {
        mergerInputStats.setNumBytes(mergerInputStats.getNumBytes() + rightInputStats.getNumBytes());
        mergerInputStats.setReadBytes(mergerInputStats.getReadBytes() + rightInputStats.getReadBytes());
        mergerInputStats.setNumRows(mergerInputStats.getNumRows() + rightInputStats.getNumRows());
      }

      return mergerInputStats;
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
      for (FileFragment frag : finalOutputFiles) {
        File tmpFile = new File(localFS.makeQualified(frag.getPath()).toUri());
        if (frag.getStartKey() == 0 && frag.getLength() == tmpFile.length()) {
          localFS.delete(frag.getPath(), true);
          LOG.info("Delete file: " + frag);
        }
      }
    }

    if(inMemoryTable != null){
      inMemoryTable.clear();
      inMemoryTable = null;
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
    }
    super.rescan();
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
