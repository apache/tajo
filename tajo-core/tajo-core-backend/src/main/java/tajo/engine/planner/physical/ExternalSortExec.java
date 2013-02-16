/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.planner.physical;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.TaskAttemptContext;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.planner.logical.SortNode;
import tajo.storage.*;

import java.io.IOException;
import java.util.*;

public class ExternalSortExec extends SortExec {
  private SortNode plan;

  private final List<Tuple> tupleSlots;
  private boolean sorted = false;
  private RawFile.RawFileScanner result;
  private RawFile.RawFileAppender appender;
  private FileSystem localFS;

  private final TableMeta meta;
  private final Path sortTmpDir;
  private int SORT_BUFFER_SIZE;

  public ExternalSortExec(final TaskAttemptContext context,
      final StorageManager sm, final SortNode plan, final PhysicalExec child)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child,
        plan.getSortKeys());
    this.plan = plan;

    this.SORT_BUFFER_SIZE = context.getConf().getIntVar(ConfVars.EXT_SORT_BUFFER);
    this.tupleSlots = new ArrayList<>(SORT_BUFFER_SIZE);

    this.sortTmpDir = new Path(context.getWorkDir(), UUID.randomUUID().toString());
    this.localFS = FileSystem.getLocal(context.getConf());
    meta = TCatUtil.newTableMeta(inSchema, StoreType.ROWFILE);
  }

  public void init() throws IOException {
    super.init();
    localFS.mkdirs(sortTmpDir);
  }

  public SortNode getPlan() {
    return this.plan;
  }

  private void sortAndStoreChunk(int chunkId, List<Tuple> tupleSlots)
      throws IOException {
    TableMeta meta = TCatUtil.newTableMeta(inSchema, StoreType.RAW);
    Collections.sort(tupleSlots, getComparator());
    // TODO - RawFile requires the local file path.
    // So, I add the scheme 'file:/' to path. But, it should be improved.
    Path localPath = new Path(sortTmpDir + "/0_" + chunkId);

    appender = new RawFile.RawFileAppender(context.getConf(), meta, localPath);
    appender.init();

    for (Tuple t : tupleSlots) {
      appender.addTuple(t);
    }
    appender.close();
    tupleSlots.clear();
  }

  /**
   * It divides all tuples into a number of chunks, then sort for each chunk.
   * @return the number of stored chunks
   * @throws java.io.IOException
   */
  private int sortAndStoreAllChunks() throws IOException {
    int chunkId = 0;

    Tuple tuple;
    while ((tuple = child.next()) != null) { // partition sort start
      tupleSlots.add(new VTuple(tuple));
      if (tupleSlots.size() == SORT_BUFFER_SIZE) {
        sortAndStoreChunk(chunkId, tupleSlots);
        chunkId++;
      }
    }

    if (tupleSlots.size() > 0) {
      sortAndStoreChunk(chunkId, tupleSlots);
      chunkId++;
    }

    return chunkId;
  }

  private Path getChunkPath(int level, int chunkId) {
    return StorageUtil.concatPath(sortTmpDir, "" + level + "_" + chunkId);
  }

  @Override
  public Tuple next() throws IOException {
    if (!sorted) {

      // the total number of chunks for zero level
      int totalChunkNumForLevel = sortAndStoreAllChunks();

      // if there are no chunk
      if (totalChunkNumForLevel == 0) {
        return null;
      }

      int level = 0;
      int chunkId = 0;

      // continue until the chunk remains only one
      while (totalChunkNumForLevel > 1) {

        while (chunkId < totalChunkNumForLevel) {

          Path nextChunk = getChunkPath(level + 1, chunkId / 2);

          // if number of chunkId is odd just copy it.
          if (chunkId + 1 >= totalChunkNumForLevel) {

            Path chunk = getChunkPath(level, chunkId);
            localFS.moveFromLocalFile(chunk, nextChunk);

          } else {

            Path leftChunk = getChunkPath(level, chunkId);
            Path rightChunk = getChunkPath(level, chunkId + 1);

            appender = new RawFile.RawFileAppender(context.getConf(), meta, nextChunk);
            appender.init();
            merge(appender, leftChunk, rightChunk);

            appender.flush();
            appender.close();
          }

          chunkId += 2;
        }

        level++;
        // init chunkId for each level
        chunkId = 0;
        // calculate the total number of chunks for next level
        totalChunkNumForLevel = totalChunkNumForLevel / 2
            + totalChunkNumForLevel % 2;
      }

      Path result = getChunkPath(level, 0);
      this.result = new RawFile.RawFileScanner(context.getConf(), meta, result);
      sorted = true;
    }

    return result.next();
  }

  private void merge(RawFile.RawFileAppender appender, Path left, Path right)
      throws IOException {
    RawFile.RawFileScanner leftScan = new RawFile.RawFileScanner(context.getConf(), meta, left);

    RawFile.RawFileScanner rightScan =
        new RawFile.RawFileScanner(context.getConf(), meta, right);

    Tuple leftTuple = leftScan.next();
    Tuple rightTuple = rightScan.next();

    Comparator<Tuple> comparator = getComparator();
    while (leftTuple != null && rightTuple != null) {
      if (comparator.compare(leftTuple, rightTuple) < 0) {
        appender.addTuple(leftTuple);
        leftTuple = leftScan.next();
      } else {
        appender.addTuple(rightTuple);
        rightTuple = rightScan.next();
      }
    }

    if (leftTuple == null) {
      appender.addTuple(rightTuple);
      while ((rightTuple = rightScan.next()) != null) {
        appender.addTuple(rightTuple);
      }
    } else {
      appender.addTuple(leftTuple);
      while ((leftTuple = leftScan.next()) != null) {
        appender.addTuple(leftTuple);
      }
    }

    leftScan.close();
    rightScan.close();
  }

  @Override
  public void rescan() throws IOException {
    if (result != null) {
      result.reset();
    }
  }
}
