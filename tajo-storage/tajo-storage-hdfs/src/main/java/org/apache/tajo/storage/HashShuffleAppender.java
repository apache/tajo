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

package org.apache.tajo.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class HashShuffleAppender implements Appender {
  private static Log LOG = LogFactory.getLog(HashShuffleAppender.class);

  private FileAppender appender;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private int partId;

  private TableStats tableStats;

  //<taskId,<page start offset,<task start, task end>>>
  private Map<TaskAttemptId, List<Pair<Long, Pair<Integer, Integer>>>> taskTupleIndexes;

  //page start offset, length
  private List<Pair<Long, Integer>> pages = new ArrayList<Pair<Long, Integer>>();

  private Pair<Long, Integer> currentPage;

  private int pageSize; //MB

  private int rowNumInPage;

  private int totalRows;

  private long offset;

  private ExecutionBlockId ebId;

  public HashShuffleAppender(ExecutionBlockId ebId, int partId, int pageSize, FileAppender appender) {
    this.ebId = ebId;
    this.partId = partId;
    this.appender = appender;
    this.pageSize = pageSize;
  }

  @Override
  public void init() throws IOException {
    currentPage = new Pair(0L, 0);
    taskTupleIndexes = new HashMap<TaskAttemptId, List<Pair<Long, Pair<Integer, Integer>>>>();
    rowNumInPage = 0;
  }

  /**
   * Write multiple tuples. Each tuple is written by a FileAppender which is responsible specified partition.
   * After writing if a current page exceeds pageSize, pageOffset will be added.
   * @param taskId
   * @param tuples
   * @return written bytes
   * @throws java.io.IOException
   */
  public int addTuples(TaskAttemptId taskId, List<Tuple> tuples) throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return 0;
      }
      long currentPos = appender.getOffset();

      for (Tuple eachTuple: tuples) {
        appender.addTuple(eachTuple);
      }
      long posAfterWritten = appender.getOffset();

      int writtenBytes = (int)(posAfterWritten - currentPos);

      int nextRowNum = rowNumInPage + tuples.size();
      List<Pair<Long, Pair<Integer, Integer>>> taskIndexes = taskTupleIndexes.get(taskId);
      if (taskIndexes == null) {
        taskIndexes = new ArrayList<Pair<Long, Pair<Integer, Integer>>>();
        taskTupleIndexes.put(taskId, taskIndexes);
      }
      taskIndexes.add(
          new Pair<Long, Pair<Integer, Integer>>(currentPage.getFirst(), new Pair(rowNumInPage, nextRowNum)));
      rowNumInPage = nextRowNum;

      if (posAfterWritten - currentPage.getFirst() > pageSize) {
        nextPage(posAfterWritten);
        rowNumInPage = 0;
      }

      totalRows += tuples.size();
      return writtenBytes;
    }
  }

  public long getOffset() throws IOException {
    if (closed.get()) {
      return offset;
    } else {
      return appender.getOffset();
    }
  }

  private void nextPage(long pos) {
    currentPage.setSecond((int) (pos - currentPage.getFirst()));
    pages.add(currentPage);
    currentPage = new Pair(pos, 0);
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    throw new IOException("Not support addTuple, use addTuples()");
  }

  @Override
  public void flush() throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return;
      }
      appender.flush();
    }
  }

  @Override
  public long getEstimatedOutputSize() throws IOException {
    return pageSize * pages.size();
  }

  @Override
  public void close() throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return;
      }
      appender.flush();
      offset = appender.getOffset();
      if (offset > currentPage.getFirst()) {
        nextPage(offset);
      }
      appender.close();
      if (LOG.isDebugEnabled()) {
        if (!pages.isEmpty()) {
          LOG.info(ebId + ",partId=" + partId + " Appender closed: fileLen=" + offset + ", pages=" + pages.size()
              + ", lastPage=" + pages.get(pages.size() - 1));
        } else {
          LOG.info(ebId + ",partId=" + partId + " Appender closed: fileLen=" + offset + ", pages=" + pages.size());
        }
      }
      closed.set(true);
      tableStats = appender.getStats();
    }
  }

  @Override
  public void enableStats() {
  }

  @Override
  public TableStats getStats() {
    synchronized(appender) {
      return appender.getStats();
    }
  }

  public List<Pair<Long, Integer>> getPages() {
    return pages;
  }

  public Map<TaskAttemptId, List<Pair<Long, Pair<Integer, Integer>>>> getTaskTupleIndexes() {
    return taskTupleIndexes;
  }

  public List<Pair<Long, Pair<Integer, Integer>>> getMergedTupleIndexes() {
    List<Pair<Long, Pair<Integer, Integer>>> merged = new ArrayList<Pair<Long, Pair<Integer, Integer>>>();

    for (List<Pair<Long, Pair<Integer, Integer>>> eachFailureIndex: taskTupleIndexes.values()) {
      merged.addAll(eachFailureIndex);
    }

    return merged;
  }

  public void taskFinished(TaskAttemptId taskId) {
    taskTupleIndexes.remove(taskId);
  }
}
