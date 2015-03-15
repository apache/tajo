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

import com.google.common.primitives.Longs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.NumberUtil.PrimitiveLongs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
  private Map<TaskAttemptId, PrimitiveLongs> taskTupleIndexes;

  //page start offset, length
  private PrimitiveLongs pages = new PrimitiveLongs(100);

  private long[] currentPage;

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
    currentPage = new long[2];
    taskTupleIndexes = new HashMap<TaskAttemptId, PrimitiveLongs>();
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
      PrimitiveLongs taskIndexes = taskTupleIndexes.get(taskId);
      if (taskIndexes == null) {
        taskIndexes = new PrimitiveLongs(100);
        taskTupleIndexes.put(taskId, taskIndexes);
      }
      taskIndexes.add(currentPage[0]);
      taskIndexes.add(NumberUtil.mergeToLong(rowNumInPage, nextRowNum));
      rowNumInPage = nextRowNum;

      if (posAfterWritten - currentPage[0] > pageSize) {
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
    currentPage[1] = pos - currentPage[0];
    pages.add(currentPage);
    currentPage = new long[] {pos, 0};
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
      if (offset > currentPage[0]) {
        nextPage(offset);
      }
      appender.close();
      if (LOG.isDebugEnabled()) {
        int size = pages.size();
        if (size > 0) {
          long[] array = pages.backingArray();
          LOG.info(ebId + ",partId=" + partId + " Appender closed: fileLen=" + offset + ", pages=" + size
              + ", lastPage=" + array[size - 2] + ", " + array[size - 1]);
        } else {
          LOG.info(ebId + ",partId=" + partId + " Appender closed: fileLen=" + offset + ", pages=" + size);
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

  public PrimitiveLongs getPages() {
    return pages;
  }

  public Map<TaskAttemptId, PrimitiveLongs> getTaskTupleIndexes() {
    return taskTupleIndexes;
  }

  public Iterable<Long> getMergedTupleIndexes() {
    return getIterable(taskTupleIndexes.values());
  }

  public Iterable<Long> getIterable(final Collection<PrimitiveLongs> values) {
    return new Iterable<Long>() {
      @Override
      public Iterator<Long> iterator() {
        final Iterator<PrimitiveLongs> iterator1 = values.iterator();
        return new Iterator<Long>() {
          Iterator<Long> iterator2 = null;
          @Override
          public boolean hasNext() {
            while (iterator2 == null || !iterator2.hasNext()) {
              if (!iterator1.hasNext()) {
                return false;
              }
              iterator2 = iterator1.next().iterator();
            }
            return true;
          }

          @Override
          public Long next() {
            return iterator2.next();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  public void taskFinished(TaskAttemptId taskId) {
    taskTupleIndexes.remove(taskId);
  }
}
