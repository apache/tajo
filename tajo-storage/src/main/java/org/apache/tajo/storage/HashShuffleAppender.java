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

import org.apache.tajo.catalog.statistics.TableStats;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class HashShuffleAppender implements Appender {
  private FileAppender appender;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private int partId;

  private TableStats tableStats;

  public HashShuffleAppender(int partId, FileAppender appender) {
    this.partId = partId;
    this.appender = appender;
  }

  @Override
  public void init() throws IOException {
  }

  public long addTuples(List<Tuple> tuples) throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return 0;
      }
      long currentPos = appender.getOffset();

      for (Tuple eachTuple: tuples) {
        appender.addTuple(eachTuple);
      }
      return appender.getOffset() - currentPos;
    }
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return;
      }
      appender.addTuple(t);
    }
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
  public void close() throws IOException {
    synchronized(appender) {
      if (closed.get()) {
        return;
      }
      appender.close();
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
}
