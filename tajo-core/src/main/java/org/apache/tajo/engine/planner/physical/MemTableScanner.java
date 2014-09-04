/***
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

import com.google.common.base.Preconditions;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

class MemTableScanner implements Scanner {
  Iterable<Tuple> iterable;
  Iterator<Tuple> iterator;
  long inputBytes;

  // for input stats
  float scannerProgress;
  int numRecords;
  int totalRecords;
  TableStats scannerTableStats;

  public MemTableScanner(@NotNull Collection<Tuple> iterable, long inputBytes) {
    Preconditions.checkNotNull(iterable);
    this.iterable = iterable;
    totalRecords = iterable.size();
    this.inputBytes = inputBytes;
  }

  @Override
  public void init() throws IOException {
    scannerProgress = 0.0f;
    numRecords = 0;

    // it will be returned as the final stats
    scannerTableStats = new TableStats();
    scannerTableStats.setNumBytes(inputBytes);
    scannerTableStats.setReadBytes(inputBytes);
    scannerTableStats.setNumRows(totalRecords);

    iterator = iterable.iterator();
  }

  @Override
  public Tuple next() throws IOException {
    if (iterator.hasNext()) {
      numRecords++;
      return new VTuple(iterator.next());
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
    scannerProgress = 1.0f;
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setSearchCondition(Object expr) {
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public float getProgress() {
    if (numRecords > 0) {
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
