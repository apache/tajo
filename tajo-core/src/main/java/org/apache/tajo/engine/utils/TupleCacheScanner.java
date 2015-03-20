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

package org.apache.tajo.engine.utils;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TupleCacheScanner implements Scanner {
  List<Tuple> cacheData;
  Schema schema;
  Iterator<Tuple> it;
  int count;
  TableStats inputStats = new TableStats();

  public TupleCacheScanner(List<Tuple> cacheData, Schema schema) {
    this.cacheData = cacheData;
    this.schema = schema;
  }
  @Override
  public void init() throws IOException {
    inputStats.setNumRows(cacheData.size());
    inputStats.setReadBytes(0);
    it = cacheData.iterator();
    count = 0;
  }

  @Override
  public Tuple next() throws IOException {
    if (it.hasNext()) {
      count++;
      Tuple tuple = it.next();
      try {
        return (Tuple)tuple.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e.getMessage(), e);
      }
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
    return true;
  }

  @Override
  public void setSearchCondition(Object expr) {
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {
    if (cacheData.size() == 0) {
      return 1.0f;
    }
    return ((float)count) / cacheData.size();
  }

  @Override
  public TableStats getInputStats() {
    return inputStats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
