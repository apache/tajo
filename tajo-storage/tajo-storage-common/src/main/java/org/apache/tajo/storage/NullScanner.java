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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

public class NullScanner implements Scanner {
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Fragment fragment;
  protected final int columnNum;
  protected Column [] targets;
  protected float progress;
  protected TableStats tableStats;

  public NullScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) {
    this.conf = conf;
    this.meta = meta;
    this.schema = schema;
    this.fragment = fragment;
    this.tableStats = new TableStats();
    this.columnNum = this.schema.size();
  }

  @Override
  public void init() throws IOException {
    progress = 0.0f;
    tableStats.setNumBytes(0);
    tableStats.setNumBlocks(0);
  }

  @Override
  public Tuple next() throws IOException {
    progress = 1.0f;
    return null;
  }

  @Override
  public void reset() throws IOException {
    progress = 0.0f;
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void setLimit(long num) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  @Override
  public float getProgress() {
    return progress;
  }

  @Override
  public TableStats getInputStats() {
    return tableStats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
