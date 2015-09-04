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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.EmptyTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.parquet.TajoParquetReader;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * Currently it is pqrquet-specific.
 *
 * TODO: change to support other file types
 */
public class StatCountExec extends ScanExec {
  private ScanNode plan;
  private CatalogProtos.FragmentProto [] fragments;
  private long numRows = 0;
  private long currentRow = 0;
  private TableStats tableStats = new TableStats();

  enum OpType {
    READ_TOTAL_ROWS
  }

  public StatCountExec(TaskAttemptContext context, ScanNode plan,
                       CatalogProtos.FragmentProto[] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.fragments = fragments;
  }

  @Override
  public String getTableName() {
    return plan.getTableName();
  }

  @Override
  public String getCanonicalName() {
    return plan.getCanonicalName();
  }

  @Override
  public CatalogProtos.FragmentProto[] getFragments() {
    return fragments;
  }

  @Override
  public void init() throws IOException {
    super.init();

    long totalLen = 0;
    int totalBlocks = 0;

    for (Fragment fragment: FragmentConvertor.convert(context.getConf(), fragments)) {
      if (fragment == null)
        continue;

      numRows += TajoParquetReader.getCount(context.getConf(), (FileFragment)fragment);

      totalLen += fragment.getLength();
      totalBlocks++;
    }

    tableStats.setNumBytes(totalLen);
    tableStats.setNumBlocks(totalBlocks);
    tableStats.setNumRows(numRows);

    Schema schema = plan.getPhysicalSchema();

    if (schema != null) {
      for(Column eachColumn: schema.getRootColumns()) {
        ColumnStats columnStats = new ColumnStats(eachColumn);
        tableStats.addColumnStat(columnStats);
      }
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (currentRow == numRows) {
      return null;
    }

    currentRow++;

    return EmptyTuple.get();
  }

  @Override
  public void rescan() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public TableStats getInputStats() {
    return tableStats;
  }

  @Override
  public float getProgress() {
    return currentRow / (float)numRows;
  }
}
