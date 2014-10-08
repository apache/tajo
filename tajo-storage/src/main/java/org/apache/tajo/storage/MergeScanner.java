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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeScanner implements Scanner {
  private Configuration conf;
  private TableMeta meta;
  private Schema schema;
  private List<FileFragment> fragments;
  private Iterator<FileFragment> iterator;
  private FileFragment currentFragment;
  private Scanner currentScanner;
  private Tuple tuple;
  private boolean projectable = false;
  private boolean selectable = false;
  private Schema target;
  private float progress;
  protected TableStats tableStats;

  public MergeScanner(Configuration conf, Schema schema, TableMeta meta, List<FileFragment> rawFragmentList)
      throws IOException {
    this(conf, schema, meta, rawFragmentList, schema);
  }

  public MergeScanner(Configuration conf, Schema schema, TableMeta meta, List<FileFragment> rawFragmentList,
                      Schema target)
      throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.meta = meta;
    this.target = target;

    this.fragments = new ArrayList<FileFragment>();

    long numBytes = 0;
    for (FileFragment eachFileFragment: rawFragmentList) {
      numBytes += eachFileFragment.getEndKey();
      if (eachFileFragment.getEndKey() > 0) {
        fragments.add(eachFileFragment);
      }
    }

    // it should keep the input order. Otherwise, it causes wrong result of sort queries.
    this.reset();

    if (currentScanner != null) {
      this.projectable = currentScanner.isProjectable();
      this.selectable = currentScanner.isSelectable();
    }

    tableStats = new TableStats();

    tableStats.setNumBytes(numBytes);
    tableStats.setNumBlocks(fragments.size());

    for(Column eachColumn: schema.getColumns()) {
      ColumnStats columnStats = new ColumnStats(eachColumn);
      tableStats.addColumnStat(columnStats);
    }
  }

  @Override
  public void init() throws IOException {
    progress = 0.0f;
  }

  @Override
  public Tuple next() throws IOException {
    if (currentScanner != null)
      tuple = currentScanner.next();

    if (tuple != null) {
      return tuple;
    } else {
      if (currentScanner != null) {
        currentScanner.close();
        TableStats scannerTableStsts = currentScanner.getInputStats();
        if (scannerTableStsts != null) {
          tableStats.setReadBytes(tableStats.getReadBytes() + scannerTableStsts.getReadBytes());
          tableStats.setNumRows(tableStats.getNumRows() + scannerTableStsts.getNumRows());
        }
      }
      currentScanner = getNextScanner();
      if (currentScanner != null) {
        tuple = currentScanner.next();
      }
    }
    return tuple;
  }

  @Override
  public void reset() throws IOException {
    this.iterator = fragments.iterator();
    this.currentScanner = getNextScanner();
  }

  private Scanner getNextScanner() throws IOException {
    if (iterator.hasNext()) {
      currentFragment = iterator.next();
      currentScanner = StorageManagerFactory.getStorageManager((TajoConf)conf).getScanner(meta, schema,
          currentFragment, target);
      currentScanner.init();
      return currentScanner;
    } else {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    if(currentScanner != null) {
      currentScanner.close();
      currentScanner = null;
    }
    iterator = null;
    progress = 1.0f;
  }

  @Override
  public boolean isProjectable() {
    return projectable;
  }

  @Override
  public void setTarget(Column[] targets) {
    this.target = new Schema(targets);
  }

  @Override
  public boolean isSelectable() {
    return selectable;
  }

  @Override
  public void setSearchCondition(Object expr) {
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean isSplittable(){
    return false;
  }

  @Override
  public float getProgress() {
    if (currentScanner != null && iterator != null && tableStats.getNumBytes() > 0) {
      TableStats scannerTableStsts = currentScanner.getInputStats();
      long currentScannerReadBytes = 0;
      if (scannerTableStsts != null) {
        currentScannerReadBytes = scannerTableStsts.getReadBytes();
      }

      return (float)(tableStats.getReadBytes() + currentScannerReadBytes) / (float)tableStats.getNumBytes();
    } else {
      return progress;
    }
  }

  @Override
  public TableStats getInputStats() {
    return tableStats;
  }
}
