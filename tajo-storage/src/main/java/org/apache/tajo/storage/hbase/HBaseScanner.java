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

package org.apache.tajo.storage.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class HBaseScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(HBaseScanner.class);
  private static final int DEFAULT_FETCH_SZIE = 1000;

  protected boolean inited = false;
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;
  private HBaseFragment fragment;
  private Scan scan;
  private HTable htable;
  private Configuration hbaseConf;
  private Column[] targets;
  private TableStats tableStats;
  private ResultScanner scanner;
  private AtomicBoolean finished = new AtomicBoolean(false);
  private float progress = 0.0f;
  private int scanFetchSize;
  private Result[] scanResults;
  private int scanResultIndex = -1;
  private Column[] schemaColumns;

  private ColumnMapping columnMapping;
  private int[] targetIndexes;

  private HBaseTextSerializerDeserializer textSerde;
  private HBaseBinarySerializerDeserializer binarySerde;

  private int numRows = 0;

  public HBaseScanner (Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    this.conf = (TajoConf)conf;
    this.schema = schema;
    this.meta = meta;
    this.fragment = (HBaseFragment)fragment;
    this.tableStats = new TableStats();
  }

  @Override
  public void init() throws IOException {
    inited = true;
    schemaColumns = schema.toArray();
    if (fragment != null) {
      tableStats.setNumBytes(0);
      tableStats.setNumBlocks(1);
    }
    if (schema != null) {
      for(Column eachColumn: schema.getColumns()) {
        ColumnStats columnStats = new ColumnStats(eachColumn);
        tableStats.addColumnStat(columnStats);
      }
    }

    textSerde = new HBaseTextSerializerDeserializer();
    binarySerde = new HBaseBinarySerializerDeserializer();

    scanFetchSize = Integer.parseInt(meta.getOption("hbase.scanner.fetch,size", "" + DEFAULT_FETCH_SZIE));
    if (targets == null) {
      targets = schema.toArray();
    }

    columnMapping = new ColumnMapping(schema, meta);
    targetIndexes = new int[targets.length];
    int index = 0;
    for (Column eachTargetColumn: targets) {
      targetIndexes[index++] = schema.getColumnId(eachTargetColumn.getQualifiedName());
    }

    hbaseConf = HBaseStorageManager.getHBaseConfiguration(conf, meta);

    initScanner();
  }

  private void initScanner() throws IOException {
    scan = new Scan();
    scan.setBatch(scanFetchSize);
    scan.setStartRow(fragment.getStartRow());
    if (fragment.isLast() && fragment.getStopRow() != null &&
        fragment.getStopRow().length > 0) {
      // last and stopRow is not empty
      Filter filter = new InclusiveStopFilter(fragment.getStopRow());
      scan.setFilter(filter);
    } else {
      scan.setStopRow(fragment.getStopRow());
    }

    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    for (int eachIndex: targetIndexes) {
      if (isRowKeyMappings[eachIndex]) {
        continue;
      }
      byte[][] mappingColumn = columnMapping.getMappingColumns()[eachIndex];
      if (mappingColumn[1] == null) {
        scan.addFamily(mappingColumn[0]);
      } else {
        scan.addColumn(mappingColumn[0], mappingColumn[1]);
      }
    }

    htable = new HTable(hbaseConf, fragment.getHbaseTableName());
    scanner = htable.getScanner(scan);
  }

  @Override
  public Tuple next() throws IOException {
    if (finished.get()) {
      return null;
    }

    if (scanResults == null || scanResultIndex >= scanResults.length) {
      scanResults = scanner.next(scanFetchSize);
      if (scanResults == null || scanResults.length == 0) {
        finished.set(true);
        progress = 1.0f;
        return null;
      }
      scanResultIndex = 0;
    }

    Result result = scanResults[scanResultIndex++];
    numRows++;
    return new HBaseLazyTuple(columnMapping, schemaColumns, targetIndexes, textSerde, binarySerde, result);
  }

  @Override
  public void reset() throws IOException {
    progress = 0.0f;
    scanResultIndex = -1;
    scanResults = null;
    finished.set(false);
    tableStats = new TableStats();

    if (scanner != null) {
      scanner.close();
    }

    initScanner();
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
    finished.set(true);
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        LOG.warn("Error while closing hbase scanner: " + e.getMessage(), e);
      }
    }
    if (htable != null) {
      htable.close();
    }
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setSearchCondition(Object expr) {
    // TODO implements adding column filter to scanner.
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
    tableStats.setNumRows(numRows);
    return tableStats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
