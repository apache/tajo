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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.BytesUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HBaseScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(HBaseScanner.class);
  private static final int DEFAULT_FETCH_SIZE = 1000;
  private static final int MAX_LIST_SIZE = 100;

  protected boolean inited = false;
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;
  private HBaseFragment fragment;
  private Scan scan;
  private HTableInterface htable;
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

  private int numRows = 0;

  private byte[][][] mappingColumnFamilies;
  private boolean[] isRowKeyMappings;
  private boolean[] isBinaryColumns;
  private boolean[] isColumnKeys;
  private boolean[] isColumnValues;

  private int[] rowKeyFieldIndexes;
  private char rowKeyDelimiter;

  public HBaseScanner (Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(meta);
    Preconditions.checkNotNull(fragment);
    Preconditions.checkArgument(conf instanceof TajoConf);

    this.conf = (TajoConf) conf;
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

    for (Column eachColumn : schema.getRootColumns()) {
      ColumnStats columnStats = new ColumnStats(eachColumn);
      tableStats.addColumnStat(columnStats);
    }

    scanFetchSize = Integer.parseInt(
        meta.getOption(HBaseStorageConstants.META_FETCH_ROWNUM_KEY, "" + DEFAULT_FETCH_SIZE));
    if (targets == null) {
      targets = schema.toArray();
    }

    columnMapping = new ColumnMapping(schema, meta.getOptions());
    targetIndexes = new int[targets.length];
    int index = 0;
    for (Column eachTargetColumn: targets) {
      targetIndexes[index++] = schema.getColumnId(eachTargetColumn.getQualifiedName());
    }

    mappingColumnFamilies = columnMapping.getMappingColumns();
    isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    isBinaryColumns = columnMapping.getIsBinaryColumns();
    isColumnKeys = columnMapping.getIsColumnKeys();
    isColumnValues = columnMapping.getIsColumnValues();

    rowKeyDelimiter = columnMapping.getRowKeyDelimiter();
    rowKeyFieldIndexes = columnMapping.getRowKeyFieldIndexes();

    HBaseTablespace space = (HBaseTablespace) TablespaceManager.get(fragment.getUri()).get();
    hbaseConf = space.getHbaseConf();
    initScanner();
  }

  private void initScanner() throws IOException {
    scan = new Scan();
    scan.setBatch(scanFetchSize);
    scan.setCacheBlocks(false);
    scan.setCaching(scanFetchSize);

    FilterList filters = null;
    if (targetIndexes == null || targetIndexes.length == 0) {
      filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filters.addFilter(new FirstKeyOnlyFilter());
      filters.addFilter(new KeyOnlyFilter());
    } else {
      boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
      for (int eachIndex : targetIndexes) {
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
    }

    scan.setStartRow(fragment.getStartRow());
    if (fragment.isLast() && fragment.getStopRow() != null &&
        fragment.getStopRow().length > 0) {
      // last and stopRow is not empty
      if (filters == null) {
        filters = new FilterList();
      }
      filters.addFilter(new InclusiveStopFilter(fragment.getStopRow()));
    } else {
      scan.setStopRow(fragment.getStopRow());
    }

    if (filters != null) {
      scan.setFilter(filters);
    }

    if (htable == null) {
      HConnection hconn = ((HBaseTablespace) TablespaceManager.get(fragment.getUri()).get()).getConnection();
      htable = hconn.getTable(fragment.getHbaseTableName());
    }
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
    Tuple resultTuple = new VTuple(targetIndexes.length);
    for (int i = 0; i < targetIndexes.length; i++) {
      resultTuple.put(i, getDatum(result, targetIndexes[i]));
    }
    numRows++;
    return resultTuple;
  }

  private Datum getDatum(Result result, int fieldId) throws IOException {
    byte[] value = null;
    if (isRowKeyMappings[fieldId]) {
      value = result.getRow();
      if (!isBinaryColumns[fieldId] && rowKeyFieldIndexes[fieldId] >= 0) {
        int rowKeyFieldIndex = rowKeyFieldIndexes[fieldId];

        byte[][] rowKeyFields = BytesUtils.splitPreserveAllTokens(
            value, rowKeyDelimiter, columnMapping.getNumColumns());

        if (rowKeyFields.length < rowKeyFieldIndex) {
          return NullDatum.get();
        } else {
          value = rowKeyFields[rowKeyFieldIndex];
        }
      }
    } else {
      if (isColumnKeys[fieldId]) {
        NavigableMap<byte[], byte[]> cfMap = result.getFamilyMap(mappingColumnFamilies[fieldId][0]);
        if (cfMap != null) {
          Set<byte[]> keySet = cfMap.keySet();
          if (keySet.size() == 1) {
            try {
              return HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], keySet.iterator().next());
            } catch (Exception e) {
              LOG.error(e.getMessage(), e);
              throw new RuntimeException(e.getMessage(), e);
            }
          } else {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            int count = 0;
            for (byte[] eachKey : keySet) {
              if (count > 0) {
                sb.append(", ");
              }
              Datum datum = HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], eachKey);
              sb.append("\"").append(datum.asChars()).append("\"");
              count++;
              if (count > MAX_LIST_SIZE) {
                break;
              }
            }
            sb.append("]");
            return new TextDatum(sb.toString());
          }
        }
      } else if (isColumnValues[fieldId]) {
        NavigableMap<byte[], byte[]> cfMap = result.getFamilyMap(mappingColumnFamilies[fieldId][0]);
        if (cfMap != null) {
          Collection<byte[]> valueList = cfMap.values();
          if (valueList.size() == 1) {
            try {
              return HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], valueList.iterator().next());
            } catch (Exception e) {
              LOG.error(e.getMessage(), e);
              throw new RuntimeException(e.getMessage(), e);
            }
          } else {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            int count = 0;
            for (byte[] eachValue : valueList) {
              if (count > 0) {
                sb.append(", ");
              }
              Datum datum = HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], eachValue);
              sb.append("\"").append(datum.asChars()).append("\"");
              count++;
              if (count > MAX_LIST_SIZE) {
                break;
              }
            }
            sb.append("]");
            return new TextDatum(sb.toString());
          }
        }
      } else {
        if (mappingColumnFamilies[fieldId][1] == null) {
          NavigableMap<byte[], byte[]> cfMap = result.getFamilyMap(mappingColumnFamilies[fieldId][0]);
          if (cfMap != null && !cfMap.isEmpty()) {
            int count = 0;
            String delim = "";

            if (cfMap.size() == 0) {
              return NullDatum.get();
            } else if (cfMap.size() == 1) {
              // If a column family is mapped without column name like "cf1:" and the number of cells is one,
              // return value is flat format not json format.
              NavigableMap.Entry<byte[], byte[]> entry = cfMap.entrySet().iterator().next();
              byte[] entryKey = entry.getKey();
              byte[] entryValue = entry.getValue();
              if (entryKey == null || entryKey.length == 0) {
                try {
                  if (isBinaryColumns[fieldId]) {
                    return HBaseBinarySerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue);
                  } else {
                    return HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue);
                  }
                } catch (Exception e) {
                  LOG.error(e.getMessage(), e);
                  throw new RuntimeException(e.getMessage(), e);
                }
              }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (NavigableMap.Entry<byte[], byte[]> entry : cfMap.entrySet()) {
              byte[] entryKey = entry.getKey();
              byte[] entryValue = entry.getValue();

              String keyText = new String(entryKey);
              String valueText = null;
              if (entryValue != null) {
                try {
                  if (isBinaryColumns[fieldId]) {
                    valueText = HBaseBinarySerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue).asChars();
                  } else {
                    valueText = HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue).asChars();
                  }
                } catch (Exception e) {
                  LOG.error(e.getMessage(), e);
                  throw new RuntimeException(e.getMessage(), e);
                }
              }
              sb.append(delim).append("\"").append(keyText).append("\":\"").append(valueText).append("\"");
              delim = ", ";
              count++;
              if (count > MAX_LIST_SIZE) {
                break;
              }
            } //end of for
            sb.append("}");
            return new TextDatum(sb.toString());
          } else {
            value = null;
          }
        } else {
          value = result.getValue(mappingColumnFamilies[fieldId][0], mappingColumnFamilies[fieldId][1]);
        }
      }
    }

    if (value == null) {
      return NullDatum.get();
    } else {
      try {
        if (isBinaryColumns[fieldId]) {
          return HBaseBinarySerializerDeserializer.deserialize(schemaColumns[fieldId], value);
        } else {
          return HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], value);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage(), e);
      }
    }
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
      scanner = null;
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
        scanner = null;
      } catch (Exception e) {
        LOG.warn("Error while closing hbase scanner: " + e.getMessage(), e);
      }
    }
    if (htable != null) {
      htable.close();
      htable = null;
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
  public void setFilter(EvalNode filter) {
    throw new UnsupportedException();
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
