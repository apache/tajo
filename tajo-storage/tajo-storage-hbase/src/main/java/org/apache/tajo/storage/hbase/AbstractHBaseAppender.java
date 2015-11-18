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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstract class for HBase appender.
 */
public abstract class AbstractHBaseAppender implements Appender {
  protected Configuration conf;
  protected Schema schema;
  protected TableMeta meta;
  protected TaskAttemptId taskAttemptId;
  protected Path stagingDir;
  protected boolean inited = false;

  protected ColumnMapping columnMapping;
  protected TableStatistics stats;
  protected boolean tableStatsEnabled;
  protected boolean[] columnStatsEnabled;

  protected int columnNum;

  protected byte[][][] mappingColumnFamilies;
  protected boolean[] isBinaryColumns;
  protected boolean[] isRowKeyMappings;
  protected boolean[] isColumnKeys;
  protected boolean[] isColumnValues;
  protected int[] rowkeyColumnIndexes;
  protected char rowKeyDelimiter;

  // the following four variables are used for '<cfname>:key:' or '<cfname>:value:' mapping
  protected int[] columnKeyValueDataIndexes;
  protected byte[][] columnKeyDatas;
  protected byte[][] columnValueDatas;
  protected byte[][] columnKeyCfNames;

  protected KeyValue[] keyValues;

  public AbstractHBaseAppender(Configuration conf, TaskAttemptId taskAttemptId,
                       Schema schema, TableMeta meta, Path stagingDir) {
    this.conf = conf;
    this.schema = schema;
    this.meta = meta;
    this.stagingDir = stagingDir;
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public void init() throws IOException {
    if (inited) {
      throw new IllegalStateException("FileAppender is already initialized.");
    }
    inited = true;
    if (tableStatsEnabled) {
      stats = new TableStatistics(this.schema, columnStatsEnabled);
    }
    try {
      columnMapping = new ColumnMapping(schema, meta.getOptions());
    } catch (TajoException e) {
      throw new TajoInternalError(e);
    }

    mappingColumnFamilies = columnMapping.getMappingColumns();

    isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    List<Integer> rowkeyColumnIndexList = new ArrayList<>();
    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (isRowKeyMappings[i]) {
        rowkeyColumnIndexList.add(i);
      }
    }
    rowkeyColumnIndexes = TUtil.toArray(rowkeyColumnIndexList);

    isBinaryColumns = columnMapping.getIsBinaryColumns();
    isColumnKeys = columnMapping.getIsColumnKeys();
    isColumnValues = columnMapping.getIsColumnValues();
    rowKeyDelimiter = columnMapping.getRowKeyDelimiter();

    this.columnNum = schema.size();

    // In the case of '<cfname>:key:' or '<cfname>:value:' KeyValue object should be set with the qualifier and value
    // which are mapped to the same column family.
    columnKeyValueDataIndexes = new int[isColumnKeys.length];
    int index = 0;
    int numKeyValues = 0;
    Map<String, Integer> cfNameIndexMap = new HashMap<>();
    for (int i = 0; i < isColumnKeys.length; i++) {
      if (isRowKeyMappings[i]) {
        continue;
      }
      if (isColumnKeys[i] || isColumnValues[i]) {
        String cfName = new String(mappingColumnFamilies[i][0]);
        if (!cfNameIndexMap.containsKey(cfName)) {
          cfNameIndexMap.put(cfName, index);
          columnKeyValueDataIndexes[i] = index;
          index++;
          numKeyValues++;
        } else {
          columnKeyValueDataIndexes[i] = cfNameIndexMap.get(cfName);
        }
      } else {
        numKeyValues++;
      }
    }
    columnKeyCfNames = new byte[cfNameIndexMap.size()][];
    for (Map.Entry<String, Integer> entry: cfNameIndexMap.entrySet()) {
      columnKeyCfNames[entry.getValue()] = entry.getKey().getBytes();
    }
    columnKeyDatas = new byte[cfNameIndexMap.size()][];
    columnValueDatas = new byte[cfNameIndexMap.size()][];

    keyValues = new KeyValue[numKeyValues];
  }

  private ByteArrayOutputStream bout = new ByteArrayOutputStream();

  protected byte[] getRowKeyBytes(Tuple tuple) throws IOException {
    Datum datum;
    byte[] rowkey;
    if (rowkeyColumnIndexes.length > 1) {
      bout.reset();
      for (int i = 0; i < rowkeyColumnIndexes.length; i++) {
        if (isBinaryColumns[rowkeyColumnIndexes[i]]) {
          rowkey = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(rowkeyColumnIndexes[i]), tuple, i);
        } else {
          rowkey = HBaseTextSerializerDeserializer.serialize(schema.getColumn(rowkeyColumnIndexes[i]), tuple, i);
        }
        bout.write(rowkey);
        if (i < rowkeyColumnIndexes.length - 1) {
          bout.write(rowKeyDelimiter);
        }
      }
      rowkey = bout.toByteArray();
    } else {
      int index = rowkeyColumnIndexes[0];
      if (isBinaryColumns[index]) {
        rowkey = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(index), tuple, index);
      } else {
        rowkey = HBaseTextSerializerDeserializer.serialize(schema.getColumn(index), tuple, index);
      }
    }

    return rowkey;
  }

  protected void readKeyValues(Tuple tuple, byte[] rowkey) throws IOException {
    int keyValIndex = 0;
    for (int i = 0; i < columnNum; i++) {
      if (isRowKeyMappings[i]) {
        continue;
      }
      byte[] value;
      if (isBinaryColumns[i]) {
        value = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(i), tuple, i);
      } else {
        value = HBaseTextSerializerDeserializer.serialize(schema.getColumn(i), tuple, i);
      }

      if (isColumnKeys[i]) {
        columnKeyDatas[columnKeyValueDataIndexes[i]] = value;
      } else if (isColumnValues[i]) {
        columnValueDatas[columnKeyValueDataIndexes[i]] = value;
      } else {
        keyValues[keyValIndex] = new KeyValue(rowkey, mappingColumnFamilies[i][0], mappingColumnFamilies[i][1], value);
        keyValIndex++;
      }
    }

    for (int i = 0; i < columnKeyDatas.length; i++) {
      keyValues[keyValIndex++] = new KeyValue(rowkey, columnKeyCfNames[i], columnKeyDatas[i], columnValueDatas[i]);
    }
  }

  @Override
  public void enableStats() {
    if (inited) {
      throw new IllegalStateException("Should enable this option before init()");
    }

    this.tableStatsEnabled = true;
    this.columnStatsEnabled = new boolean[schema.size()];
  }

  @Override
  public void enableStats(List<Column> columnList) {
    enableStats();
    columnList.forEach(column -> columnStatsEnabled[schema.getIndex(column)] = true);
  }

  @Override
  public TableStats getStats() {
    if (tableStatsEnabled) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
