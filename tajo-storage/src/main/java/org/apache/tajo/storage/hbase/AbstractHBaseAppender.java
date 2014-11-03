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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractHBaseAppender implements Appender {
  protected Configuration conf;
  protected Schema schema;
  protected TableMeta meta;
  protected QueryUnitAttemptId taskAttemptId;
  protected Path stagingDir;
  protected boolean inited = false;

  protected ColumnMapping columnMapping;
  protected TableStatistics stats;
  protected boolean enabledStats;

  protected int columnNum;

  protected byte[][][] mappingColumnFamilies;
  protected boolean[] isBinaryColumns;
  protected boolean[] isRowKeyMappings;
  protected int[] rowKeyFieldIndexes;
  protected int[] rowkeyColumnIndexes;
  protected char rowKeyDelimiter;

  public AbstractHBaseAppender(Configuration conf, QueryUnitAttemptId taskAttemptId,
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
    if (enabledStats) {
      stats = new TableStatistics(this.schema);
    }
    columnMapping = new ColumnMapping(schema, meta);

    mappingColumnFamilies = columnMapping.getMappingColumns();

    isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    List<Integer> rowkeyColumnIndexList = new ArrayList<Integer>();
    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (isRowKeyMappings[i]) {
        rowkeyColumnIndexList.add(i);
      }
    }
    rowkeyColumnIndexes = TUtil.toArray(rowkeyColumnIndexList);

    isBinaryColumns = columnMapping.getIsBinaryColumns();
    rowKeyDelimiter = columnMapping.getRowKeyDelimiter();
    rowKeyFieldIndexes = columnMapping.getRowKeyFieldIndexes();

    this.columnNum = schema.size();
  }

  private ByteArrayOutputStream bout = new ByteArrayOutputStream();

  protected byte[] getRowKeyBytes(Tuple tuple) throws IOException {
    Datum datum;
    byte[] rowkey;
    if (rowkeyColumnIndexes.length > 1) {
      bout.reset();
      for (int i = 0; i < rowkeyColumnIndexes.length; i++) {
        datum = tuple.get(rowkeyColumnIndexes[i]);
        if (isBinaryColumns[rowkeyColumnIndexes[i]]) {
          rowkey = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(rowkeyColumnIndexes[i]), datum);
        } else {
          rowkey = HBaseTextSerializerDeserializer.serialize(schema.getColumn(rowkeyColumnIndexes[i]), datum);
        }
        bout.write(rowkey);
        if (i < rowkeyColumnIndexes.length - 1) {
          bout.write(rowKeyDelimiter);
        }
      }
      rowkey = bout.toByteArray();
    } else {
      int index = rowkeyColumnIndexes[0];
      datum = tuple.get(index);
      if (isBinaryColumns[index]) {
        rowkey = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(index), datum);
      } else {
        rowkey = HBaseTextSerializerDeserializer.serialize(schema.getColumn(index), datum);
      }
    }

    return rowkey;
  }

  protected KeyValue getKeyValue(Tuple tuple, int index, byte[] rowkey) throws IOException {
    Datum datum = tuple.get(index);
    byte[] value;
    if (isBinaryColumns[index]) {
      value = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(index), datum);
    } else {
      value = HBaseTextSerializerDeserializer.serialize(schema.getColumn(index), datum);
    }
    return new KeyValue(rowkey, mappingColumnFamilies[index][0], mappingColumnFamilies[index][1], value);
  }

  @Override
  public void enableStats() {
    enabledStats = true;
  }

  @Override
  public TableStats getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
