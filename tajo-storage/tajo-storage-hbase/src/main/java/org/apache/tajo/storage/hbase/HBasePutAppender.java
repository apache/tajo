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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.net.URI;

public class HBasePutAppender extends AbstractHBaseAppender {
  private URI uri;
  private HTableInterface htable;
  private long totalNumBytes;

  public HBasePutAppender(Configuration conf, URI uri, TaskAttemptId taskAttemptId,
                          Schema schema, TableMeta meta, Path stagingDir) {
    super(conf, taskAttemptId, schema, meta, stagingDir);
    this.uri = uri;
  }

  @Override
  public void init() throws IOException {
    super.init();

    HBaseTablespace space = (HBaseTablespace) TablespaceManager.get(uri).get();
    HConnection hconn = space.getConnection();
    htable = hconn.getTable(columnMapping.getHbaseTableName());
    htable.setAutoFlushTo(false);
    htable.setWriteBufferSize(5 * 1024 * 1024);
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    byte[] rowkey = getRowKeyBytes(tuple);
    totalNumBytes += rowkey.length;
    Put put = new Put(rowkey);
    readKeyValues(tuple, rowkey);

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
        put.add(mappingColumnFamilies[i][0], mappingColumnFamilies[i][1], value);
        totalNumBytes += value.length;
      }
    }

    for (int i = 0; i < columnKeyDatas.length; i++) {
     put.add(columnKeyCfNames[i], columnKeyDatas[i], columnValueDatas[i]);
      totalNumBytes += columnKeyDatas[i].length + columnValueDatas[i].length;
    }

    htable.put(put);

    if (enabledStats) {
      stats.incrementRow();
      stats.setNumBytes(totalNumBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    htable.flushCommits();
  }

  @Override
  public long getEstimatedOutputSize() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (htable != null) {
      htable.flushCommits();
      htable.close();
    }
    if (enabledStats) {
      stats.setNumBytes(totalNumBytes);
    }
  }
}
