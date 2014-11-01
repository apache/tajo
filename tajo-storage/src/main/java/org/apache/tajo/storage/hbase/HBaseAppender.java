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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.TUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class HBaseAppender implements Appender {
  private static final Log LOG = LogFactory.getLog(HBaseAppender.class);

  private RecordWriter<ImmutableBytesWritable, Cell> writer;
  private Configuration conf;
  private Schema schema;
  private TableMeta meta;
  private QueryUnitAttemptId taskAttemptId;
  private Path stagingDir;
  private boolean inited = false;
  private TaskAttemptContext writerContext;
  private int columnNum;
  private ColumnMapping columnMapping;
  private TableStatistics stats;
  private boolean enabledStats;

  private byte[][][] mappingColumnFamilies;
  private boolean[] isBinaryColumns;
  private boolean[] isRowKeyMappings;
  private int[] rowKeyFieldIndexes;
  private int[] rowkeyColumnIndexes;
  private char rowKeyDelimiter;

  private Path workingFilePath;
  private FileOutputCommitter committer;

  public HBaseAppender (Configuration conf, QueryUnitAttemptId taskAttemptId,
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

    Configuration taskConf = new Configuration();
    taskConf.set(FileOutputFormat.OUTDIR, stagingDir.toString());

    ExecutionBlockId ebId = taskAttemptId.getQueryUnitId().getExecutionBlockId();
    writerContext = new TaskAttemptContextImpl(taskConf,
        new TaskAttemptID(ebId.getQueryId().toString(), ebId.getId(), TaskType.MAP,
            taskAttemptId.getQueryUnitId().getId(), taskAttemptId.getId()));

    HFileOutputFormat2 hFileOutputFormat2 = new HFileOutputFormat2();
    try {
      writer = hFileOutputFormat2.getRecordWriter(writerContext);

      committer = new FileOutputCommitter(FileOutputFormat.getOutputPath(writerContext), writerContext);
      workingFilePath = committer.getWorkPath();
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }

    LOG.info("Created hbase file writer: " + workingFilePath);
  }

  long totalNumBytes = 0;
  ByteArrayOutputStream bout = new ByteArrayOutputStream();
  ImmutableBytesWritable keyWritable = new ImmutableBytesWritable();
  boolean first = true;
  TreeSet<KeyValue> kvSet = new TreeSet<KeyValue>(KeyValue.COMPARATOR);

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    Datum datum;

    byte[] rowkey;
    // make rowkey
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
    if (!first && !Bytes.equals(keyWritable.get(), 0, keyWritable.getLength(), rowkey, 0, rowkey.length)) {
      try {
        for (KeyValue kv : kvSet) {
          writer.write(keyWritable, kv);
          totalNumBytes += keyWritable.getLength() + keyWritable.getLength();
        }
        kvSet.clear();
        // Statistical section
        if (enabledStats) {
          stats.incrementRow();
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    first = false;

    keyWritable.set(rowkey);
    for (int i = 0; i < columnNum; i++) {
      if (isRowKeyMappings[i]) {
        continue;
      }
      datum = tuple.get(i);
      byte[] value;
      if (isBinaryColumns[i]) {
        value = HBaseBinarySerializerDeserializer.serialize(schema.getColumn(i), datum);
      } else {
        value = HBaseTextSerializerDeserializer.serialize(schema.getColumn(i), datum);
      }
      KeyValue keyValue = new KeyValue(rowkey, mappingColumnFamilies[i][0], mappingColumnFamilies[i][1], value);
      kvSet.add(keyValue);
    }
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public long getEstimatedOutputSize() throws IOException {
    // StoreTableExec uses this value as rolling file length
    // Not rolling
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (!kvSet.isEmpty()) {
      try {
        for (KeyValue kv : kvSet) {
          writer.write(keyWritable, kv);
          totalNumBytes += keyWritable.getLength() + keyWritable.getLength();
        }
        kvSet.clear();
        // Statistical section
        if (enabledStats) {
          stats.incrementRow();
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    if (enabledStats) {
      stats.setNumBytes(totalNumBytes);
    }
    if (writer != null) {
      try {
        writer.close(writerContext);
        committer.commitTask(writerContext);
      } catch (InterruptedException e) {
      }
    }
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
