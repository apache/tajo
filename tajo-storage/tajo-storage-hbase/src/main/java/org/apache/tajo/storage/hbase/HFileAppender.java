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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Bytes;

import java.io.IOException;
import java.util.TreeSet;

public class HFileAppender extends AbstractHBaseAppender {
  private static final Log LOG = LogFactory.getLog(HFileAppender.class);

  private RecordWriter<ImmutableBytesWritable, Cell> writer;
  private TaskAttemptContext writerContext;
  private Path workingFilePath;
  private FileOutputCommitter committer;

  public HFileAppender(Configuration conf, TaskAttemptId taskAttemptId,
                       Schema schema, TableMeta meta, Path stagingDir) {
    super(conf, taskAttemptId, schema, meta, stagingDir);
  }

  @Override
  public void init() throws IOException {
    super.init();

    Configuration taskConf = new Configuration();
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    taskConf.set(FileOutputFormat.OUTDIR, stagingResultDir.toString());

    ExecutionBlockId ebId = taskAttemptId.getTaskId().getExecutionBlockId();
    writerContext = new TaskAttemptContextImpl(taskConf,
        new TaskAttemptID(ebId.getQueryId().toString(), ebId.getId(), TaskType.MAP,
            taskAttemptId.getTaskId().getId(), taskAttemptId.getId()));

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
  ImmutableBytesWritable keyWritable = new ImmutableBytesWritable();
  boolean first = true;
  TreeSet<KeyValue> kvSet = new TreeSet<>(KeyValue.COMPARATOR);


  @Override
  public void addTuple(Tuple tuple) throws IOException {
    Datum datum;

    byte[] rowkey = getRowKeyBytes(tuple);

    if (!first && !Bytes.equals(keyWritable.get(), 0, keyWritable.getLength(), rowkey, 0, rowkey.length)) {
      try {
        for (KeyValue kv : kvSet) {
          writer.write(keyWritable, kv);
          totalNumBytes += keyWritable.getLength() + kv.getLength();
        }
        kvSet.clear();
        // Statistical section
        if (tableStatsEnabled) {
          stats.incrementRow();
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    first = false;

    keyWritable.set(rowkey);

    readKeyValues(tuple, rowkey);
    if (keyValues != null) {
      for (KeyValue eachKeyVal: keyValues) {
        kvSet.add(eachKeyVal);
      }
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
        if (tableStatsEnabled) {
          stats.incrementRow();
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    if (tableStatsEnabled) {
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
}
