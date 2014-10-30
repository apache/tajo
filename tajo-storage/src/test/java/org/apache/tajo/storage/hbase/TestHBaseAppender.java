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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.junit.Test;

public class TestHBaseAppender {
  @Test
  public void testOutputPath() throws Exception {
    Configuration taskConf = new Configuration();
    taskConf.set(FileOutputFormat.OUTDIR, "file:///tmp/tajo-babokim/test/q-1234");
    TaskAttemptContext context = new TaskAttemptContextImpl(taskConf,
        new TaskAttemptID("200707121733", 1, TaskType.MAP, 2, 3));
    HFileOutputFormat2 hFileOutputFormat2 = new HFileOutputFormat2();
    try {
      RecordWriter<ImmutableBytesWritable, Cell> writer = hFileOutputFormat2.getRecordWriter(context);
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      key.set("aaa".getBytes());

      KeyValue value = new KeyValue("aaa".getBytes(), "cf1".getBytes(), "cname".getBytes(), "value1".getBytes());
      writer.write(key, value);
      writer.close(context);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
