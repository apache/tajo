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

package org.apache.tajo.storage.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.orc.objectinspector.ObjectInspectorFactory;
import org.apache.tajo.storage.thirdparty.orc.CompressionKind;
import org.apache.tajo.storage.thirdparty.orc.OrcFile;
import org.apache.tajo.storage.thirdparty.orc.Writer;

import java.io.IOException;
import java.util.TimeZone;

public class ORCAppender extends FileAppender {
  private Writer writer;
  private TableStatistics stats;
  private TimeZone timezone;

  public ORCAppender(Configuration conf, TaskAttemptId taskAttemptId, Schema schema,
                     TableMeta meta, Path workDir) {
    super(conf, taskAttemptId, schema, meta, workDir);

    timezone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
        TajoConstants.DEFAULT_SYSTEM_TIMEZONE));
  }

  @Override
  public void init() throws IOException {
    writer = OrcFile.createWriter(workDir.getFileSystem(conf), path, conf,
      ObjectInspectorFactory.buildStructObjectInspector(schema),
      Long.parseLong(meta.getProperty(StorageConstants.ORC_STRIPE_SIZE,
        StorageConstants.DEFAULT_ORC_STRIPE_SIZE)), getCompressionKind(),
      Integer.parseInt(meta.getProperty(StorageConstants.ORC_BUFFER_SIZE,
        StorageConstants.DEFAULT_ORC_BUFFER_SIZE)),
      Integer.parseInt(meta.getProperty(StorageConstants.ORC_ROW_INDEX_STRIDE,
        StorageConstants.DEFAULT_ORC_ROW_INDEX_STRIDE)),
      timezone);

    if (tableStatsEnabled) {
      this.stats = new TableStatistics(schema, columnStatsEnabled);
    }

    super.init();
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    if (tableStatsEnabled) {
      for (int i = 0; i < schema.size(); ++i) {
        stats.analyzeField(i, tuple);
      }
    }
    writer.addTuple(tuple);
    if (tableStatsEnabled) {
      stats.incrementRow();
    }
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void close() throws IOException {
    writer.close();

    // TODO: getOffset is not implemented yet
//    if (tableStatsEnabled) {
//      stats.setNumBytes(getOffset());
//    }
  }

  @Override
  public TableStats getStats() {
    if (tableStatsEnabled) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }

  @Override
  public long getEstimatedOutputSize() throws IOException {
    return writer.getRawDataSize() * writer.getNumberOfRows();
  }

  private CompressionKind getCompressionKind() {
    String kindstr = meta.getProperty(StorageConstants.ORC_COMPRESSION, StorageConstants.DEFAULT_ORC_COMPRESSION_KIND);

    if (kindstr.equalsIgnoreCase(StorageConstants.ORC_COMPRESSION_KIND_ZIP)) {
      return CompressionKind.ZLIB;
    }

    if (kindstr.equalsIgnoreCase(StorageConstants.ORC_COMPRESSION_KIND_SNAPPY)) {
      return CompressionKind.SNAPPY;
    }

    if (kindstr.equalsIgnoreCase(StorageConstants.ORC_COMPRESSION_KIND_LZO)) {
      return CompressionKind.LZO;
    }

    return CompressionKind.NONE;
  }
}
