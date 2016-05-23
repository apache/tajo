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
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.thirdparty.orc.OrcFile;
import org.apache.tajo.storage.thirdparty.orc.OrcFile.EncodingStrategy;
import org.apache.tajo.storage.thirdparty.orc.OrcUtils;
import org.apache.tajo.storage.thirdparty.orc.Writer;

import java.io.IOException;
import java.util.Properties;
import java.util.TimeZone;

public class ORCAppender extends FileAppender {
  private Writer writer;
  private TableStatistics stats;
  private TimeZone timezone;

  public ORCAppender(Configuration conf, TaskAttemptId taskAttemptId, Schema schema,
                     TableMeta meta, Path workDir) {
    super(conf, taskAttemptId, schema, meta, workDir);
  }

  @Override
  public void init() throws IOException {
    timezone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
        StorageUtil.TAJO_CONF.getSystemTimezone().getID()));

    writer = OrcFile.createWriter(path, buildWriterOptions(conf, meta, schema), timezone);

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
    return writer.getRawDataSize();
  }

  private static OrcFile.WriterOptions buildWriterOptions(Configuration conf, TableMeta meta, Schema schema) {
    return OrcFile.writerOptions(conf)
        .setSchema(OrcUtils.convertSchema(schema))
        .compress(getCompressionKind(meta))
        .stripeSize(Long.parseLong(meta.getProperty(OrcConf.STRIPE_SIZE.getAttribute(),
            String.valueOf(OrcConf.STRIPE_SIZE.getDefaultValue()))))
        .blockSize(Long.parseLong(meta.getProperty(OrcConf.BLOCK_SIZE.getAttribute(),
            String.valueOf(OrcConf.BLOCK_SIZE.getDefaultValue()))))
        .rowIndexStride(Integer.parseInt(meta.getProperty(OrcConf.ROW_INDEX_STRIDE.getAttribute(),
            String.valueOf(OrcConf.ROW_INDEX_STRIDE.getDefaultValue()))))
        .bufferSize(Integer.parseInt(meta.getProperty(OrcConf.BUFFER_SIZE.getAttribute(),
            String.valueOf(OrcConf.BUFFER_SIZE.getDefaultValue()))))
        .blockPadding(Boolean.parseBoolean(meta.getProperty(OrcConf.BLOCK_PADDING.getAttribute(),
            String.valueOf(OrcConf.BLOCK_PADDING.getDefaultValue()))))
        .encodingStrategy(EncodingStrategy.valueOf(meta.getProperty(OrcConf.ENCODING_STRATEGY.getAttribute(),
            String.valueOf(OrcConf.ENCODING_STRATEGY.getDefaultValue()))))
        .bloomFilterFpp(Double.parseDouble(meta.getProperty(OrcConf.BLOOM_FILTER_FPP.getAttribute(),
            String.valueOf(OrcConf.BLOOM_FILTER_FPP.getDefaultValue()))))
        .bloomFilterColumns(meta.getProperty(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(),
            String.valueOf(OrcConf.BLOOM_FILTER_COLUMNS.getDefaultValue())));
  }

  private static CompressionKind getCompressionKind(TableMeta meta) {
    String kindstr = meta.getProperty(OrcConf.COMPRESS.getAttribute(),
        String.valueOf(OrcConf.COMPRESS.getDefaultValue()));

    if (kindstr.equalsIgnoreCase(CompressionKind.ZLIB.name())) {
      return CompressionKind.ZLIB;
    }

    if (kindstr.equalsIgnoreCase(CompressionKind.SNAPPY.name())) {
      return CompressionKind.SNAPPY;
    }

    if (kindstr.equalsIgnoreCase(CompressionKind.LZO.name())) {
      return CompressionKind.LZO;
    }

    return CompressionKind.NONE;
  }

  /**
   * Options for creating ORC file writers.
   */
  public static class WriterOptions extends OrcFile.WriterOptions {
    // Setting the default batch size to 1000 makes the memory check at 5000
    // rows work the same as the row by row writer. (If it was the default 1024,
    // the smallest stripe size would be 5120 rows, which changes the output
    // of some of the tests.)
    private int batchSize = 1000;

    public WriterOptions(Properties tableProperties, Configuration conf) {
      super(tableProperties, conf);
    }

    /**
     * Set the schema for the file. This is a required parameter.
     * @param schema the schema for the file.
     * @return this
     */
    public WriterOptions setSchema(TypeDescription schema) {
      super.setSchema(schema);
      return this;
    }

    protected WriterOptions batchSize(int maxSize) {
      batchSize = maxSize;
      return this;
    }

    int getBatchSize() {
      return batchSize;
    }
  }
}
