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

package org.apache.tajo.storage.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

/**
 * FileAppender for writing to Parquet files.
 */
public class ParquetAppender extends FileAppender {
  private TajoParquetWriter writer;
  private int blockSize;
  private int pageSize;
  private CompressionCodecName compressionCodecName;
  private boolean enableDictionary;
  private boolean validating;
  private TableStatistics stats;

  /**
   * Creates a new ParquetAppender.
   *
   * @param conf Configuration properties.
   * @param schema The table schema.
   * @param meta The table metadata.
   * @param workDir The path of the Parquet file to write to.
   */
  public ParquetAppender(Configuration conf, TaskAttemptId taskAttemptId, Schema schema, TableMeta meta,
                         Path workDir) throws IOException {
    super(conf, taskAttemptId, schema, meta, workDir);
    this.blockSize = Integer.parseInt(
        meta.getProperty(ParquetOutputFormat.BLOCK_SIZE, StorageConstants.PARQUET_DEFAULT_BLOCK_SIZE));
    this.pageSize = Integer.parseInt(
        meta.getProperty(ParquetOutputFormat.PAGE_SIZE, StorageConstants.PARQUET_DEFAULT_PAGE_SIZE));
    this.compressionCodecName = CompressionCodecName.fromConf(
        meta.getProperty(ParquetOutputFormat.COMPRESSION, StorageConstants.PARQUET_DEFAULT_COMPRESSION_CODEC_NAME));
    this.enableDictionary = Boolean.parseBoolean(
        meta.getProperty(ParquetOutputFormat.ENABLE_DICTIONARY, StorageConstants.PARQUET_DEFAULT_IS_DICTIONARY_ENABLED));
    this.validating = Boolean.parseBoolean(
        meta.getProperty(ParquetOutputFormat.VALIDATION, StorageConstants.PARQUET_DEFAULT_IS_VALIDATION_ENABLED));
  }

  /**
   * Initializes the Appender. This method creates a new TajoParquetWriter
   * and initializes the table statistics if enabled.
   */
  public void init() throws IOException {
    writer = new TajoParquetWriter(path,
                                   schema,
                                   compressionCodecName,
                                   blockSize,
                                   pageSize,
                                   enableDictionary,
                                   validating);
    if (tableStatsEnabled) {
      this.stats = new TableStatistics(schema, columnStatsEnabled);
    }
    super.init();
  }

  /**
   * Write a Tuple to the Parquet file.
   *
   * @param tuple The Tuple to write.
   */
  @Override
  public void addTuple(Tuple tuple) throws IOException {
    writer.write(tuple);
    if (tableStatsEnabled) {
      stats.incrementRow();
    }
  }

  /**
   * The ParquetWriter does not need to be flushed, so this is a no-op.
   */
  @Override
  public void flush() throws IOException {
  }

  /**
   * Closes the Appender.
   */
  @Override
  public void close() throws IOException {
    IOUtils.cleanup(null, writer);

    // TODO: getOffset is not implemented yet
//    if (tableStatsEnabled) {
//      stats.setNumBytes(getOffset());
//    }
  }

  public long getEstimatedOutputSize() throws IOException {
    return writer.getDataSize();
  }

  /**
   * If table statistics is enabled, retrieve the table statistics.
   *
   * @return Table statistics if enabled or null otherwise.
   */
  @Override
  public TableStats getStats() {
    if (tableStatsEnabled) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
