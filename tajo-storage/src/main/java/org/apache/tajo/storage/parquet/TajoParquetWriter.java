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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.thirdparty.parquet.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

/**
 * Tajo implementation of {@link ParquetWriter} to write Tajo records to a
 * Parquet file. Users should use {@link ParquetAppender} and not this class
 * directly.
 */
public class TajoParquetWriter extends ParquetWriter<Tuple> {
  /**
   * Create a new TajoParquetWriter
   *
   * @param file The file name to write to.
   * @param schema The Tajo schema of the table.
   * @param compressionCodecName Compression codec to use, or
   *                             CompressionCodecName.UNCOMPRESSED.
   * @param blockSize The block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages
   *                 for alignment.
   * @throws IOException
   */
  public TajoParquetWriter(Path file,
                           Schema schema,
                           CompressionCodecName compressionCodecName,
                           int blockSize,
                           int pageSize) throws IOException {
    super(file,
          new TajoWriteSupport(schema),
          compressionCodecName,
          blockSize,
          pageSize);
  }

  /**
   * Create a new TajoParquetWriter.
   *
   * @param file The file name to write to.
   * @param schema The Tajo schema of the table.
   * @param compressionCodecName Compression codec to use, or
   *                             CompressionCodecName.UNCOMPRESSED.
   * @param blockSize The block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages
   *                 for alignment.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @param validating Whether to turn on validation.
   * @throws IOException
   */
  public TajoParquetWriter(Path file,
                           Schema schema,
                           CompressionCodecName compressionCodecName,
                           int blockSize,
                           int pageSize,
                           boolean enableDictionary,
                           boolean validating) throws IOException {
    super(file,
          new TajoWriteSupport(schema),
          compressionCodecName,
          blockSize,
          pageSize,
          enableDictionary,
          validating);
  }

  /**
   * Creates a new TajoParquetWriter. The default block size is 128 MB.
   * The default page size is 1 MB. Default compression is no compression.
   *
   * @param file The Path of the file to write to.
   * @param schema The Tajo schema of the table.
   * @throws IOException
   */
  public TajoParquetWriter(Path file, Schema schema) throws IOException {
    this(file,
         schema,
         CompressionCodecName.UNCOMPRESSED,
         DEFAULT_BLOCK_SIZE,
         DEFAULT_PAGE_SIZE);
  }
}
