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

package org.apache.tajo.storage;

import java.net.URI;

public class StorageConstants {

  // Tablespace  -------------------------------------------------

  public static final URI LOCAL_FS_URI = URI.create("file:/");

  // Common table properties -------------------------------------------------

  // Insert
  public static final String INSERT_DIRECTLY = "insert.direct";

  // time zone
  public static final String TIMEZONE = "timezone";

  // compression
  public static final String COMPRESSION_CODEC = "compression.codec";
  public static final String COMPRESSION_TYPE = "compression.type";

  // Text file properties -------------------------------------------------
  @Deprecated
  public static final String CSVFILE_SERDE = "csvfile.serde";

  public static final String TEXT_DELIMITER = "text.delimiter";
  public static final String TEXT_NULL = "text.null";
  public static final String TEXT_SERDE_CLASS = "text.serde";
  public static final String DEFAULT_TEXT_SERDE_CLASS = "org.apache.tajo.storage.text.CSVLineSerDe";

  public static final String TEXT_SKIP_HEADER_LINE = "text.skip.headerlines";

  /**
   * It's the maximum number of parsing error torrence.
   *
   * <ul>
   *   <li>If it is -1, it is always torrent against any parsing error.</li>
   *   <li>If it is 0, it does not permit any parsing error.</li>
   *   <li>If it is some positive integer (i.e., > 0), the given number of parsing errors in each
   *       task will be permissible</li>
   * </ul>
   **/
  public static final String TEXT_ERROR_TOLERANCE_MAXNUM = "text.error-tolerance.max-num";
  public static final String DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM = "0";

  // Sequence file properties -------------------------------------------------
  @Deprecated
  public static final String SEQUENCEFILE_DELIMITER = "sequencefile.delimiter";
  @Deprecated
  public static final String SEQUENCEFILE_NULL = "sequencefile.null";
  public static final String SEQUENCEFILE_SERDE = "sequencefile.serde";

  // RC file properties -------------------------------------------------
  @Deprecated
  public static final String RCFILE_NULL = "rcfile.null";
  public static final String RCFILE_SERDE = "rcfile.serde";

  public static final String DEFAULT_FIELD_DELIMITER = "|";
  public static final String DEFAULT_BINARY_SERDE = "org.apache.tajo.storage.BinarySerializerDeserializer";
  public static final String DEFAULT_TEXT_SERDE = "org.apache.tajo.storage.TextSerializerDeserializer";

  // ORC file properties -------------------------------------------------
  public static final String ORC_MAX_MERGE_DISTANCE = "orc.max.merge.distance";
  public static final String DEFAULT_ORC_MAX_MERGE_DISTANCE = "1048576";  // 1MB

  public static final String ORC_STRIPE_SIZE = "orc.stripe.size";
  public static final String DEFAULT_ORC_STRIPE_SIZE = "67108864"; // 64MB

  public static final String ORC_COMPRESSION_KIND = "orc.compression.kind";
  public static final String ORC_COMPRESSION_KIND_NONE = "none";
  public static final String ORC_COMPRESSION_KIND_SNAPPY = "snappy";
  public static final String ORC_COMPRESSION_KIND_LZO = "lzo";
  public static final String ORC_COMPRESSION_KIND_ZIP = "zlip";
  public static final String DEFAULT_ORC_COMPRESSION_KIND = ORC_COMPRESSION_KIND_NONE;

  public static final String ORC_BUFFER_SIZE = "orc.buffer.size";
  public static final String DEFAULT_ORC_BUFFER_SIZE = "262144"; // 256KB

  public static final String ORC_ROW_INDEX_STRIDE = "orc.rowindex.stride";
  public static final String DEFAULT_ORC_ROW_INDEX_STRIDE = "10000";

  // Parquet file properties -------------------------------------------------
  public static final String PARQUET_DEFAULT_BLOCK_SIZE;
  public static final String PARQUET_DEFAULT_PAGE_SIZE;
  public static final String PARQUET_DEFAULT_COMPRESSION_CODEC_NAME;
  public static final String PARQUET_DEFAULT_IS_DICTIONARY_ENABLED;
  public static final String PARQUET_DEFAULT_IS_VALIDATION_ENABLED;

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;


  // Avro file properties -------------------------------------------------
  public static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
  public static final String AVRO_SCHEMA_URL = "avro.schema.url";

  // Internal storage properties -------------------------------------------------
  public static final String SHUFFLE_TYPE = "shuffle.type";

  static {
    PARQUET_DEFAULT_BLOCK_SIZE = Integer.toString(DEFAULT_BLOCK_SIZE);
    PARQUET_DEFAULT_PAGE_SIZE = Integer.toString(DEFAULT_PAGE_SIZE);

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME.
    PARQUET_DEFAULT_COMPRESSION_CODEC_NAME = "uncompressed";
        // CompressionCodecName.UNCOMPRESSED.name().toLowerCase();

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED.
    PARQUET_DEFAULT_IS_DICTIONARY_ENABLED = "true";

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED.
    PARQUET_DEFAULT_IS_VALIDATION_ENABLED = "false";
  }
}
