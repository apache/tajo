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

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

public class StorageConstants {
  // table options
  public static final String COMPRESSION_CODEC = "compression.codec";
  public static final String COMPRESSION_TYPE = "compression.type";

  public static final String CSVFILE_DELIMITER = "csvfile.delimiter";
  public static final String CSVFILE_NULL = "csvfile.null";
  public static final String CSVFILE_SERDE = "csvfile.serde";


  public static final String SEQUENCEFILE_DELIMITER = "sequencefile.delimiter";
  public static final String SEQUENCEFILE_NULL = "sequencefile.null";
  public static final String SEQUENCEFILE_SERDE = "sequencefile.serde";

  public static final String RCFILE_NULL = "rcfile.null";
  public static final String RCFILE_SERDE = "rcfile.serde";

  public static final String DEFAULT_FIELD_DELIMITER = "|";
  public static final String DEFAULT_BINARY_SERDE = BinarySerializerDeserializer.class.getName();
  public static final String DEFAULT_TEXT_SERDE = TextSerializerDeserializer.class.getName();

  public static final String PARQUET_DEFAULT_BLOCK_SIZE;
  public static final String PARQUET_DEFAULT_PAGE_SIZE;
  public static final String PARQUET_DEFAULT_COMPRESSION_CODEC_NAME;
  public static final String PARQUET_DEFAULT_IS_DICTIONARY_ENABLED;
  public static final String PARQUET_DEFAULT_IS_VALIDATION_ENABLED;

  static {
    PARQUET_DEFAULT_BLOCK_SIZE =
        Integer.toString(ParquetWriter.DEFAULT_BLOCK_SIZE);
    PARQUET_DEFAULT_PAGE_SIZE =
        Integer.toString(ParquetWriter.DEFAULT_PAGE_SIZE);

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME.
    PARQUET_DEFAULT_COMPRESSION_CODEC_NAME =
        CompressionCodecName.UNCOMPRESSED.name().toLowerCase();

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED.
    PARQUET_DEFAULT_IS_DICTIONARY_ENABLED = "true";

    // When parquet-hadoop 1.3.3 is available, this should be changed to
    // ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED.
    PARQUET_DEFAULT_IS_VALIDATION_ENABLED = "false";
  }
}
