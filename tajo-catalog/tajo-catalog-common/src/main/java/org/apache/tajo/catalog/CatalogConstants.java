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

package org.apache.tajo.catalog;

public class CatalogConstants {
  public static final String STORE_CLASS="tajo.catalog.store.class";

  public static final String CONNECTION_ID = "tajo.catalog.connection.id";
  public static final String CONNECTION_PASSWORD = "tajo.catalog.connection.password";
  public static final String CATALOG_URI="tajo.catalog.uri";

  public static final String DEPRECATED_CONNECTION_ID = "tajo.catalog.jdbc.connection.id";
  public static final String DEPRECATED_CONNECTION_PASSWORD = "tajo.catalog.jdbc.connection.password";
  public static final String DEPRECATED_CATALOG_URI="tajo.catalog.jdbc.uri";

  public static final String TB_META = "META";
  public static final String TB_SPACES = "TABLESPACES";
  public static final String TB_DATABASES = "DATABASES_";
  public static final String TB_TABLES = "TABLES";
  public static final String TB_COLUMNS = "COLUMNS";
  public static final String TB_OPTIONS = "OPTIONS";
  public static final String TB_INDEXES = "INDEXES";
  public static final String TB_STATISTICS = "STATS";
  public static final String TB_PARTITION_METHODS = "PARTITION_METHODS";
  public static final String TB_PARTTIONS = "PARTITIONS";

  public static final String COL_TABLESPACE_PK = "SPACE_ID";
  public static final String COL_DATABASES_PK = "DB_ID";
  public static final String COL_TABLES_PK = "TID";
  public static final String COL_TABLES_NAME = "TABLE_NAME";

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
  public static final String DEFAULT_BINARY_SERDE = "org.apache.tajo.storage.BinarySerializerDeserializer";
  public static final String DEFAULT_TEXT_SERDE = "org.apache.tajo.storage.TextSerializerDeserializer";
}
