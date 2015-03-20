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
  public final static String IDENTIFIER_DELIMITER_REGEXP = "\\.";
  public final static String IDENTIFIER_DELIMITER = ".";
  public final static String IDENTIFIER_QUOTE_STRING = "\"";
  public final static int MAX_IDENTIFIER_LENGTH = 128;
  // Linux and BSD's max username length is 32. For compatibility with other systems, we should follow it.
  public final static int MAX_USERNAME_LENGTH = 32;
  public final static int MAX_STATEMENT_LENGTH = 128 * 1024;
  public final static int MAX_CONNECTION_LENGTH = 10 * 1024;

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
  
  public static final String INFORMATION_SCHEMA_DB_NAME = "information_schema";
}
