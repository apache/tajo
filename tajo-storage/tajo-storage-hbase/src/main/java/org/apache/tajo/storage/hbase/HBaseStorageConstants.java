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

public interface HBaseStorageConstants {
  public static final String KEY_COLUMN_MAPPING = "key";
  public static final String VALUE_COLUMN_MAPPING = "value";
  public static final String META_FETCH_ROWNUM_KEY = "fetch.rownum";
  public static final String META_TABLE_KEY = "table";
  public static final String META_COLUMNS_KEY = "columns";
  public static final String META_SPLIT_ROW_KEYS_KEY = "hbase.split.rowkeys";
  public static final String META_SPLIT_ROW_KEYS_FILE_KEY = "hbase.split.rowkeys.file";
  public static final String META_ZK_QUORUM_KEY = "hbase.zookeeper.quorum";
  public static final String META_ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
  public static final String META_ROWKEY_DELIMITER = "hbase.rowkey.delimiter";

  public static final String INSERT_PUT_MODE = "tajo.hbase.insert.put.mode";
}
