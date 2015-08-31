/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.mysql;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.jdbc.JdbcFragment;
import org.apache.tajo.storage.jdbc.JdbcScanner;
import org.apache.tajo.storage.jdbc.SQLBuilder;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

public class MySQLJdbcScanner extends JdbcScanner {

  public MySQLJdbcScanner(DatabaseMetaData dbMetaData,
                          Schema tableSchema,
                          TableMeta tableMeta,
                          JdbcFragment fragment) {
    super(dbMetaData, tableSchema, tableMeta, fragment);
  }
}
