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

/**
 *
 */
package org.apache.tajo.catalog.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.exception.InternalException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MariaDBStore extends AbstractDBStore {
  private static final String CATALOG_DRIVER = "org.mariadb.jdbc.Driver";

  public MariaDBStore(Configuration conf) throws InternalException {
    super(conf);
  }

  @Override
  protected String getCatalogDriverName() {
    return CATALOG_DRIVER;
  }

  @Override
  protected String getCatalogSchemaPath() {
    return "schemas/mariadb";
  }

  @Override
  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri(), this.connectionId, this.connectionPassword);
  }

  @Override
  protected void createDatabaseDependants() throws CatalogException {
  }
}
