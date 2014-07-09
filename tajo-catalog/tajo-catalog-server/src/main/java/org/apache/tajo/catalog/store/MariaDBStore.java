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

public class MariaDBStore extends AbstractMySQLMariaDBStore {
  /** 2014-06-09: First versioning */
  private static final int MARIADB_CATALOG_STORE_VERSION = 2;

  private static final String CATALOG_DRIVER = "org.mariadb.jdbc.Driver";

  @Override
  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public MariaDBStore(final Configuration conf) throws InternalException {
    super(conf);
  }

  @Override
  public int getDriverVersion() {
    return MARIADB_CATALOG_STORE_VERSION;
  }

  @Override
  public String readSchemaFile(String filename) throws CatalogException {
    return super.readSchemaFile("mariadb/" + filename);
  }
}
