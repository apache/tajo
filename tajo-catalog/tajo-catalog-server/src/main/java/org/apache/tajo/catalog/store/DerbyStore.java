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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.exception.InternalException;

import java.sql.*;

public class DerbyStore extends AbstractDBStore {

  private static final String CATALOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";

  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public DerbyStore(final Configuration conf) throws InternalException {
    super(conf);
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri());
  }

  @Override
  public String readSchemaFile(String filename) throws CatalogException {
    return super.readSchemaFile("derby/" + filename);
  }

  @Override
  public final void close() {
    Connection conn = null;
    // shutdown embedded database.
    try {
      // the shutdown=true attribute shuts down Derby.
      conn = DriverManager.getConnection("jdbc:derby:;shutdown=true");
    } catch (SQLException se) {
      if ( (se.getErrorCode() == 50000)
          && (se.getSQLState().equals("XJ015"))) {
        // tajo got the expected exception
        LOG.info("Derby shutdown complete normally.");
      } else {
        LOG.info("Derby shutdown complete abnormally. - message:" + se.getMessage());
      }
    } finally {
      CatalogUtil.closeQuietly(conn);
    }
    LOG.info("Shutdown database (" + catalogUri + ")");
  }

  @Override
  protected void createDatabaseDependants() throws CatalogException {
    String schemaName = catalogSchemaManager.getCatalogStore().getSchema().getSchemaName();
    Statement stmt = null;
    
    if (schemaName != null && !schemaName.isEmpty()) {
      try {
        stmt = getConnection().createStatement();
        stmt.executeUpdate("CREATE SCHEMA " + schemaName);
      } catch (SQLException e) {
        throw new CatalogException(e);
      }
    }
  }

  @Override
  protected String getCatalogSchemaPath() {
    return "schemas/derby";
  }
}
