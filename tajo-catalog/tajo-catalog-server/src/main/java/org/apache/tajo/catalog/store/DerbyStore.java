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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.exception.TajoInternalError;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DerbyStore extends AbstractDBStore {

  private static final Log LOG = LogFactory.getLog(DerbyStore.class);

  private static final String CATALOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";

  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public DerbyStore(final Configuration conf) {
    super(conf);
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri());
  }

  @Override
  public String readSchemaFile(String filename) {
    return super.readSchemaFile("derby/" + filename);
  }

  public static void shutdown() {
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
        LOG.info("Derby shutdown complete abnormally. - message: " + se.getMessage());
      }
    } finally {
      CatalogUtil.closeQuietly(conn);
    }
    LOG.info("Shutdown database");
  }

  @Override
  protected void createDatabaseDependants() {
    String schemaName = catalogSchemaManager.getCatalogStore().getSchema().getSchemaName();
    Statement stmt = null;
    
    if (schemaName != null && !schemaName.isEmpty()) {
      try {
        stmt = getConnection().createStatement();
        stmt.executeUpdate("CREATE SCHEMA " + schemaName);
      } catch (SQLException e) {
        throw new TajoInternalError(e);
      } finally {
        CatalogUtil.closeQuietly(stmt);
      }
    }
  }

  @Override
  protected String getCatalogSchemaPath() {
    return "schemas/derby";
  }
}
