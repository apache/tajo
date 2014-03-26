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
import java.util.HashMap;
import java.util.Map;

public class DerbyStore extends AbstractDBStore {

  /** 2014-03-20: First versioning */
  private static final int DERBY_STORE_VERSION_2 = 2;
  /** Before 2013-03-20 */
  private static final int DERBY_STORE_VERSION_1 = 1;
  private static final String CATALOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";

  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public DerbyStore(final Configuration conf) throws InternalException {
    super(conf);
  }

  public int getDriverVersion() {
    return DERBY_STORE_VERSION_2;
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri());
  }

  @Override
  public String readSchemaFile(String filename) throws CatalogException {
    return super.readSchemaFile("derby/" + filename);
  }

  // TODO - DDL and index statements should be renamed
  protected void createBaseTable() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;

    try {
      conn = getConnection();
      stmt = conn.createStatement();

      //META
      if (!baseTableMaps.get(TB_META)) {

        String sql = super.readSchemaFile("common/meta.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);

        LOG.info("Table '" + TB_META + "' is created.");
        baseTableMaps.put(TB_META, true);
      }

      // TABLE SPACES
      if (!baseTableMaps.get(TB_SPACES)) {

        String sql = readSchemaFile("tablespaces.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);

        LOG.info("Table '" + TB_SPACES + "' is created.");
        baseTableMaps.put(TB_SPACES, true);
      }

      // DATABASES
      if (!baseTableMaps.get(TB_DATABASES)) {
        String sql = readSchemaFile("databases.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        sql = readSchemaFile("databases_idx.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        stmt.executeBatch();
        LOG.info("Table '" + TB_DATABASES + "' is created.");
        baseTableMaps.put(TB_DATABASES, true);
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {

        String sql = readSchemaFile("tables.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);


        sql = "CREATE UNIQUE INDEX idx_tables_tid on TABLES (TID)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);



        sql = "CREATE UNIQUE INDEX idx_tables_name on TABLES (DB_ID, TABLE_NAME)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        stmt.executeBatch();
        LOG.info("Table '" + TB_TABLES + "' is created.");
        baseTableMaps.put(TB_TABLES, true);
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        String sql = readSchemaFile("columns.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        sql = "CREATE UNIQUE INDEX idx_fk_columns_table_name on " + TB_COLUMNS + "(TID, COLUMN_NAME)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
        LOG.info("Table '" + TB_COLUMNS + " is created.");
        baseTableMaps.put(TB_COLUMNS, true);
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        String sql = readSchemaFile("table_properties.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);


        sql = "CREATE INDEX idx_options_key on " + TB_OPTIONS + "(TID)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        stmt.executeBatch();

        LOG.info("Table '" + TB_OPTIONS + " is created.");
        baseTableMaps.put(TB_OPTIONS, true);
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        String sql = readSchemaFile("indexes.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        sql = "CREATE UNIQUE INDEX idx_indexes_pk ON " + TB_INDEXES + "(" + COL_DATABASES_PK + ",index_name)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        sql = "CREATE INDEX idx_indexes_columns ON " + TB_INDEXES + "(" + COL_DATABASES_PK + ",column_name)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
        LOG.info("Table '" + TB_INDEXES + "' is created.");
        baseTableMaps.put(TB_INDEXES, true);
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        String sql = readSchemaFile("stats.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);


        sql = "CREATE UNIQUE INDEX idx_stats_table_name ON " + TB_STATISTICS + "(TID)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
        baseTableMaps.put(TB_STATISTICS, true);
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        String sql = readSchemaFile("partition_methods.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);


        sql = "CREATE INDEX idx_partition_methods_table_id ON " + TB_PARTITION_METHODS + "(TID)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
        baseTableMaps.put(TB_PARTITION_METHODS, true);
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        String sql = readSchemaFile("partitions.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);


        sql = "CREATE INDEX idx_partitions_table_name ON PARTITIONS(TID)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
        baseTableMaps.put(TB_PARTTIONS, true);
      }

      insertSchemaVersion();

    } catch (SQLException se) {
      throw new CatalogException("failed to create base tables for Derby catalog store.", se);
    } finally {
      CatalogUtil.closeQuietly(stmt);
    }
  }

  @Override
  protected void dropBaseTable() throws CatalogException {
    Connection conn;
    Statement stmt = null;
    Map<String, Boolean> droppedTable = new HashMap<String, Boolean>();

    try {
      conn = getConnection();
      stmt = conn.createStatement();

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if(entry.getValue() && !entry.getKey().equals(TB_TABLES)) {
          String sql = "DROP TABLE " + entry.getKey();
          stmt.addBatch(sql);
          droppedTable.put(entry.getKey(), true);
        }
      }
      if(baseTableMaps.get(TB_TABLES)) {
        String sql = "DROP TABLE " + TB_TABLES;
        stmt.addBatch(sql);
        droppedTable.put(TB_TABLES, true);
      }
      stmt.executeBatch();

      for(String tableName : droppedTable.keySet()) {
        LOG.info("Table '" + tableName + "' is dropped");
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt);
    }
  }

  @Override
  protected boolean isInitialized() throws CatalogException {
    Connection conn;
    ResultSet res = null;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, null, null,
          new String [] {"TABLE"});

      baseTableMaps.put(TB_META, false);
      baseTableMaps.put(TB_SPACES, false);
      baseTableMaps.put(TB_DATABASES, false);
      baseTableMaps.put(TB_TABLES, false);
      baseTableMaps.put(TB_COLUMNS, false);
      baseTableMaps.put(TB_OPTIONS, false);
      baseTableMaps.put(TB_STATISTICS, false);
      baseTableMaps.put(TB_INDEXES, false);
      baseTableMaps.put(TB_PARTITION_METHODS, false);
      baseTableMaps.put(TB_PARTTIONS, false);

      while (res.next()) {
        baseTableMaps.put(res.getString("TABLE_NAME"), true);
      }
    } catch (SQLException se){
      throw  new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(res);
    }

    for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
      if (!entry.getValue()) {
        return false;
      }
    }

    return true;
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
}
