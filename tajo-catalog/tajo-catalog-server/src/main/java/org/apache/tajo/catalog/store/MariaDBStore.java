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

public class MariaDBStore extends AbstractDBStore  {
  /** 2014-06-09: First versioning */
  private static final int MARIADB_CATALOG_STORE_VERSION = 2;

  private static final String CATALOG_DRIVER = "org.mariadb.jdbc.Driver";
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

  protected Connection createConnection(Configuration conf) throws SQLException {
    Connection con = DriverManager.getConnection(getCatalogUri(), this.connectionId,
        this.connectionPassword);
    //TODO con.setAutoCommit(false);
    return con;
  }

  @Override
  protected boolean isConnValid(int timeout) throws CatalogException {
    boolean isValid = false;

    try {
      isValid = super.isConnValid(timeout);
    } catch (NullPointerException e) {
      LOG.info("Conn abortion when checking isValid; retrieve false to create another Conn.");
    }
    return isValid;
  }

  @Override
  public String readSchemaFile(String filename) throws CatalogException {
    return super.readSchemaFile("mariadb/" + filename);
  }

  // TODO - DDL and index statements should be renamed
  @Override
  protected void createBaseTable() throws CatalogException {
    Statement stmt = null;
    Connection conn = null;

    try {
      conn = getConnection();
      stmt = conn.createStatement();


      // META
      if (!baseTableMaps.get(TB_META)) {
        String sql = super.readSchemaFile("common/meta.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_META + " is created.");
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
        LOG.info("Table '" + TB_DATABASES + "' is created.");
        baseTableMaps.put(TB_DATABASES, true);
        stmt.executeUpdate(sql);
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        String sql = readSchemaFile("tables.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.executeUpdate(sql);
        LOG.info("Table '" + TB_TABLES + "' is created.");
        baseTableMaps.put(TB_TABLES, true);
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        String sql = readSchemaFile("columns.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_COLUMNS + " is created.");
        baseTableMaps.put(TB_COLUMNS, true);
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        String sql = readSchemaFile("table_properties.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_OPTIONS + " is created.");
        baseTableMaps.put(TB_OPTIONS, true);
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        String sql = readSchemaFile("indexes.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_INDEXES + "' is created.");
        baseTableMaps.put(TB_INDEXES, true);
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        String sql = readSchemaFile("stats.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
        baseTableMaps.put(TB_STATISTICS, true);
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        String sql = readSchemaFile("partition_methods.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
        baseTableMaps.put(TB_PARTITION_METHODS, true);
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        String sql = readSchemaFile("partitions.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
        baseTableMaps.put(TB_PARTTIONS, true);
      }

      insertSchemaVersion();

    } catch (SQLException se) {
      throw new CatalogException("failed to create base tables for MariaDB catalog store", se);
    } finally {
      CatalogUtil.closeQuietly(stmt);
    }
  }

  @Override
  protected void dropBaseTable() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    Map<String, Boolean> droppedTable = new HashMap<String, Boolean>();

    try {
      conn = getConnection();
      stmt = conn.createStatement();
      StringBuilder sql = new StringBuilder();

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if(entry.getValue() && !entry.getKey().equals(TB_TABLES)) {
          sql.delete(0, sql.length());
          sql.append("DROP TABLE ").append(entry.getKey());
          stmt.addBatch(sql.toString());
          droppedTable.put(entry.getKey(), true);
        }
      }
      if(baseTableMaps.get(TB_TABLES)) {
        sql.delete(0, sql.length());
        sql.append("DROP TABLE ").append(TB_TABLES);
        stmt.addBatch(sql.toString());
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
          new String[]{"TABLE"});

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

      if (res.wasNull())
        return false;

      while (res.next()) {
        // if my.cnf has lower_case_table_names = 1,
        // TABLE_NAME returns lower case even it created by upper case.
        baseTableMaps.put(res.getString("TABLE_NAME").toUpperCase(), true);
      }

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if (!entry.getValue()) {
          return false;
        }
      }

    } catch(SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(res);
    }

    return  true;
  }
}
