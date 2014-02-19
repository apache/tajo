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
import java.util.Map;

public class MySQLStore extends AbstractDBStore  {

  private static final String CATALOG_DRIVER = "com.mysql.jdbc.Driver";
  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public MySQLStore(Configuration conf) throws InternalException {
    super(conf);
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    Connection con = DriverManager.getConnection(getCatalogUri(), this.connectionId,
        this.connectionPassword);
    //TODO con.setAutoCommit(false);
    return con;
  }

  // TODO - DDL and index statements should be renamed
  @Override
  protected void createBaseTable() throws CatalogException {
    Statement stmt = null;
    Connection conn = null;

    try {
      conn = getConnection();
      stmt = conn.createStatement();

      StringBuilder sql = new StringBuilder();
      // META
      if (!baseTableMaps.get(TB_META)) {
        sql.append("CREATE TABLE ").append(TB_META).append(" (version int NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_META + " is created.");
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_TABLES).append("(");
        sql.append("TID int NOT NULL AUTO_INCREMENT PRIMARY KEY, ");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL UNIQUE, ");
        sql.append("path TEXT, ").append("store_type CHAR(16)").append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_TABLES + "' is created.");
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_COLUMNS).append("(");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL,");
        sql.append("column_id INT NOT NULL,");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type CHAR(16), ");
        sql.append("type_length INTEGER, ");
        sql.append("UNIQUE KEY(").append(C_TABLE_ID).append(", column_name),");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID).append(") REFERENCES ");
        sql.append(TB_TABLES).append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_COLUMNS + " is created.");
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_OPTIONS).append("(");
        sql.append(C_TABLE_ID).append( " VARCHAR(255) NOT NULL,");
        sql.append("key_ VARCHAR(255) NOT NULL, value_ VARCHAR(255) NOT NULL,");
        sql.append("INDEX(").append(C_TABLE_ID).append(", key_),");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_OPTIONS + " is created.");
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_INDEXES).append("(");
        sql.append("index_name VARCHAR(255) NOT NULL PRIMARY KEY, ");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL,");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type VARCHAR(255) NOT NULL, ");
        sql.append("index_type CHAR(32) NOT NULL, ");
        sql.append("is_unique BOOLEAN NOT NULL, ");
        sql.append("is_clustered BOOLEAN NOT NULL, ");
        sql.append("is_ascending BOOLEAN NOT NULL,");
        sql.append("INDEX(").append(C_TABLE_ID).append(", column_name),");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_INDEXES + "' is created.");
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_STATISTICS).append("(");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL,");
        sql.append("num_rows BIGINT, ");
        sql.append("num_bytes BIGINT,");
        sql.append("INDEX(").append(C_TABLE_ID).append("),");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_PARTITION_METHODS).append("(");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) PRIMARY KEY,");
        sql.append("partition_type VARCHAR(10) NOT NULL,");
        sql.append("expression TEXT NOT NULL,");
        sql.append("expression_schema VARBINARY(1024) NOT NULL, ");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_PARTTIONS).append("(");
        sql.append("PID int NOT NULL AUTO_INCREMENT PRIMARY KEY, ");
        sql.append(C_TABLE_ID).append( " VARCHAR(255) NOT NULL,");
        sql.append("partition_name VARCHAR(255), ");
        sql.append("ordinal_position INT NOT NULL,");
        sql.append("partition_value TEXT,");
        sql.append("path TEXT,");
        sql.append("cache_nodes VARCHAR(255), ");
        sql.append("UNIQUE KEY(").append(C_TABLE_ID).append(", partition_name),");
        sql.append("FOREIGN KEY(").append(C_TABLE_ID);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(C_TABLE_ID).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());

        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, stmt);
    }
  }

  @Override
  protected boolean isInitialized() throws CatalogException {
    Connection conn = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, null, null,
          new String[]{"TABLE"});

      baseTableMaps.put(TB_META, false);
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
        baseTableMaps.put(res.getString("TABLE_NAME"), true);
      }

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if (!entry.getValue()) {
          return false;
        }
      }

    } catch(SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, res);
    }

    return  true;
  }

}
