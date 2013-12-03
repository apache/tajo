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
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;
import java.util.List;
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
  protected void createBaseTable() throws SQLException {

    int result;
    Statement stmt = null;
    try {
      stmt = getConnection().createStatement();

      // META
      if (!baseTableMaps.get(TB_META)) {
        String meta_ddl = "CREATE TABLE " + TB_META + " (version int NOT NULL)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(meta_ddl);
        }
        result = stmt.executeUpdate(meta_ddl);
        LOG.info("Table '" + TB_META + " is created.");
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        String tables_ddl = "CREATE TABLE "
            + TB_TABLES + " ("
            + "TID int NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            + C_TABLE_ID + " VARCHAR(255) NOT NULL UNIQUE, "
            + "path TEXT, "
            + "store_type CHAR(16)"
            + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(tables_ddl);
        }

        LOG.info("Table '" + TB_TABLES + "' is created.");
        result = stmt.executeUpdate(tables_ddl);
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        String columns_ddl =
            "CREATE TABLE " + TB_COLUMNS + " ("
                + "TID INT NOT NULL,"
                + C_TABLE_ID + " VARCHAR(255) NOT NULL,"
                + "column_id INT NOT NULL,"
                + "column_name VARCHAR(255) NOT NULL, " + "data_type CHAR(16), " + "type_length INTEGER, "
                + "UNIQUE KEY(" + C_TABLE_ID + ", column_name),"
                + "FOREIGN KEY(TID) REFERENCES "+TB_TABLES+"(TID) ON DELETE CASCADE,"
                + "FOREIGN KEY("+C_TABLE_ID+") REFERENCES "+TB_TABLES+"("+C_TABLE_ID+") ON DELETE CASCADE)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(columns_ddl);
        }

        LOG.info("Table '" + TB_COLUMNS + " is created.");
        result = stmt.executeUpdate(columns_ddl);
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        String options_ddl =
            "CREATE TABLE " + TB_OPTIONS + " ("
                + C_TABLE_ID + " VARCHAR(255) NOT NULL,"
                + "key_ VARCHAR(255) NOT NULL, value_ VARCHAR(255) NOT NULL,"
                + "INDEX("+C_TABLE_ID+", key_),"
                + "FOREIGN KEY("+C_TABLE_ID+") REFERENCES "+TB_TABLES+"("+C_TABLE_ID+") ON DELETE CASCADE)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(options_ddl);
        }
        LOG.info("Table '" + TB_OPTIONS + " is created.");
        result = stmt.executeUpdate(options_ddl);
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        String indexes_ddl = "CREATE TABLE " + TB_INDEXES + "("
            + "index_name VARCHAR(255) NOT NULL PRIMARY KEY, "
            + C_TABLE_ID + " VARCHAR(255) NOT NULL,"
            + "column_name VARCHAR(255) NOT NULL, "
            + "data_type VARCHAR(255) NOT NULL, "
            + "index_type CHAR(32) NOT NULL, "
            + "is_unique BOOLEAN NOT NULL, "
            + "is_clustered BOOLEAN NOT NULL, "
            + "is_ascending BOOLEAN NOT NULL,"
            + "INDEX(" + C_TABLE_ID + ", column_name),"
            + "FOREIGN KEY("+C_TABLE_ID+") REFERENCES "+TB_TABLES+"("+C_TABLE_ID+") ON DELETE CASCADE)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(indexes_ddl);
        }
        LOG.info("Table '" + TB_INDEXES + "' is created.");
        result = stmt.executeUpdate(indexes_ddl);
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        String stats_ddl = "CREATE TABLE " + TB_STATISTICS + "("
            + C_TABLE_ID + " VARCHAR(255) NOT NULL,"
            + "num_rows BIGINT, "
            + "num_bytes BIGINT,"
            + "INDEX("+C_TABLE_ID+"),"
            + "FOREIGN KEY("+C_TABLE_ID+") REFERENCES "+TB_TABLES+"("+C_TABLE_ID+") ON DELETE CASCADE)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(stats_ddl);
        }
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
        result = stmt.executeUpdate(stats_ddl);
      }

      // PARTITION
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        String partition_ddl = "CREATE TABLE " + TB_PARTTIONS + " ("
            + "PID int NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "TID INT NOT NULL,"
            + "type VARCHAR(10) NOT NULL,"
            + "quantity INT ,"
            + "columns VARCHAR(255),"
            + "expressions TEXT )";
        if (LOG.isDebugEnabled()) {
          LOG.debug(partition_ddl);
        }
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
        result = stmt.executeUpdate(partition_ddl);
      }
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  protected boolean isInitialized() throws SQLException {
    ResultSet res = getConnection().getMetaData().getTables(null, null, null,
        new String[]{"TABLE"});

    try {
      baseTableMaps.put(TB_META, false);
      baseTableMaps.put(TB_TABLES, false);
      baseTableMaps.put(TB_COLUMNS, false);
      baseTableMaps.put(TB_OPTIONS, false);
      baseTableMaps.put(TB_STATISTICS, false);
      baseTableMaps.put(TB_INDEXES, false);
      baseTableMaps.put(TB_PARTTIONS, false);

      if (res.wasNull())
        return false;

      while (res.next()) {
        baseTableMaps.put(res.getString("TABLE_NAME"), true);
      }
    } finally {
      CatalogUtil.closeSQLWrapper(res);
    }

    for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
      if (!entry.getValue()) {
        return false;
      }
    }

    return  true;
//    return false;
  }

  @Override
  public final void addFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet    
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet    
  }

  @Override
  public final void existFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet    
  }

  @Override
  public final List<String> getAllFunctionNames() throws IOException {
    // TODO - not implemented yet
    return null;
  }
}
