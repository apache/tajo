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

public class MySQLStore extends AbstractDBStore  {

  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  protected String getJDBCDriverName(){
    return JDBC_DRIVER;
  }

  public MySQLStore(Configuration conf) throws InternalException {
    super(conf);
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    Connection con = DriverManager.getConnection(getJDBCUri(), conf.get(CONNECTION_ID), conf.get(CONNECTION_PASSWORD));
    //TODO con.setAutoCommit(false);
    return con;
  }

  // TODO - DDL and index statements should be renamed
  protected void createBaseTable() throws SQLException {

    // META
    Statement stmt = conn.createStatement();
    String meta_ddl = "CREATE TABLE " + TB_META + " (version int NOT NULL)";
    if (LOG.isDebugEnabled()) {
      LOG.debug(meta_ddl);
    }
    try {
      int result = stmt.executeUpdate(meta_ddl);
      LOG.info("Table '" + TB_META + " is created.");

      // TABLES
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
      // COLUMNS

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
      // OPTIONS

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
      // INDEXES

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
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  protected boolean isInitialized() throws SQLException {
    boolean found = false;
    ResultSet res = conn.getMetaData().getTables(null, null, null,
        new String[]{"TABLE"});

    String resName;
    try {
      while (res.next() && !found) {
        resName = res.getString("TABLE_NAME");
        if (TB_META.equals(resName)
            || TB_TABLES.equals(resName)
            || TB_COLUMNS.equals(resName)
            || TB_OPTIONS.equals(resName)) {
          return true;
        }
      }
    } finally {
      CatalogUtil.closeSQLWrapper(res);
    }
    return false;
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