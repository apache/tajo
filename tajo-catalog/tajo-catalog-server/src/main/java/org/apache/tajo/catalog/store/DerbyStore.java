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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DerbyStore extends AbstractDBStore {
  private static final String CATALOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";

  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }
  
  public DerbyStore(final Configuration conf)
      throws InternalException {
    super(conf);
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri());
  }

  // TODO - DDL and index statements should be renamed
  protected void createBaseTable() throws IOException {
    Statement stmt = null;
    try {
      // META
      stmt = getConnection().createStatement();

      if (!baseTableMaps.get(TB_META)) {
        String meta_ddl = "CREATE TABLE " + TB_META + " (version int NOT NULL)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(meta_ddl);
        }
        stmt.executeUpdate(meta_ddl);
        LOG.info("Table '" + TB_META + " is created.");
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        String tables_ddl = "CREATE TABLE "
            + TB_TABLES + " ("
            + "TID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + C_TABLE_ID + " VARCHAR(255) NOT NULL CONSTRAINT TABLE_ID_UNIQ UNIQUE, "
            + "path VARCHAR(1024), "
            + "store_type CHAR(16), "
            + "CONSTRAINT TABLES_PK PRIMARY KEY (TID)" +
            ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(tables_ddl);
        }
        stmt.addBatch(tables_ddl);

        String idx_tables_tid =
            "CREATE UNIQUE INDEX idx_tables_tid on " + TB_TABLES + " (TID)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_tables_tid);
        }
        stmt.addBatch(idx_tables_tid);

        String idx_tables_name = "CREATE UNIQUE INDEX idx_tables_name on "
            + TB_TABLES + "(" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_tables_name);
        }
        stmt.addBatch(idx_tables_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_TABLES + "' is created.");
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        String columns_ddl =
            "CREATE TABLE " + TB_COLUMNS + " ("
                + "TID INT NOT NULL REFERENCES " + TB_TABLES + " (TID) ON DELETE CASCADE, "
                + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES " + TB_TABLES + "("
                + C_TABLE_ID + ") ON DELETE CASCADE, "
                + "column_id INT NOT NULL,"
                + "column_name VARCHAR(255) NOT NULL, " + "data_type CHAR(16), " + "type_length INTEGER, "
                + "CONSTRAINT C_COLUMN_ID UNIQUE (" + C_TABLE_ID + ", column_name))";
        if (LOG.isDebugEnabled()) {
          LOG.debug(columns_ddl);
        }
        stmt.addBatch(columns_ddl);

        String idx_fk_columns_table_name =
            "CREATE UNIQUE INDEX idx_fk_columns_table_name on "
                + TB_COLUMNS + "(" + C_TABLE_ID + ", column_name)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_fk_columns_table_name);
        }
        stmt.addBatch(idx_fk_columns_table_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_COLUMNS + " is created.");
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        String options_ddl =
            "CREATE TABLE " + TB_OPTIONS +" ("
                + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES TABLES (" + C_TABLE_ID +") "
                + "ON DELETE CASCADE, "
                + "key_ VARCHAR(255) NOT NULL, value_ VARCHAR(255) NOT NULL)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(options_ddl);
        }
        stmt.addBatch(options_ddl);

        String idx_options_key =
            "CREATE INDEX idx_options_key on " + TB_OPTIONS + " (" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_options_key);
        }
        stmt.addBatch(idx_options_key);
        String idx_options_table_name =
            "CREATE INDEX idx_options_table_name on " + TB_OPTIONS
                + "(" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_options_table_name);
        }
        stmt.addBatch(idx_options_table_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_OPTIONS + " is created.");
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        String indexes_ddl = "CREATE TABLE " + TB_INDEXES +"("
            + "index_name VARCHAR(255) NOT NULL PRIMARY KEY, "
            + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
            + "ON DELETE CASCADE, "
            + "column_name VARCHAR(255) NOT NULL, "
            + "data_type VARCHAR(255) NOT NULL, "
            + "index_type CHAR(32) NOT NULL, "
            + "is_unique BOOLEAN NOT NULL, "
            + "is_clustered BOOLEAN NOT NULL, "
            + "is_ascending BOOLEAN NOT NULL)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(indexes_ddl);
        }
        stmt.addBatch(indexes_ddl);

        String idx_indexes_key = "CREATE UNIQUE INDEX idx_indexes_key ON "
            + TB_INDEXES + " (index_name)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_indexes_key);
        }
        stmt.addBatch(idx_indexes_key);

        String idx_indexes_columns = "CREATE INDEX idx_indexes_columns ON "
            + TB_INDEXES + " (" + C_TABLE_ID + ", column_name)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_indexes_columns);
        }
        stmt.addBatch(idx_indexes_columns);
        stmt.executeBatch();
        LOG.info("Table '" + TB_INDEXES + "' is created.");
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        String stats_ddl = "CREATE TABLE " + TB_STATISTICS + "("
            + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
            + "ON DELETE CASCADE, "
            + "num_rows BIGINT, "
            + "num_bytes BIGINT)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(stats_ddl);
        }
        stmt.addBatch(stats_ddl);

        String idx_stats_fk_table_name = "CREATE INDEX idx_stats_table_name ON "
            + TB_STATISTICS + " (" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_stats_fk_table_name);
        }
        stmt.addBatch(idx_stats_fk_table_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        String partition_method_ddl = "CREATE TABLE " + TB_PARTITION_METHODS + " ("
            + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
            + "ON DELETE CASCADE, "
            + "partition_type VARCHAR(10) NOT NULL,"
            + "expression VARCHAR(1024) NOT NULL,"
            + "expression_schema VARCHAR(1024) FOR BIT DATA NOT NULL)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(partition_method_ddl);
        }
        stmt.addBatch(partition_method_ddl);

        String idx_partition_methods_table_name = "CREATE INDEX idx_partition_methods_table_name ON "
            + TB_PARTITION_METHODS + " (" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_partition_methods_table_name);
        }
        stmt.addBatch(idx_partition_methods_table_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        String partition_ddl = "CREATE TABLE " + TB_PARTTIONS + " ("
            + "PID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + C_TABLE_ID + " VARCHAR(255) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
            + "ON DELETE CASCADE, "
            + "partition_name VARCHAR(255), "
            + "ordinal_position INT NOT NULL,"
            + "partition_value VARCHAR(1024),"
            + "path VARCHAR(1024),"
            + "cache_nodes VARCHAR(255), "
            + " CONSTRAINT PARTITION_PK PRIMARY KEY (PID)"
            + "   )";
        if (LOG.isDebugEnabled()) {
          LOG.debug(partition_ddl);
        }
        stmt.addBatch(partition_ddl);

        String idx_partitions_table_name = "CREATE INDEX idx_partitions_table_name ON "
            + TB_PARTTIONS + " (" + C_TABLE_ID + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(idx_partitions_table_name);
        }
        stmt.addBatch(idx_partitions_table_name);
        stmt.executeBatch();
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
      }

    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  protected boolean isInitialized() throws IOException {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    ResultSet res = null;
    int foundCount = 0;
    try {
      res = getConnection().getMetaData().getTables(null, null, null,
          new String [] {"TABLE"});

      baseTableMaps.put(TB_META, false);
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
      throw  new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res);
    }

    for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
      if (!entry.getValue()) {
        return false;
      }
    }

    return true;
  }
  
  final boolean checkInternalTable(final String tableName) throws IOException {
    ResultSet res = null;
    try {
      boolean found = false;
      res = getConnection().getMetaData().getTables(null, null, null,
              new String [] {"TABLE"});
      while(res.next() && !found) {
        if (tableName.equals(res.getString("TABLE_NAME")))
          found = true;
      }
      
      return found;
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res);
    }
  }
  

  @Override
  public final void close() {
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    } catch (SQLException e) {
      // TODO - to be fixed
      //LOG.error(e.getMessage(), e);
    }
    
    LOG.info("Shutdown database (" + catalogUri + ")");
  }
  
  
  public static void main(final String[] args) throws IOException {
    @SuppressWarnings("unused")
    DerbyStore store = new DerbyStore(new TajoConf());
  }
}
