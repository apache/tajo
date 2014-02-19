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
  protected void createBaseTable() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;

    try {
      conn = getConnection();
      stmt = conn.createStatement();

      StringBuilder sql = new StringBuilder();

      //META
      if (!baseTableMaps.get(TB_META)) {
        sql.append("CREATE TABLE ");
        sql.append(TB_META);
        sql.append(" (version int NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_META + " is created.");
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ");
        sql.append(TB_TABLES);
        sql.append(" (");
        sql.append("TID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),");
        sql.append(C_TABLE_ID);
        sql.append(" VARCHAR(255) NOT NULL CONSTRAINT TABLE_ID_UNIQ UNIQUE, ");
        sql.append("path VARCHAR(1024), ");
        sql.append("store_type CHAR(16), ");
        sql.append("CONSTRAINT TABLES_PK PRIMARY KEY (TID)");
        sql.append( ")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE UNIQUE INDEX idx_tables_tid on ");
        sql.append(TB_TABLES);
        sql.append(" (TID)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE UNIQUE INDEX idx_tables_name on ");
        sql.append(TB_TABLES);
        sql.append("(");
        sql.append(C_TABLE_ID);
        sql.append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_TABLES + "' is created.");

      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ");
        sql.append(TB_COLUMNS);
        sql.append(" (");
        sql.append("TID INT NOT NULL REFERENCES ");
        sql.append(TB_TABLES);
        sql.append(" (TID) ON DELETE CASCADE, ");
        sql.append(C_TABLE_ID);
        sql.append( " VARCHAR(255) NOT NULL REFERENCES ");
        sql.append(TB_TABLES);
        sql.append("(");
        sql.append(C_TABLE_ID);
        sql.append(") ON DELETE CASCADE, ");
        sql.append("column_id INT NOT NULL,");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type CHAR(16), type_length INTEGER, ");
        sql.append("CONSTRAINT C_COLUMN_ID UNIQUE (");
        sql.append(C_TABLE_ID);
        sql.append(", column_name))");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append( "CREATE UNIQUE INDEX idx_fk_columns_table_name on ");
        sql.append(TB_COLUMNS);
        sql.append("(");
        sql.append(C_TABLE_ID);
        sql.append(", column_name)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_COLUMNS + " is created.");
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        sql.delete(0, sql.length());
        sql.append( "CREATE TABLE ");
        sql.append(TB_OPTIONS);
        sql.append(" (").append(C_TABLE_ID);
        sql.append(" VARCHAR(255) NOT NULL REFERENCES TABLES (");
        sql.append(C_TABLE_ID).append(") ON DELETE CASCADE, ");
        sql.append("key_ VARCHAR(255) NOT NULL, value_ VARCHAR(255) NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_options_key on ");
        sql.append(TB_OPTIONS).append( " (").append(C_TABLE_ID).append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_options_table_name on ").append(TB_OPTIONS);
        sql.append("(" ).append(C_TABLE_ID).append(")");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_OPTIONS + " is created.");
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_INDEXES).append("(");
        sql.append( "index_name VARCHAR(255) NOT NULL PRIMARY KEY, ");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL REFERENCES TABLES (");
        sql.append(C_TABLE_ID).append(") ");
        sql.append("ON DELETE CASCADE, ");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type VARCHAR(255) NOT NULL, ");
        sql.append("index_type CHAR(32) NOT NULL, ");
        sql.append("is_unique BOOLEAN NOT NULL, ");
        sql.append("is_clustered BOOLEAN NOT NULL, ");
        sql.append("is_ascending BOOLEAN NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE UNIQUE INDEX idx_indexes_key ON ");
        sql.append(TB_INDEXES).append(" (index_name)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_indexes_columns ON ");
        sql.append(TB_INDEXES).append(" (").append(C_TABLE_ID).append(", column_name)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_INDEXES + "' is created.");
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_STATISTICS).append( "(");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL REFERENCES TABLES (");
        sql.append(C_TABLE_ID).append(") ");
        sql.append("ON DELETE CASCADE, ");
        sql.append("num_rows BIGINT, ");
        sql.append("num_bytes BIGINT)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_stats_table_name ON ");
        sql.append(TB_STATISTICS).append(" (").append(C_TABLE_ID).append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_PARTITION_METHODS).append(" (");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL REFERENCES TABLES (");
        sql.append(C_TABLE_ID).append(") ");
        sql.append("ON DELETE CASCADE, ");
        sql.append("partition_type VARCHAR(10) NOT NULL,");
        sql.append("expression VARCHAR(1024) NOT NULL,");
        sql.append("expression_schema VARCHAR(1024) FOR BIT DATA NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_partition_methods_table_name ON ");
        sql.append(TB_PARTITION_METHODS).append(" (").append(C_TABLE_ID).append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_PARTTIONS).append("(");
        sql.append("PID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),");
        sql.append(C_TABLE_ID).append(" VARCHAR(255) NOT NULL REFERENCES TABLES (");
        sql.append(C_TABLE_ID).append(")");
        sql.append("ON DELETE CASCADE, ");
        sql.append("partition_name VARCHAR(255), ");
        sql.append("ordinal_position INT NOT NULL,");
        sql.append("partition_value VARCHAR(1024),");
        sql.append("path VARCHAR(1024),");
        sql.append("cache_nodes VARCHAR(255), ");
        sql.append(" CONSTRAINT PARTITION_PK PRIMARY KEY (PID))");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());

        sql.delete(0, sql.length());
        sql.append("CREATE INDEX idx_partitions_table_name ON ");
        sql.append(TB_PARTTIONS).append(" (").append(C_TABLE_ID).append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }
        stmt.addBatch(sql.toString());
        stmt.executeBatch();
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
    int foundCount = 0;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, null, null,
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
      throw  new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, res);
    }

    for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
      if (!entry.getValue()) {
        return false;
      }
    }

    return true;
  }

  final boolean checkInternalTable(final String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    boolean found = false;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, null, null,
          new String [] {"TABLE"});
      while(res.next() && !found) {
        if (tableName.equals(res.getString("TABLE_NAME")))
          found = true;
      }

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, res);
    }
    return found;
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
