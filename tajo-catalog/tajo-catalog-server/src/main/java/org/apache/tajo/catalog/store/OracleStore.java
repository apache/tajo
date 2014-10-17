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
package org.apache.tajo.catalog.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.exception.InternalException;

public class OracleStore extends AbstractDBStore {
  
  private static final int ORACLE_STORE_VERSION = 2;
  private static final String CATALOG_DRIVER = "oracle.jdbc.OracleDriver";

  public OracleStore(Configuration conf) throws InternalException {
    super(conf);
  }

  @Override
  protected String getCatalogDriverName() {
    return CATALOG_DRIVER;
  }

  @Override
  protected Connection createConnection(Configuration conf) throws SQLException {
    return DriverManager.getConnection(getCatalogUri(), this.connectionId, this.connectionPassword);
  }

  @Override
  protected boolean isInitialized() throws CatalogException {
    Connection conn;
    ResultSet res = null;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, 
          this.connectionId != null?this.connectionId.toUpperCase():null, 
              null, new String[] { "TABLE" });
      
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
    } catch (SQLException se) {
      throw new CatalogException(se);
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
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);
        LOG.info("Table '" + TB_META + " is created.");
        baseTableMaps.put(TB_META, true);
      }

      // TABLE SPACES
      if (!baseTableMaps.get(TB_SPACES)) {
        String sql = readSchemaFile("tablespaces.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE SEQUENCE TABLESPACES_SEQ";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE OR REPLACE TRIGGER TABLESPACES_AUTOINC " +
                "BEFORE INSERT ON TABLESPACES " +
                "FOR EACH ROW " +
                "WHEN (new.SPACE_ID IS NULL) " +
                "BEGIN " +
                "  SELECT TABLESPACES_SEQ.NEXTVAL INTO :new.SPACE_ID FROM DUAL; " +
                "END;";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);

        stmt.executeBatch();
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
        
        sql = "CREATE SEQUENCE DATABASES__SEQ";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE OR REPLACE TRIGGER DATABASES__AUTOINC " +
            "BEFORE INSERT ON DATABASES_ " +
            "FOR EACH ROW " +
            "WHEN (new.DB_ID IS NULL) " +
            "BEGIN " +
            "  SELECT DATABASES__SEQ.NEXTVAL INTO :new.DB_ID FROM DUAL; " +
            "END;";
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
        
        sql = "CREATE SEQUENCE TABLES_SEQ";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE OR REPLACE TRIGGER TABLES_AUTOINC " +
            "BEFORE INSERT ON TABLES " +
            "FOR EACH ROW " +
            "WHEN (new.TID IS NULL) " +
            "BEGIN " +
            "  SELECT TABLES_SEQ.NEXTVAL INTO :new.TID FROM DUAL; " +
            "END;";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE INDEX TABLES_IDX_DB_ID on TABLES (DB_ID)";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE UNIQUE INDEX TABLES_IDX_TABLE_ID on TABLES (DB_ID, TABLE_NAME)";
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

        stmt.executeUpdate(sql);
        LOG.info("Table '" + TB_COLUMNS + " is created.");
        baseTableMaps.put(TB_COLUMNS, true);
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        String sql = readSchemaFile("table_properties.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);
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
        
        sql = "CREATE INDEX INDEXES_IDX_TID_COLUMN_NAME on INDEXES (TID, COLUMN_NAME)";
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

        stmt.executeUpdate(sql);
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
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        
        sql = "CREATE INDEX PARTITIONS_IDX_TID on PARTITIONS (TID)";
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
      throw new CatalogException("failed to create base tables for Oracle catalog store", se);
    } finally {
      CatalogUtil.closeQuietly(stmt);
    }
  }

  @Override
  protected void dropBaseTable() throws CatalogException {
    Connection conn;
    Statement stmt = null;
    PreparedStatement dropSequence = null;
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
      
      // Drop sequences
      dropSequence = conn.prepareStatement("SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ?");
      for(String tableName : droppedTable.keySet()) {
        String possibleSeqName = tableName + "_SEQ";
        dropSequence.setString(1, possibleSeqName);
        ResultSet rs = dropSequence.executeQuery();
        
        if (rs.next() && rs.getInt(1) > 0) {
          String sql = "DROP SEQUENCE " + possibleSeqName;
          stmt.execute(sql);
          LOG.info("Sequence '" + possibleSeqName + "' is dropped");
        }
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt);
      CatalogUtil.closeQuietly(dropSequence);
    }
  }

  @Override
  public int getDriverVersion() {
    return ORACLE_STORE_VERSION;
  }

  @Override
  public String readSchemaFile(String path) throws CatalogException {
    return super.readSchemaFile("oracle/" + path);
  }
}
