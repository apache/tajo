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
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.Partitions;
import org.apache.tajo.catalog.partition.Specifier;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DerbyStore extends AbstractDBStore {
  private static final String CATALOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver";
  protected Lock rlock;
  protected Lock wlock;

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
  protected void createBaseTable() throws SQLException {
    wlock.lock();
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
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
      }

      // PARTITION
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        String partition_ddl = "CREATE TABLE " + TB_PARTTIONS + " ("
            + "PID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + "name VARCHAR(255), "
            + "TID INT NOT NULL REFERENCES " + TB_TABLES + " (TID) ON DELETE CASCADE, "
            + "type VARCHAR(10) NOT NULL,"
            + "quantity INT ,"
            + "columns VARCHAR(255),"
            + "expressions VARCHAR(1024)"
            + ", CONSTRAINT PARTITION_PK PRIMARY KEY (PID)"
            + "   )";

        if (LOG.isDebugEnabled()) {
          LOG.debug(partition_ddl);
        }
        stmt.addBatch(partition_ddl);
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
        stmt.executeBatch();
      }

    } finally {
      wlock.unlock();
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  protected boolean isInitialized() throws SQLException {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.rlock = lock.readLock();
    this.wlock = lock.writeLock();

    wlock.lock();
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
      baseTableMaps.put(TB_PARTTIONS, false);

      while (res.next()) {
        baseTableMaps.put(res.getString("TABLE_NAME"), true);
      }
    } finally {
      wlock.unlock();
      CatalogUtil.closeSQLWrapper(res);
    }

    for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
      if (!entry.getValue()) {
        return false;
      }
    }

    return true;
  }
  
  final boolean checkInternalTable(final String tableName) throws SQLException {
    rlock.lock();
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
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res);
    }
  }
  
  @Override
  public final void addTable(final TableDesc table) throws IOException {
    PreparedStatement pstmt = null;
    Statement stmt = null;
    ResultSet res = null;

    String sql = 
        "INSERT INTO " + TB_TABLES + " (" + C_TABLE_ID + ", path, store_type) "
        + "VALUES('" + table.getName() + "', "
        + "'" + table.getPath() + "', "
        + "'" + table.getMeta().getStoreType() + "'"
        + ")";

    wlock.lock();
    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt.executeUpdate(sql);

      sql = "SELECT TID from " + TB_TABLES + " WHERE " + C_TABLE_ID 
          + " = '" + table.getName() + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);
      if (!res.next()) {
        throw new IOException("ERROR: there is no tid matched to " 
            + table.getName());
      }
      int tid = res.getInt("TID");

      String colSql;
      int columnId = 0;
      for (Column col : table.getSchema().getColumns()) {
        colSql = columnToSQL(tid, table, columnId, col);
        if (LOG.isDebugEnabled()) {
          LOG.debug(colSql);
        }
        stmt.addBatch(colSql);
        columnId++;
      }
      
      Iterator<Entry<String,String>> it = table.getMeta().toMap().entrySet().iterator();
      String optSql;
      while (it.hasNext()) {
        optSql = keyvalToSQL(table, it.next());
        if (LOG.isDebugEnabled()) {
          LOG.debug(optSql);
        }
        stmt.addBatch(optSql);
      }
      stmt.executeBatch();
      if (table.getStats() != null) {
        sql = "INSERT INTO " + TB_STATISTICS + " (" + C_TABLE_ID + ", num_rows, num_bytes) "
            + "VALUES ('" + table.getName() + "', "
            + table.getStats().getNumRows() + ","
            + table.getStats().getNumBytes() + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.executeUpdate(sql);
      }

      //Partition
      if (table.getPartitions() != null && !table.getPartitions().toString().isEmpty()) {
        try {
          Partitions partitions = table.getPartitions();
          List<Column> columnList = partitions.getColumns();

          // Find columns which used for a partitioned table.
          StringBuffer columns = new StringBuffer();
          for(Column eachColumn : columnList) {
            sql = "SELECT column_id from " + TB_COLUMNS + " WHERE TID "
                + " = " + tid + " AND column_name = '" + eachColumn.getColumnName() + "'";

            if (LOG.isDebugEnabled()) {
              LOG.debug(sql);
            }
            res = stmt.executeQuery(sql);
            if (!res.next()) {
              throw new IOException("ERROR: there is no columnId matched to "
                  + table.getName());
            }
            columnId = res.getInt("column_id");

            if(columns.length() > 0) {
              columns.append(",");
            }
            columns.append(columnId);
          }

          // Set default partition name. But if user named to subpartition, it would be updated.
//          String partitionName = partitions.getPartitionsType().name() + "_" + table.getName();

          sql = "INSERT INTO " + TB_PARTTIONS + " (name, TID, "
              + " type, quantity, columns, expressions) VALUES (?, ?, ?, ?, ?, ?) ";
          pstmt = getConnection().prepareStatement(sql);
          if (LOG.isDebugEnabled()) {
            LOG.debug(sql);
          }

          // Find information for subpartitions
          if (partitions.getSpecifiers() != null) {
            int count = 1;
            if (partitions.getSpecifiers().size() == 0) {
              pstmt.clearParameters();
              pstmt.setString(1, null);
              pstmt.setInt(2, tid);
              pstmt.setString(3, partitions.getPartitionsType().name());
              pstmt.setInt(4, partitions.getNumPartitions());
              pstmt.setString(5, columns.toString());
              pstmt.setString(6, null);
              pstmt.addBatch();
            } else {
              for(Specifier eachValue: partitions.getSpecifiers()) {
                pstmt.clearParameters();
                if (eachValue.getName() != null && !eachValue.getName().equals("")) {
                  pstmt.setString(1, eachValue.getName());
                } else {
                  pstmt.setString(1, null);
                }
                pstmt.setInt(2, tid);
                pstmt.setString(3, partitions.getPartitionsType().name());
                pstmt.setInt(4, partitions.getNumPartitions());
                pstmt.setString(5, columns.toString());
                pstmt.setString(6, eachValue.getExpressions());
                pstmt.addBatch();
                count++;
              }
            }
          } else {
            pstmt.clearParameters();
            pstmt.setString(1, null);
            pstmt.setInt(2, tid);
            pstmt.setString(3, partitions.getPartitionsType().name());
            pstmt.setInt(4, partitions.getNumPartitions());
            pstmt.setString(5, columns.toString());
            pstmt.setString(6, null);
            pstmt.addBatch();
          }
          pstmt.executeBatch();
        } finally {
          CatalogUtil.closeSQLWrapper(pstmt);
        }
      }

    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      wlock.unlock();
      CatalogUtil.closeSQLWrapper(res, pstmt);
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
  }
  
  private String columnToSQL(final int tid, final TableDesc desc, 
      final int columnId, final Column col) {
    String sql =
        "INSERT INTO " + TB_COLUMNS 
        + " (tid, " + C_TABLE_ID + ", column_id, column_name, data_type, type_length) "
        + "VALUES("
        + tid + ","
        + "'" + desc.getName() + "',"
        + columnId + ", "
        + "'" + col.getColumnName() + "',"
        + "'" + col.getDataType().getType().name() + "',"
        + (col.getDataType().hasLength() ? col.getDataType().getLength() : 0)
        + ")";
    
    return sql;
  }
  
  private String keyvalToSQL(final TableDesc desc,
      final Entry<String,String> keyVal) {
    String sql =
        "INSERT INTO " + TB_OPTIONS 
        + " (" + C_TABLE_ID + ", key_, value_) "
        + "VALUES("
        + "'" + desc.getName() + "',"
        + "'" + keyVal.getKey() + "',"
        + "'" + keyVal.getValue() + "'"
        + ")";
    
    return sql;
  }
  
  @Override
  public final boolean existTable(final String name) throws IOException {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT " + C_TABLE_ID + " from ")
    .append(TB_TABLES)
    .append(" WHERE " + C_TABLE_ID + " = '")
    .append(name)
    .append("'");

    Statement stmt = null;
    ResultSet res = null;
    boolean exist = false;
    rlock.lock();
    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }
      res = stmt.executeQuery(sql.toString());
      exist = res.next();
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return exist;
  }

  @Override
  public final void deleteTable(final String name) throws IOException {
    Statement stmt = null;
    String sql = null;
    try {
      wlock.lock();
      stmt = getConnection().createStatement();
      try {
        sql = "DELETE FROM " + TB_COLUMNS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      }

      try {
        sql = "DELETE FROM " + TB_OPTIONS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      }

      try {
        sql = "DELETE FROM " + TB_STATISTICS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      }

      try {
        sql = "DELETE FROM " + TB_PARTTIONS + " WHERE TID IN ("
            + " SELECT TID FROM " + TB_TABLES
            + " WHERE " + C_TABLE_ID + " = '" + name + "' )";
        LOG.info(sql);
        stmt = getConnection().createStatement();
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      }

      try {
        sql = "DELETE FROM " + TB_TABLES +
            " WHERE " + C_TABLE_ID +" = '" + name + "'";
        LOG.info(sql);
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      wlock.unlock();
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  public final TableDesc getTable(final String name) throws IOException {
    ResultSet res = null;
    Statement stmt = null;

    String tableName = null;
    Path path = null;
    StoreType storeType = null;
    Options options;
    TableStats stat = null;
    Partitions partitions = null;
    int tid = 0;

    try {
      rlock.lock();
      stmt = getConnection().createStatement();

      try {
        String sql = 
            "SELECT " + C_TABLE_ID + ", path, store_type, TID from " + TB_TABLES
            + " WHERE " + C_TABLE_ID + "='" + name + "'";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        res = stmt.executeQuery(sql);
        if (!res.next()) { // there is no table of the given name.
          return null;
        }
        tableName = res.getString(C_TABLE_ID).trim();
        path = new Path(res.getString("path").trim());
        storeType = CatalogUtil.getStoreType(res.getString("store_type").trim());
        tid = res.getInt("TID");
      } catch (SQLException se) { 
        throw new IOException(se);
      } finally {
        CatalogUtil.closeSQLWrapper(res);
      }
      
      Schema schema = null;
      try {
        String sql = "SELECT column_name, data_type, type_length from " + TB_COLUMNS
            + " WHERE " + C_TABLE_ID + "='" + name + "' ORDER by column_id asc";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        res = stmt.executeQuery(sql);

        schema = new Schema();
        while (res.next()) {
          String columnName = tableName + "." 
              + res.getString("column_name").trim();
          Type dataType = getDataType(res.getString("data_type")
              .trim());
          int typeLength = res.getInt("type_length");
          if (typeLength > 0) {
            schema.addColumn(columnName, dataType, typeLength);
          } else {
            schema.addColumn(columnName, dataType);
          }
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        CatalogUtil.closeSQLWrapper(res);
      }
      
      options = Options.create();
      try {
        String sql = "SELECT key_, value_ from " + TB_OPTIONS
            + " WHERE " + C_TABLE_ID + "='" + name + "'";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        res = stmt.executeQuery(sql);        
        
        while (res.next()) {
          options.put(
              res.getString("key_"),
              res.getString("value_"));          
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        CatalogUtil.closeSQLWrapper(res);
      }

      try {
        String sql = "SELECT num_rows, num_bytes from " + TB_STATISTICS
            + " WHERE " + C_TABLE_ID + "='" + name + "'";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        res = stmt.executeQuery(sql);

        if (res.next()) {
          stat = new TableStats();
          stat.setNumRows(res.getLong("num_rows"));
          stat.setNumBytes(res.getLong("num_bytes"));
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        CatalogUtil.closeSQLWrapper(res);
      }


      try {
        String sql = "SELECT name, type, quantity, columns, expressions from " + TB_PARTTIONS
            + " WHERE TID =" + tid + "";
        stmt = getConnection().createStatement();
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        res = stmt.executeQuery(sql);

        while (res.next()) {
          if (partitions == null) {
            partitions = new Partitions();
            String[] columns = res.getString("columns").split(",");
            for(String eachColumn: columns) {
              partitions.addColumn(getColumn(tableName, tid, eachColumn));
            }
            partitions.setPartitionsType(CatalogProtos.PartitionsType.valueOf(res.getString
                ("type")));
            partitions.setNumPartitions(res.getInt("quantity"));
          }

          Specifier specifier = new Specifier(res.getString("name"), res.getString("expressions"));
          partitions.addSpecifier(specifier);
        }

      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        CatalogUtil.closeSQLWrapper(res, stmt);
      }

      TableMeta meta = new TableMeta(storeType, options);
      TableDesc table = new TableDesc(tableName, schema, meta, path);
      if (stat != null) {
        table.setStats(stat);
      }

      if (partitions != null) {
        table.setPartitions(partitions);
      }

      return table;
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  private Column getColumn(String tableName, int tid, String columnId) throws IOException {
    ResultSet res = null;
    Column column = null;
    Statement stmt = null;

    try {
      String sql = "SELECT column_name, data_type, type_length from "
          + TB_COLUMNS + " WHERE TID = " + tid + " AND column_id = " + columnId;

      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);

      if (res.next()) {
        String columnName = tableName + "."
            + res.getString("column_name").trim();
        Type dataType = getDataType(res.getString("data_type")
            .trim());
        int typeLength = res.getInt("type_length");
        if (typeLength > 0) {
          column = new Column(columnName, dataType, typeLength);
        } else {
          column = new Column(columnName, dataType);
        }
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return column;
  }

  private Type getDataType(final String typeStr) {
    try {
    return Enum.valueOf(Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot find a matched type aginst from '" + typeStr + "'");
      return null;
    }
  }
  
  @Override
  public final List<String> getAllTableNames() throws IOException {
    String sql = "SELECT " + C_TABLE_ID + " from " + TB_TABLES;
    
    Statement stmt = null;
    ResultSet res = null;
    
    List<String> tables = new ArrayList<String>();
    rlock.lock();
    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);
      while (res.next()) {
        tables.add(res.getString(C_TABLE_ID).trim());
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return tables;
  }
  
  public final void addIndex(final IndexDescProto proto) throws IOException {
    String sql = 
        "INSERT INTO indexes (index_name, " + C_TABLE_ID + ", column_name, " 
        +"data_type, index_type, is_unique, is_clustered, is_ascending) VALUES "
        +"(?,?,?,?,?,?,?,?)";
    
    PreparedStatement stmt = null;

    wlock.lock();
    try {
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, proto.getName());
      stmt.setString(2, proto.getTableId());
      stmt.setString(3, proto.getColumn().getColumnName());
      stmt.setString(4, proto.getColumn().getDataType().getType().name());
      stmt.setString(5, proto.getIndexMethod().toString());
      stmt.setBoolean(6, proto.hasIsUnique() && proto.getIsUnique());
      stmt.setBoolean(7, proto.hasIsClustered() && proto.getIsClustered());
      stmt.setBoolean(8, proto.hasIsAscending() && proto.getIsAscending());
      stmt.executeUpdate();
      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.toString());
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      wlock.unlock();
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }
  
  public final void delIndex(final String indexName) throws IOException {
    String sql =
        "DELETE FROM " + TB_INDEXES
        + " WHERE index_name='" + indexName + "'";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }
      
      Statement stmt = null;
      wlock.lock(); 
      try {
        stmt = getConnection().createStatement();
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.executeUpdate(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        wlock.unlock();
        CatalogUtil.closeSQLWrapper(stmt);
      }
  }
  
  public final IndexDescProto getIndex(final String indexName) 
      throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;
    
    IndexDescProto proto = null;
    
    rlock.lock();
    try {
      String sql = 
          "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where index_name = ?";
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, indexName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.toString());
      }
      res = stmt.executeQuery();
      if (!res.next()) {
        throw new IOException("ERROR: there is no index matched to " + indexName);
      }
      proto = resultToProto(res);
    } catch (SQLException se) {
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return proto;
  }
  
  public final IndexDescProto getIndex(final String tableName, 
      final String columnName) throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;
    
    IndexDescProto proto = null;
    
    rlock.lock();
    try {
      String sql = 
          "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where " + C_TABLE_ID + " = ? AND column_name = ?";
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, tableName);
      stmt.setString(2, columnName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery();
      if (!res.next()) {
        throw new IOException("ERROR: there is no index matched to " 
            + tableName + "." + columnName);
      }      
      proto = resultToProto(res);
    } catch (SQLException se) {
      new IOException(se);
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return proto;
  }
  
  public final boolean existIndex(final String indexName) throws IOException {
    String sql = "SELECT index_name from " + TB_INDEXES 
        + " WHERE index_name = ?";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    PreparedStatement stmt = null;
    ResultSet res = null;
    boolean exist = false;
    rlock.lock();
    try {
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, indexName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return exist;
  }
  
  @Override
  public boolean existIndex(String tableName, String columnName)
      throws IOException {
    String sql = "SELECT index_name from " + TB_INDEXES 
        + " WHERE " + C_TABLE_ID + " = ? AND COLUMN_NAME = ?";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    PreparedStatement stmt = null;
    ResultSet res = null;
    boolean exist = false;
    rlock.lock();
    try {
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, tableName);
      stmt.setString(2, columnName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return exist;
  }
  
  public final IndexDescProto [] getIndexes(final String tableName) 
      throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;
    
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    
    rlock.lock();
    try {
      String sql = "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where " + C_TABLE_ID + "= ?";
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery();      
      while (res.next()) {
        protos.add(resultToProto(res));
      }
    } catch (SQLException se) {
    } finally {
      rlock.unlock();
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    
    return protos.toArray(new IndexDescProto [protos.size()]);
  }
  
  private IndexDescProto resultToProto(final ResultSet res) throws SQLException {
    IndexDescProto.Builder builder = IndexDescProto.newBuilder();
    builder.setName(res.getString("index_name"));
    builder.setTableId(res.getString(C_TABLE_ID));
    builder.setColumn(resultToColumnProto(res));
    builder.setIndexMethod(getIndexMethod(res.getString("index_type").trim()));
    builder.setIsUnique(res.getBoolean("is_unique"));
    builder.setIsClustered(res.getBoolean("is_clustered"));
    builder.setIsAscending(res.getBoolean("is_ascending"));
    return builder.build();
  }
  
  private ColumnProto resultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setColumnName(res.getString("column_name"));
    builder.setDataType(CatalogUtil.newSimpleDataType(
        getDataType(res.getString("data_type").trim())));
    return builder.build();
  }
  
  private IndexMethod getIndexMethod(final String typeStr) {
    if (typeStr.equals(IndexMethod.TWO_LEVEL_BIN_TREE.toString())) {
      return IndexMethod.TWO_LEVEL_BIN_TREE;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
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
