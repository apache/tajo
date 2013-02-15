/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package tajo.catalog.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.*;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Hyunsik Choi
 */
public class DBStore implements CatalogStore {
  private final Log LOG = LogFactory.getLog(DBStore.class); 
  private final Configuration conf;
  private final String driver;
  private final String jdbcUri;
  private Connection conn;  

  private static final int VERSION = 1;

  private static final String TB_META = "META";
  private static final String TB_TABLES = "TABLES";
  private static final String TB_COLUMNS = "COLUMNS";
  private static final String TB_OPTIONS = "OPTIONS";
  private static final String TB_INDEXES = "INDEXES";
  private static final String TB_STATISTICS = "STATS";
  
  private static final String C_TABLE_ID = "TABLE_ID";
  
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock rlock = lock.readLock();
  private Lock wlock = lock.writeLock();
  
  public DBStore(final Configuration conf)
      throws InternalException {
    this.conf = conf;
    
    this.driver =
        this.conf.get(CatalogConstants.JDBC_DRIVER, CatalogConstants.DEFAULT_JDBC_DRIVER);
    this.jdbcUri =
        this.conf.get(CatalogConstants.JDBC_URI);

    try {
      Class.forName(driver).newInstance();
      LOG.info("Loaded the JDBC driver (" + driver +")");
    } catch (Exception e) {
      throw new InternalException("Cannot load JDBC driver " + driver, e);
    }

    try {
      LOG.info("Trying to connect database (" + jdbcUri + ")");
      conn = DriverManager.getConnection(jdbcUri + ";create=true");
      LOG.info("Connected to database (" + jdbcUri +")");
    } catch (SQLException e) {
      throw new InternalException("Cannot connect to database (" + jdbcUri
          +")", e);
    }

    try {
      if (!isInitialized()) {
        LOG.info("The base tables of CatalogServer are created.");
        createBaseTable();
      } else {
        LOG.info("The base tables of CatalogServer already is initialized.");
      }
    } catch (SQLException se) {
      throw new InternalException(
          "Cannot initialize the persistent storage of Catalog", se);
    }

    int dbVersion = 0;
    try {
      dbVersion = needUpgrade();
    } catch (SQLException e) {
      throw new InternalException(
          "Cannot check if the DB need to be upgraded", e);
    }

//    if (dbVersion < VERSION) {
//      LOG.info("DB Upgrade is needed");
//      try {
//        upgrade(dbVersion, VERSION);
//      } catch (SQLException e) {
//        LOG.error(e.getMessage());
//        throw new InternalException("DB upgrade is failed.", e);
//      }
//    }
  }

  private int needUpgrade() throws SQLException {
    String sql = "SELECT VERSION FROM " + TB_META;
    Statement stmt = conn.createStatement();
    ResultSet res = stmt.executeQuery(sql);

    if (res.next() == false) { // if this db version is 0
      insertVersion();
      return 0;
    } else {
      return res.getInt(1);
    }
  }

  private void insertVersion() throws SQLException {
    String sql = "INSERT INTO " + TB_META + " values (0)";
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(sql);
    stmt.close();
  }

  private void upgrade(int from, int to) throws SQLException {
    String sql;
    Statement stmt;
    if (from == 0 ) {
      if (to == 1) {
        sql = "DROP INDEX idx_options_key";
        LOG.info(sql);

        stmt = conn.createStatement();
        stmt.addBatch(sql);

        sql =
            "CREATE INDEX idx_options_key on " + TB_OPTIONS + " (" + C_TABLE_ID + ")";
        stmt.addBatch(sql);
        LOG.info(sql);
        stmt.executeBatch();
        stmt.close();

        LOG.info("DB Upgraded from " + from + " to " + to);
      } else {
        LOG.info("DB Upgraded from " + from + " to " + to);
      }
    }
  }

  // TODO - DDL and index statements should be renamed
  private void createBaseTable() throws SQLException {
    wlock.lock();
    try {
      // META
      Statement stmt = conn.createStatement();
      String meta_ddl = "CREATE TABLE " + TB_META + " (version int NOT NULL)";
      if (LOG.isDebugEnabled()) {
        LOG.debug(meta_ddl);
      }
      stmt.executeUpdate(meta_ddl);
      LOG.info("Table '" + TB_META + " is created.");

      // TABLES
      stmt = conn.createStatement();
      String tables_ddl = "CREATE TABLE "
          + TB_TABLES + " ("
          + "TID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
          + C_TABLE_ID + " VARCHAR(256) NOT NULL CONSTRAINT TABLE_ID_UNIQ UNIQUE, "
          + "path VARCHAR(1024), "
          + "store_type CHAR(16), "
          + "options VARCHAR(32672), "
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

      // COLUMNS
      stmt = conn.createStatement();
      String columns_ddl = 
          "CREATE TABLE " + TB_COLUMNS + " ("
          + "TID INT NOT NULL REFERENCES " + TB_TABLES + " (TID) ON DELETE CASCADE, "
          + C_TABLE_ID + " VARCHAR(256) NOT NULL REFERENCES " + TB_TABLES + "("
          + C_TABLE_ID + ") ON DELETE CASCADE, "
          + "column_id INT NOT NULL,"
          + "column_name VARCHAR(256) NOT NULL, " + "data_type CHAR(16), "
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

      // OPTIONS
      stmt = conn.createStatement();
      String options_ddl = 
          "CREATE TABLE " + TB_OPTIONS +" ("
          + C_TABLE_ID + " VARCHAR(256) NOT NULL REFERENCES TABLES (" + C_TABLE_ID +") "
          + "ON DELETE CASCADE, "
          + "key_ VARCHAR(256) NOT NULL, value_ VARCHAR(256) NOT NULL)";
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
      
      // INDEXES
      stmt = conn.createStatement();
      String indexes_ddl = "CREATE TABLE " + TB_INDEXES +"("
          + "index_name VARCHAR(256) NOT NULL PRIMARY KEY, "
          + C_TABLE_ID + " VARCHAR(256) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
          + "ON DELETE CASCADE, "
          + "column_name VARCHAR(256) NOT NULL, "
          + "data_type VARCHAR(256) NOT NULL, "
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

      String stats_ddl = "CREATE TABLE " + TB_STATISTICS + "("
          + C_TABLE_ID + " VARCHAR(256) NOT NULL REFERENCES TABLES (" + C_TABLE_ID + ") "
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

    } finally {
      wlock.unlock();
    }
  }
  
  private boolean isInitialized() throws SQLException {
    wlock.lock();
    try {
      boolean found = false;
      ResultSet res = conn.getMetaData().getTables(null, null, null, 
          new String [] {"TABLE"});
      
      String resName;
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
      wlock.unlock();
    }    
    return false;
  }
  
  final boolean checkInternalTable(final String tableName) throws SQLException {
    rlock.lock();
    try {
      boolean found = false;
      ResultSet res = conn.getMetaData().getTables(null, null, null, 
              new String [] {"TABLE"});
      while(res.next() && !found) {
        if (tableName.equals(res.getString("TABLE_NAME")))
          found = true;
      }
      
      return found;
    } finally {
      rlock.unlock();
    }
  }
  
  @Override
  public final void addTable(final TableDesc table) throws IOException {
    Statement stmt = null;
    ResultSet res;

    String sql = 
        "INSERT INTO " + TB_TABLES + " (" + C_TABLE_ID + ", path, store_type) "
        + "VALUES('" + table.getId() + "', "
        + "'" + table.getPath() + "', "
        + "'" + table.getMeta().getStoreType() + "'"
        + ")";

    wlock.lock();
    try {
      stmt = conn.createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt.addBatch(sql);
      stmt.executeBatch();
      
      stmt = conn.createStatement();
      sql = "SELECT TID from " + TB_TABLES + " WHERE " + C_TABLE_ID 
          + " = '" + table.getId() + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);
      if (!res.next()) {
        throw new IOException("ERROR: there is no tid matched to " 
            + table.getId());
      }
      int tid = res.getInt("TID");

      String colSql;
      int columnId = 0;
      for (Column col : table.getMeta().getSchema().getColumns()) {
        colSql = columnToSQL(tid, table, columnId, col);
        if (LOG.isDebugEnabled()) {
          LOG.debug(colSql);
        }
        stmt.addBatch(colSql);
        columnId++;
      }
      
      Iterator<Entry<String,String>> it = table.getMeta().getOptions();
      String optSql;
      while (it.hasNext()) {
        optSql = keyvalToSQL(table, it.next());
        if (LOG.isDebugEnabled()) {
          LOG.debug(optSql);
        }
        stmt.addBatch(optSql);
      }
      stmt.executeBatch();
      if (table.getMeta().getStat() != null) {
        sql = "INSERT INTO " + TB_STATISTICS + " (" + C_TABLE_ID + ", num_rows, num_bytes) "
            + "VALUES ('" + table.getId() + "', "
            + table.getMeta().getStat().getNumRows() + ","
            + table.getMeta().getStat().getNumBytes() + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
      }
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      wlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }      
    }
  }
  
  private String columnToSQL(final int tid, final TableDesc desc, 
      final int columnId, final Column col) {
    String sql =
        "INSERT INTO " + TB_COLUMNS 
        + " (tid, " + C_TABLE_ID + ", column_id, column_name, data_type) "
        + "VALUES("
        + tid + ","
        + "'" + desc.getId() + "',"
        + columnId + ", "
        + "'" + col.getColumnName() + "',"
        + "'" + col.getDataType().toString() + "'"
        + ")";
    
    return sql;
  }
  
  private String keyvalToSQL(final TableDesc desc,
      final Entry<String,String> keyVal) {
    String sql =
        "INSERT INTO " + TB_OPTIONS 
        + " (" + C_TABLE_ID + ", key_, value_) "
        + "VALUES("
        + "'" + desc.getId() + "',"
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
    boolean exist = false;
    rlock.lock();
    try {
      stmt = conn.createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }
      ResultSet res = stmt.executeQuery(sql.toString());
      exist = res.next();
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
      }         
    }
    
    return exist;
  }

  @Override
  public final void deleteTable(final String name) throws IOException {
    Statement stmt = null;
    String sql = null;
    try {
      wlock.lock();
      try {
        stmt = conn.createStatement();
        sql = "DELETE FROM " + TB_COLUMNS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
        }
      }

      try {
        sql = "DELETE FROM " + TB_OPTIONS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt = conn.createStatement();
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
        }
      }

      try {
        sql = "DELETE FROM " + TB_STATISTICS +
            " WHERE " + C_TABLE_ID + " = '" + name + "'";
        LOG.info(sql);
        stmt = conn.createStatement();
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
        }
      }

      try {
        sql = "DELETE FROM " + TB_TABLES +
            " WHERE " + C_TABLE_ID +" = '" + name + "'";
        LOG.info(sql);
        stmt = conn.createStatement();
        stmt.execute(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
        }
      }

    } finally {
      wlock.unlock();
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
    TableStat stat = null;

    rlock.lock();
    try {
      try {
        String sql = 
            "SELECT " + C_TABLE_ID + ", path, store_type from " + TB_TABLES
            + " WHERE " + C_TABLE_ID + "='" + name + "'";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt = conn.createStatement();
        res = stmt.executeQuery(sql);
        if (!res.next()) { // there is no table of the given name.
          return null;
        }
        tableName = res.getString(C_TABLE_ID).trim();
        path = new Path(res.getString("path").trim());
        storeType = CatalogUtil.getStoreType(res.getString("store_type").trim());
      } catch (SQLException se) { 
        throw new IOException(se);
      } finally {
        stmt.close();
        res.close();
      }
      
      Schema schema = null;
      try {
        String sql = "SELECT column_name, data_type from " + TB_COLUMNS
            + " WHERE " + C_TABLE_ID + "='" + name + "' ORDER by column_id asc";

        stmt = conn.createStatement();
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        res = stmt.executeQuery(sql);

        schema = new Schema();
        while (res.next()) {
          String columnName = tableName + "." 
              + res.getString("column_name").trim();
          DataType dataType = getDataType(res.getString("data_type")
              .trim());
          schema.addColumn(columnName, dataType);
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        stmt.close();
        res.close();
      }
      
      options = Options.create();
      try {
        String sql = "SELECT key_, value_ from " + TB_OPTIONS
            + " WHERE " + C_TABLE_ID + "='" + name + "'";
        stmt = conn.createStatement();
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
        stmt.close();
        res.close();
      }

      try {
        String sql = "SELECT num_rows, num_bytes from " + TB_STATISTICS
            + " WHERE " + C_TABLE_ID + "='" + name + "'";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt = conn.createStatement();
        res = stmt.executeQuery(sql);

        if (res.next()) {
          stat = new TableStat();
          stat.setNumRows(res.getLong("num_rows"));
          stat.setNumBytes(res.getLong("num_bytes"));
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        stmt.close();
        res.close();
      }

      TableMeta meta = new TableMetaImpl(schema, storeType, options);
      if (stat != null) {
        meta.setStat(stat);
      }
      TableDesc table = new TableDescImpl(tableName, meta, path);

      return table;
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
    }
  }
  
  private DataType getDataType(final String typeStr) {
    if (typeStr.equals(DataType.BOOLEAN.toString())) {
      return DataType.BOOLEAN;
    } else if (typeStr.equals(DataType.BYTE.toString())) {
      return DataType.BYTE;
    } else if (typeStr.equals(DataType.CHAR.toString())) {
      return DataType.CHAR;
    } else if (typeStr.equals(DataType.SHORT.toString())) {
      return DataType.SHORT;
    } else if (typeStr.equals(DataType.INT.toString())) {
      return DataType.INT;
    } else if (typeStr.equals(DataType.LONG.toString())) {
      return DataType.LONG;
    } else if (typeStr.equals(DataType.FLOAT.toString())) {
      return DataType.FLOAT;
    } else if (typeStr.equals(DataType.DOUBLE.toString())) {
      return DataType.DOUBLE;
    } else if (typeStr.equals(DataType.STRING.toString())) {
      return DataType.STRING;
    } else if (typeStr.equals(DataType.IPv4.toString())) {
      return DataType.IPv4;
    } else if (typeStr.equals(DataType.IPv6.toString())) {
      return DataType.IPv6;
    } else if (typeStr.equals(DataType.BYTES.toString())) {
      return DataType.BYTES;
    } else if (typeStr.equals(DataType.DATE.toString())) {
      return DataType.DATE;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
  
  @Override
  public final List<String> getAllTableNames() throws IOException {
    String sql = "SELECT " + C_TABLE_ID + " from " + TB_TABLES;
    
    Statement stmt = null;
    ResultSet res;
    
    List<String> tables = new ArrayList<String>();
    rlock.lock();
    try {
      stmt = conn.createStatement();
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
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
      }
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
      stmt = conn.prepareStatement(sql);
      stmt.setString(1, proto.getName());
      stmt.setString(2, proto.getTableId());
      stmt.setString(3, proto.getColumn().getColumnName());
      stmt.setString(4, proto.getColumn().getDataType().toString());
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
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {     
      }
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
        stmt = conn.createStatement();
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.executeUpdate(sql);
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        wlock.unlock();
        try {
          if (stmt != null) {
            stmt.close();
          }
        } catch (SQLException e) {
        }      
      }
  }
  
  public final IndexDescProto getIndex(final String indexName) 
      throws IOException {
    ResultSet res;
    PreparedStatement stmt;
    
    IndexDescProto proto = null;
    
    rlock.lock();
    try {
      String sql = 
          "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where index_name = ?";
      stmt = conn.prepareStatement(sql);
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
    }
    
    return proto;
  }
  
  public final IndexDescProto getIndex(final String tableName, 
      final String columnName) throws IOException {
    ResultSet res;
    PreparedStatement stmt;
    
    IndexDescProto proto = null;
    
    rlock.lock();
    try {
      String sql = 
          "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where " + C_TABLE_ID + " = ? AND column_name = ?";
      stmt = conn.prepareStatement(sql);
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
    boolean exist = false;
    rlock.lock();
    try {
      stmt = conn.prepareStatement(sql);
      stmt.setString(1, indexName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      ResultSet res = stmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      
    } finally {
      rlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }         
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
    boolean exist = false;
    rlock.lock();
    try {
      stmt = conn.prepareStatement(sql);
      stmt.setString(1, tableName);
      stmt.setString(2, columnName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      ResultSet res = stmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      
    } finally {
      rlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }         
    }
    
    return exist;
  }
  
  public final IndexDescProto [] getIndexes(final String tableName) 
      throws IOException {
    ResultSet res;
    PreparedStatement stmt;
    
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    
    rlock.lock();
    try {
      String sql = "SELECT index_name, " + C_TABLE_ID + ", column_name, data_type, " 
          + "index_type, is_unique, is_clustered, is_ascending FROM indexes "
          + "where " + C_TABLE_ID + "= ?";
      stmt = conn.prepareStatement(sql);
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
    builder.setDataType(getDataType(res.getString("data_type").trim()));
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
    
    LOG.info("Shutdown database (" + jdbcUri + ")");
  }
  
  
  public static void main(final String[] args) throws IOException {
    @SuppressWarnings("unused")
    DBStore store = new DBStore(new TajoConf());
  }
}