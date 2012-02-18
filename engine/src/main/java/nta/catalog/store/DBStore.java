/**
 * 
 */
package nta.catalog.store;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nta.catalog.Column;
import nta.catalog.FunctionDesc;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TConstants;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.exception.InternalException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * @author Hyunsik Choi
 */
public class DBStore implements CatalogStore {
  private final Log LOG = LogFactory.getLog(DBStore.class); 
  private final Configuration conf;
  private final String driver;
  private final String jdbcUri;
  private Connection conn;  
  
  private static final String TB_META = "META";
  private static final String TB_TABLES = "TABLES";
  private static final String TB_COLUMNS = "COLUMNS";
  
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock rlock = lock.readLock();
  private Lock wlock = lock.writeLock();
  
  public DBStore(final Configuration conf) throws InternalException {
    this.conf = conf;
    
    this.driver =
        this.conf.get(TConstants.JDBC_DRIVER, TConstants.DEFAULT_JDBC_DRIVER);
    this.jdbcUri =
        this.conf.get(TConstants.JDBC_URI);
    
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
  }
  
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
          + TB_TABLES
          + " (table_name VARCHAR(256) NOT NULL PRIMARY KEY, path VARCHAR(1024), "
          + " store_type char(16), options VARCHAR(32672))";
      LOG.info(tables_ddl);
      if (LOG.isDebugEnabled()) {
        LOG.debug(tables_ddl);
      }
      stmt.addBatch(tables_ddl);
      String tables_idx = "CREATE UNIQUE INDEX idx_table_name on " + TB_TABLES
          + "(table_name)";
      LOG.info(tables_idx);
      stmt.addBatch(tables_idx);
      stmt.executeBatch();
      LOG.info("Table '" + TB_TABLES + "' is created.");

      // COLUMNS
      stmt = conn.createStatement();
      String columns_ddl = "CREATE TABLE " + TB_COLUMNS + "("
          + "table_name VARCHAR(256) NOT NULL REFERENCES TABLES (TABLE_NAME) "
          + "ON DELETE CASCADE, "
          + "column_name VARCHAR(256) NOT NULL, " + "data_type CHAR(16)" + ")";
      LOG.info(columns_ddl);
      if (LOG.isDebugEnabled()) {
        LOG.debug(columns_ddl);
      }
      stmt.addBatch(columns_ddl);

      String columns_idx_fk_table_name = "CREATE INDEX idx_fk_table_name on "
          + TB_COLUMNS + "(table_name)";
      LOG.info(columns_idx_fk_table_name);
      stmt.addBatch(columns_idx_fk_table_name);
      stmt.executeBatch();
      LOG.info("Table '" + TB_COLUMNS + " is created.");

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
      while (res.next() && found == false) {
        resName = res.getString("TABLE_NAME");
        if (TB_META.equals(resName)
            || TB_TABLES.equals(resName)) {
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
      while(res.next() && found == false) {
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

    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO " + TB_TABLES + " (table_name, path, store_type)")
        .append("VALUES(").append("'" + table.getId() + "'")
        .append(",'" + table.getPath() + "'")
        .append(",'" + table.getMeta().getStoreType() + "'").append(")");
    LOG.info(sql.toString());
    wlock.lock();
    try {
      stmt = conn.createStatement();      
      stmt.addBatch(sql.toString());

      String colSql = null;
      for (Column col : table.getMeta().getSchema().getColumns()) {
        colSql = columnToInsertSQL(table, col);
        LOG.info(colSql);
        stmt.addBatch(colSql);
      }
      stmt.executeBatch();
    } catch (SQLException se) {

    } finally {
      wlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }      
    }
  }
  
  private String columnToInsertSQL(final TableDesc desc, final Column col) {
    String sql =
        "INSERT INTO " + TB_COLUMNS 
        + "(table_name, column_name, data_type) "
        + "VALUES("
        + "'" + desc.getId() + "',"
        + "'" + col.getColumnName() + "',"
        + "'" + col.getDataType().toString() + "'"
        + ")";
    
    return sql;
  }
  
  @Override
  public final boolean existTable(final String name) throws IOException {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT table_name from ")
    .append(TB_TABLES)
    .append(" WHERE table_name = '")
    .append(name)
    .append("'");    
    LOG.info(sql.toString());

    Statement stmt = null;
    boolean exist = false;
    rlock.lock();
    try {
      stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(sql.toString());
      exist = res.next() == true;
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
  public final void deleteTable(final String name) throws IOException {
    String sql =
      "DELETE FROM " + TB_TABLES
      + " WHERE table_name='" + name + "'";
    LOG.info(sql.toString());
    
    Statement stmt = null;
    wlock.lock(); 
    try {
      stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      wlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }      
    }
  }

  @Override
  public final TableDesc getTable(final String name) throws IOException {
    String sql = "SELECT table_name, path, store_type from " + TB_TABLES
        + " WHERE table_name='" + name + "'";

    ResultSet tableRes = null;
    Statement tableStmt = null;

    TableDesc table = null;
    String tableName = null;
    Path path = null;
    StoreType storeType = null;

    rlock.lock();
    try {
      try {
        tableStmt = conn.createStatement();
        tableRes = tableStmt.executeQuery(sql);
        if (!tableRes.next()) { // there is no table of the given name.
          return null;
        }
        tableName = tableRes.getString("table_name").trim();
        path = new Path(tableRes.getString("path").trim());
        storeType = getStoreType(tableRes.getString("store_type").trim());
      } catch (SQLException se) { 
        throw new IOException(se);
      } finally {
        tableStmt.close();
      }

      sql = "SELECT column_name, data_type from " + TB_COLUMNS
          + " WHERE table_name='" + name + "'";

      Schema schema = null;
      try {
        tableStmt = conn.createStatement();
        tableRes = tableStmt.executeQuery(sql);

        schema = new Schema();
        while (tableRes.next()) {
          String columnName = tableName + "." 
              + tableRes.getString("column_name").trim();
          DataType dataType = getDataType(tableRes.getString("data_type")
              .trim());
          schema.addColumn(columnName, dataType);
        }
      } catch (SQLException se) {
        throw new IOException(se);
      } finally {
        tableStmt.close();
      }

      TableMeta meta = new TableMetaImpl(schema, storeType, new Options());
      table = new TableDescImpl(tableName, meta, path);

      return table;
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
    }
  }
  
  private StoreType getStoreType(final String typeStr) {
    if (typeStr.equals(StoreType.CSV.toString())) {
      return StoreType.CSV;
    } else if (typeStr.equals(StoreType.RAW.toString())) {
      return StoreType.RAW;
    } else if (typeStr.equals(StoreType.CSV.toString())) {
      return StoreType.CSV;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
  
  private DataType getDataType(final String typeStr) {
    if (typeStr.equals(DataType.BOOLEAN.toString())) {
      return DataType.BOOLEAN;
    } else if (typeStr.equals(DataType.BYTE.toString())) {
      return DataType.BYTE;
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
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
  
  @Override
  public final List<String> getAllTableNames() throws IOException {
    String sql = "SELECT table_name from " + TB_TABLES;
    
    Statement stmt = null;
    ResultSet res = null;
    
    List<String> tables = new ArrayList<String>();
    rlock.lock();
    try {
      stmt = conn.createStatement();
      res = stmt.executeQuery(sql);
      while (res.next()) {
        tables.add(res.getString("table_name").trim());
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      rlock.unlock();
      try {
        stmt.close();
      } catch (SQLException e) {
      }
    }
    return tables;
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
    DBStore store = new DBStore(NtaConf.create());
  }
}