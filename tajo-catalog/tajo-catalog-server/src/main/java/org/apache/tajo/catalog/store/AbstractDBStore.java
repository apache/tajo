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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public abstract class AbstractDBStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());
  protected Configuration conf;
  protected String connectionId;
  protected String connectionPassword;
  protected String catalogUri;
  private Connection conn;

  protected static final int VERSION = 1;

  protected abstract String getCatalogDriverName();

  protected abstract Connection createConnection(final Configuration conf) throws SQLException;

  protected abstract boolean isInitialized() throws SQLException;

  protected abstract void createBaseTable() throws SQLException;

  public AbstractDBStore(Configuration conf)
      throws InternalException {

    this.conf = conf;

    if(conf.get(CatalogConstants.DEPRECATED_CATALOG_URI) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CATALOG_URI + " " +
          "is deprecated. Use " + CatalogConstants.CATALOG_URI + " instead.");
      this.catalogUri = conf.get(CatalogConstants.DEPRECATED_CATALOG_URI);
    } else {
      this.catalogUri = conf.get(CatalogConstants.CATALOG_URI);
    }

    if(conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CONNECTION_ID + " " +
          "is deprecated. Use " + CatalogConstants.CONNECTION_ID + " instead.");
      this.connectionId = conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID);
    } else {
      this.connectionId = conf.get(CatalogConstants.CONNECTION_ID);
    }

    if(conf.get(CatalogConstants.DEPRECATED_CONNECTION_PASSWORD) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CONNECTION_PASSWORD + " " +
          "is deprecated. Use " + CatalogConstants.CONNECTION_PASSWORD + " instead.");
      this.connectionPassword = conf.get(CatalogConstants.DEPRECATED_CONNECTION_PASSWORD);
    } else {
      this.connectionPassword = conf.get(CatalogConstants.CONNECTION_PASSWORD);
    }

    String catalogDriver = getCatalogDriverName();
    try {
      Class.forName(getCatalogDriverName()).newInstance();
      LOG.info("Loaded the Catalog driver (" + catalogDriver + ")");
    } catch (Exception e) {
      throw new InternalException("Cannot load Catalog driver " + catalogDriver, e);
    }

    try {
      LOG.info("Trying to connect database (" + catalogUri + ")");
      conn = createConnection(conf);
      LOG.info("Connected to database (" + catalogUri + ")");
    } catch (SQLException e) {
      throw new InternalException("Cannot connect to database (" + catalogUri
          + ")", e);
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

  protected String getCatalogUri(){
    return catalogUri;
  }

  public Connection getConnection() throws SQLException{
    boolean isValid = false;
    try{
      isValid = conn.isValid(100);
    } catch (SQLException e){
    }

    if(!isValid){
      CatalogUtil.closeSQLWrapper(conn);
      conn = createConnection(conf);
    }
    return conn;
  }

  private int needUpgrade() throws SQLException {
    String sql = "SELECT VERSION FROM " + TB_META;
    Statement stmt = getConnection().createStatement();
    ResultSet res = stmt.executeQuery(sql);

    if (!res.next()) { // if this db version is 0
      insertVersion();
      return 0;
    } else {
      return res.getInt(1);
    }
  }

  private void insertVersion() throws SQLException {
    String sql = "INSERT INTO " + TB_META + " values (0)";
    Statement stmt = getConnection().createStatement();
    stmt.executeUpdate(sql);
    stmt.close();
  }

  private void upgrade(int from, int to) throws SQLException {
    String sql;
    Statement stmt;
    if (from == 0) {
      if (to == 1) {
        sql = "DROP INDEX idx_options_key";
        LOG.info(sql);

        stmt = getConnection().createStatement();
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


  @Override
  public void addTable(final TableDesc table) throws IOException {
    Statement stmt = null;
    ResultSet res;

    String sql =
        "INSERT INTO " + TB_TABLES + " (" + C_TABLE_ID + ", path, store_type) "
            + "VALUES('" + table.getName() + "', "
            + "'" + table.getPath() + "', "
            + "'" + table.getMeta().getStoreType() + "'"
            + ")";

    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt.executeUpdate(sql);


      stmt = getConnection().createStatement();
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


      String optSql = String.format("INSERT INTO %s (%s, key_, value_) VALUES(?, ?, ?)", TB_OPTIONS, C_TABLE_ID);
      PreparedStatement pstmt = getConnection().prepareStatement(optSql);
      try {
        for (Entry<String, String> entry : table.getMeta().toMap().entrySet()) {
          pstmt.setString(1, table.getName());
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          if (LOG.isDebugEnabled()) {
            LOG.debug(optSql);
          }
          pstmt.addBatch();
        }
        pstmt.executeBatch();
      } finally {
        CatalogUtil.closeSQLWrapper(pstmt);
      }

      if (table.getStats() != null) {
        sql = "INSERT INTO " + TB_STATISTICS + " (" + C_TABLE_ID + ", num_rows, num_bytes) "
            + "VALUES ('" + table.getName() + "', "
            + table.getStats().getNumRows() + ","
            + table.getStats().getNumBytes() + ")";
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.addBatch(sql);
        stmt.executeBatch();
      }
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
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

  @Override
  public boolean existTable(final String name) throws IOException {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT " + C_TABLE_ID + " from ")
        .append(TB_TABLES)
        .append(" WHERE " + C_TABLE_ID + " = '")
        .append(name)
        .append("'");

    Statement stmt = null;
    boolean exist = false;

    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }
      ResultSet res = stmt.executeQuery(sql.toString());
      exist = res.next();
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }

    return exist;
  }

  @Override
  public void deleteTable(final String name) throws IOException {
    Statement stmt = null;
    String sql = null;

    try {
      stmt = getConnection().createStatement();
      sql = "DELETE FROM " + TB_COLUMNS +
          " WHERE " + C_TABLE_ID + " = '" + name + "'";
      LOG.info(sql);
      stmt.execute(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }

    try {
      sql = "DELETE FROM " + TB_OPTIONS +
          " WHERE " + C_TABLE_ID + " = '" + name + "'";
      LOG.info(sql);
      stmt = getConnection().createStatement();
      stmt.execute(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }

    try {
      sql = "DELETE FROM " + TB_STATISTICS +
          " WHERE " + C_TABLE_ID + " = '" + name + "'";
      LOG.info(sql);
      stmt = getConnection().createStatement();
      stmt.execute(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }

    try {
      sql = "DELETE FROM " + TB_TABLES +
          " WHERE " + C_TABLE_ID + " = '" + name + "'";
      LOG.info(sql);
      stmt = getConnection().createStatement();
      stmt.execute(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  public TableDesc getTable(final String name) throws IOException {
    ResultSet res = null;
    Statement stmt = null;

    String tableName = null;
    Path path = null;
    StoreType storeType = null;
    Options options;
    TableStats stat = null;

    try {
      String sql =
          "SELECT " + C_TABLE_ID + ", path, store_type from " + TB_TABLES
              + " WHERE " + C_TABLE_ID + "='" + name + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    Schema schema = null;
    try {
      String sql = "SELECT column_name, data_type, type_length from " + TB_COLUMNS
          + " WHERE " + C_TABLE_ID + "='" + name + "' ORDER by column_id asc";

      stmt = getConnection().createStatement();
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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    options = Options.create();
    try {
      String sql = "SELECT key_, value_ from " + TB_OPTIONS
          + " WHERE " + C_TABLE_ID + "='" + name + "'";
      stmt = getConnection().createStatement();
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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    try {
      String sql = "SELECT num_rows, num_bytes from " + TB_STATISTICS
          + " WHERE " + C_TABLE_ID + "='" + name + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
      res = stmt.executeQuery(sql);

      if (res.next()) {
        stat = new TableStats();
        stat.setNumRows(res.getLong("num_rows"));
        stat.setNumBytes(res.getLong("num_bytes"));
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

    return table;
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
  public List<String> getAllTableNames() throws IOException {
    String sql = "SELECT " + C_TABLE_ID + " from " + TB_TABLES;

    Statement stmt = null;
    ResultSet res = null;

    List<String> tables = new ArrayList<String>();

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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return tables;
  }

  @Override
  public void addIndex(final IndexDescProto proto) throws IOException {
    String sql =
        "INSERT INTO indexes (index_name, " + C_TABLE_ID + ", column_name, "
            + "data_type, index_type, is_unique, is_clustered, is_ascending) VALUES "
            + "(?,?,?,?,?,?,?,?)";

    PreparedStatement stmt = null;

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
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  public void delIndex(final String indexName) throws IOException {
    String sql =
        "DELETE FROM " + TB_INDEXES
            + " WHERE index_name='" + indexName + "'";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    Statement stmt = null;

    try {
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt.executeUpdate(sql);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  @Override
  public IndexDescProto getIndex(final String indexName)
      throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;

    IndexDescProto proto = null;

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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return proto;
  }

  @Override
  public IndexDescProto getIndex(final String tableName,
                                 final String columnName) throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;

    IndexDescProto proto = null;

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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return proto;
  }

  @Override
  public boolean existIndex(final String indexName) throws IOException {
    String sql = "SELECT index_name from " + TB_INDEXES
        + " WHERE index_name = ?";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    PreparedStatement stmt = null;
    ResultSet res = null;
    boolean exist = false;

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
    boolean exist = false;
    ResultSet res = null;

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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return exist;
  }

  @Override
  public IndexDescProto[] getIndexes(final String tableName)
      throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;

    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();

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
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
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
    builder.setDataType(CatalogUtil.newSimpleDataType(getDataType(res.getString("data_type").trim())));
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
  public void close() {
    CatalogUtil.closeSQLWrapper(conn);
    LOG.info("Shutdown database (" + catalogUri + ")");
  }
}
