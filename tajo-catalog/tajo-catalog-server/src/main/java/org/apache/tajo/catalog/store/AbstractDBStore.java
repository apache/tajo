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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractDBStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());
  protected Configuration conf;
  protected String connectionId;
  protected String connectionPassword;
  protected String catalogUri;
  private Connection conn;
  protected Map<String, Boolean> baseTableMaps = new HashMap<String, Boolean>();

  protected static final int VERSION = 1;

  protected abstract String getCatalogDriverName();

  protected abstract Connection createConnection(final Configuration conf) throws SQLException;

  protected abstract boolean isInitialized() throws IOException;

  protected abstract void createBaseTable() throws IOException;

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
    } catch (IOException se) {
      throw new InternalException(
          "Cannot initialize the persistent storage of Catalog", se);
    }

    int dbVersion = 0;
    try {
      dbVersion = needUpgrade();
    } catch (IOException e) {
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

  public Connection getConnection() throws IOException {
    try {
      boolean isValid = conn.isValid(100);
      if (!isValid) {
        CatalogUtil.closeSQLWrapper(conn);
        conn = createConnection(conf);
      }
    } catch (SQLException e) {
      CatalogUtil.closeSQLWrapper(conn);
      throw new IOException(e);
    }
    return conn;
  }

  private int needUpgrade() throws IOException {
    Statement stmt = null;
    try {
      String sql = "SELECT VERSION FROM " + TB_META;
      stmt = getConnection().createStatement();
      ResultSet res = stmt.executeQuery(sql);

      if (!res.next()) { // if this db version is 0
        insertVersion();
        return 0;
      } else {
        return res.getInt(1);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  private void insertVersion() throws IOException {
    Statement stmt = null;
    try {
      String sql = "INSERT INTO " + TB_META + " values (0)";
      stmt = getConnection().createStatement();
      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }

  private void upgrade(int from, int to) throws IOException {
    Statement stmt = null;
    try {
      if (from == 0) {
        if (to == 1) {
          String sql = "DROP INDEX idx_options_key";
          LOG.info(sql);

          stmt = getConnection().createStatement();
          stmt.addBatch(sql);

          sql =
              "CREATE INDEX idx_options_key on " + TB_OPTIONS + " (" + C_TABLE_ID + ")";
          stmt.addBatch(sql);
          LOG.info(sql);
          stmt.executeBatch();

          LOG.info("DB Upgraded from " + from + " to " + to);
        } else {
          LOG.info("DB Upgraded from " + from + " to " + to);
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
  }


  @Override
  public void addTable(final CatalogProtos.TableDescProto table) throws IOException {
    PreparedStatement pstmt = null;
    ResultSet res = null;
    Connection conn = getConnection();
    try {
      conn.setAutoCommit(false);

      String tableName = table.getId().toLowerCase();

      String sql = String.format("INSERT INTO %s (%s, path, store_type) VALUES(?, ?, ?) ", TB_TABLES, C_TABLE_ID);
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      pstmt.setString(2, table.getPath());
      pstmt.setString(3, table.getMeta().getStoreType().name());
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      pstmt.executeUpdate();

      String tidSql = String.format("SELECT TID from %s WHERE %s = ?", TB_TABLES, C_TABLE_ID);
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setString(1, tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(tidSql);
      }
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new IOException("ERROR: there is no tid matched to "
            + table.getId());
      }
      int tid = res.getInt("TID");

      String colSql = String.format("INSERT INTO %s (TID, %s, column_id, column_name, data_type, type_length)"
          + " VALUES(?, ?, ?, ?, ?, ?) ", TB_COLUMNS, C_TABLE_ID);
      pstmt = conn.prepareStatement(colSql);
      for(int i = 0; i < table.getSchema().getFieldsCount(); i++) {
        ColumnProto col = table.getSchema().getFields(i);
        pstmt.setInt(1, tid);
        pstmt.setString(2, tableName);
        pstmt.setInt(3, i);
        pstmt.setString(4, col.getColumnName());
        pstmt.setString(5, col.getDataType().getType().name());
        pstmt.setInt(6, (col.getDataType().hasLength() ? col.getDataType().getLength() : 0));
        pstmt.addBatch();
        if (LOG.isDebugEnabled()) {
          LOG.debug(colSql);
        }
      }
      pstmt.executeBatch();

      if(table.getMeta().hasParams()) {
        String optSql = String.format("INSERT INTO %s (%s, key_, value_) VALUES(?, ?, ?)", TB_OPTIONS, C_TABLE_ID);
        pstmt = conn.prepareStatement(optSql);
        for (CatalogProtos.KeyValueProto entry : table.getMeta().getParams().getKeyvalList()) {
          pstmt.setString(1, tableName);
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          pstmt.addBatch();
          if (LOG.isDebugEnabled()) {
            LOG.debug(optSql);
          }
        }
        pstmt.executeBatch();
      }


      if (table.hasStats()) {
        String statSql =
            String.format("INSERT INTO %s (%s, num_rows, num_bytes) VALUES(?, ?, ?)", TB_STATISTICS, C_TABLE_ID);
        pstmt = conn.prepareStatement(statSql);
        pstmt.setString(1, tableName);
        pstmt.setLong(2, table.getStats().getNumRows());
        pstmt.setLong(3, table.getStats().getNumBytes());
        pstmt.addBatch();
        if (LOG.isDebugEnabled()) {
          LOG.debug(statSql);
        }
        pstmt.executeBatch();
      }

      if(table.hasPartition()) {
        String partSql =
            String.format("INSERT INTO %s (%s, partition_type, expression, expression_schema) VALUES(?, ?, ?, ?)",
                TB_PARTITION_METHODS, C_TABLE_ID);
        pstmt = conn.prepareStatement(partSql);
        pstmt.setString(1, tableName);
        pstmt.setString(2, table.getPartition().getPartitionType().name());
        pstmt.setString(3, table.getPartition().getExpression());
        pstmt.setBytes(4, table.getPartition().getExpressionSchema().toByteArray());
        pstmt.executeUpdate();
      }

      conn.commit();
    } catch (SQLException se) {
      try {
        conn.rollback();
      } catch (SQLException rbe) {
        throw new IOException(se.getMessage(), rbe);
      }
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(pstmt);
    }
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
      getConnection().setAutoCommit(false);

    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(stmt);
    }
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
      sql = "DELETE FROM " + TB_PARTTIONS +
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
  public CatalogProtos.TableDescProto getTable(final String name) throws IOException {
    ResultSet res = null;
    Statement stmt = null;

    CatalogProtos.TableDescProto.Builder tableBuilder = CatalogProtos.TableDescProto.newBuilder();
    StoreType storeType = null;
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
      tableBuilder.setId(res.getString(C_TABLE_ID).trim());
      tableBuilder.setPath(res.getString("path").trim());

      storeType = CatalogUtil.getStoreType(res.getString("store_type").trim());

    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    CatalogProtos.SchemaProto.Builder schemaBuilder = CatalogProtos.SchemaProto.newBuilder();
    try {
      String sql = "SELECT column_name, data_type, type_length from " + TB_COLUMNS
          + " WHERE " + C_TABLE_ID + "='" + name + "' ORDER by column_id asc";

      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);
      while (res.next()) {
        schemaBuilder.addFields(resultToColumnProto(res));
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    tableBuilder.setSchema(CatalogUtil.getQualfiedSchema(name, schemaBuilder.build()));

    CatalogProtos.TableProto.Builder metaBuilder = CatalogProtos.TableProto.newBuilder();
    metaBuilder.setStoreType(storeType);
    try {
      String sql = "SELECT key_, value_ from " + TB_OPTIONS
          + " WHERE " + C_TABLE_ID + "='" + name + "'";
      stmt = getConnection().createStatement();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      res = stmt.executeQuery(sql);
      metaBuilder.setParams(resultToKeyValueSetProto(res));
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    tableBuilder.setMeta(metaBuilder);

    try {
      String sql = "SELECT num_rows, num_bytes from " + TB_STATISTICS
          + " WHERE " + C_TABLE_ID + "='" + name + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
      res = stmt.executeQuery(sql);

      if (res.next()) {
        TableStatsProto.Builder statBuilder = TableStatsProto.newBuilder();
        statBuilder.setNumRows(res.getLong("num_rows"));
        statBuilder.setNumBytes(res.getLong("num_bytes"));
        tableBuilder.setStats(statBuilder);
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    try {
      String sql =
          "SELECT partition_type, expression, expression_schema from " + TB_PARTITION_METHODS
              + " WHERE " + C_TABLE_ID + "='" + name + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
      res = stmt.executeQuery(sql);

      if (res.next()) {
        tableBuilder.setPartition(resultToPartitionMethodProto(name, res));
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return tableBuilder.build();
  }

  private ColumnProto getColumn(String tableName, int tid, String columnId) throws IOException {
    ResultSet res = null;
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
        return resultToColumnProto(res);
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return null;
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
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws  IOException {
    PreparedStatement pstmt = null;
    String sql = "INSERT INTO " + TB_PARTTIONS + " (" + C_TABLE_ID
        + ",  partition_name, ordinal_position, path, cache_nodes) "
        + "VALUES (?, ?, ?, ?, ?, ?) ";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }
    try {
      pstmt = getConnection().prepareStatement(sql);
      for (CatalogProtos.PartitionDescProto proto : partitionsProto.getPartitionList()) {
        pstmt.setString(1, proto.getTableId());
        pstmt.setString(2, proto.getPartitionName());
        pstmt.setInt(3, proto.getOrdinalPosition());
        pstmt.setString(4, proto.getPartitionValue());
        pstmt.setString(5, proto.getPath());
        pstmt.addBatch();
      }
      pstmt.executeBatch();
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(pstmt);
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto proto) throws IOException {
    PreparedStatement pstmt = null;
    String sql = "INSERT INTO " + TB_PARTITION_METHODS + " (" + C_TABLE_ID
        + ", partition_type,  expression, expression_schema) "
        + "VALUES (?, ?, ?, ?) ";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    try {
      pstmt = getConnection().prepareStatement(sql);
      pstmt.setString(1, proto.getTableId().toLowerCase());
      pstmt.setString(2, proto.getPartitionType().name());
      pstmt.setString(3, proto.getExpression());
      pstmt.setBytes(4, proto.getExpressionSchema().toByteArray());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(pstmt);
    }
  }

  @Override
  public void delPartitionMethod(String tableName) throws IOException {
    String sql =
        "DELETE FROM " + TB_PARTITION_METHODS
            + " WHERE " + C_TABLE_ID + " ='" + tableName + "'";
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
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String tableName) throws IOException {
    ResultSet res = null;
    Statement stmt = null;

    try {
      String sql =
          "SELECT partition_type, expression, expression_schema from " + TB_PARTITION_METHODS
              + " WHERE " + C_TABLE_ID + "='" + tableName + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
      res = stmt.executeQuery(sql);

      if (res.next()) {
        return resultToPartitionMethodProto(tableName, res);
      }
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return null;
  }

  @Override
  public boolean existPartitionMethod(String tableName) throws IOException {
    ResultSet res = null;
    Statement stmt = null;
    boolean exist = false;
    try {
      String sql =
          "SELECT partition_type, expression, expression_schema from " + TB_PARTITION_METHODS
              + " WHERE " + C_TABLE_ID + "='" + tableName + "'";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      stmt = getConnection().createStatement();
      res = stmt.executeQuery(sql);
      exist = res.next();
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }
    return exist;
  }

  @Override
  public void addPartition(CatalogProtos.PartitionDescProto proto) throws IOException {
    PreparedStatement pstmt = null;
    String sql = "INSERT INTO " + TB_PARTTIONS + " (" + C_TABLE_ID
        + ", partition_method_id,  partition_name, ordinal_position, path, cache_nodes) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?) ";
     if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    try {
      pstmt = getConnection().prepareStatement(sql);
      pstmt.setString(1, proto.getTableId().toLowerCase());
      pstmt.setString(2, proto.getPartitionName().toLowerCase());
      pstmt.setInt(3, proto.getOrdinalPosition());
      pstmt.setString(4, proto.getPartitionValue());
      pstmt.setString(5, proto.getPath());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;
    CatalogProtos.PartitionDescProto proto = null;

    String sql = "SELECT " + C_TABLE_ID
        + ", partition_name, ordinal_position, partition_value, path, cache_nodes FROM "
        + TB_PARTTIONS + "where partition_name = ?";

    try {
      stmt = getConnection().prepareStatement(sql);
      stmt.setString(1, partitionName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.toString());
      }
      res = stmt.executeQuery();
      if (!res.next()) {
        throw new IOException("ERROR: there is no index matched to " + partitionName);
      }

      proto = resultToPartitionDescProto(res);
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return proto;
  }


  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws IOException {
    ResultSet res = null;
    PreparedStatement stmt = null;
    CatalogProtos.PartitionsProto proto = null;

    String partsSql = "SELECT " + C_TABLE_ID
        + ", partition_name, ordinal_position, partition_value, path, cache_nodes FROM "
        + TB_PARTTIONS + "where " + C_TABLE_ID + " = ?";

    try {
      // PARTITIONS
      stmt = getConnection().prepareStatement(partsSql);
      stmt.setString(1, tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.toString());
      }
      res = stmt.executeQuery();
      CatalogProtos.PartitionsProto.Builder builder = CatalogProtos.PartitionsProto.newBuilder();
      while(res.next()) {
        builder.addPartition(resultToPartitionDescProto(res));
      }
      proto = builder.build();
    } catch (SQLException se) {
      throw new IOException(se.getMessage(), se);
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return proto;
  }


  @Override
  public void delPartition(String partitionName) throws IOException {
    String sql =
        "DELETE FROM " + TB_PARTTIONS
            + " WHERE partition_name ='" + partitionName + "'";
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
  public void delPartitions(String tableName) throws IOException {
    String sql =
        "DELETE FROM " + TB_PARTTIONS
            + " WHERE " + C_TABLE_ID + " ='" + tableName + "'";
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
      proto = resultToIndexDescProto(res);
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
      proto = resultToIndexDescProto(res);
    } catch (SQLException se) {
      throw new IOException(se);
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
        protos.add(resultToIndexDescProto(res));
      }
    } catch (SQLException se) {
    } finally {
      CatalogUtil.closeSQLWrapper(res, stmt);
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  private IndexDescProto resultToIndexDescProto(final ResultSet res) throws SQLException {
    IndexDescProto.Builder builder = IndexDescProto.newBuilder();
    builder.setName(res.getString("index_name"));
    builder.setTableId(res.getString(C_TABLE_ID));
    builder.setColumn(indexResultToColumnProto(res));
    builder.setIndexMethod(getIndexMethod(res.getString("index_type").trim()));
    builder.setIsUnique(res.getBoolean("is_unique"));
    builder.setIsClustered(res.getBoolean("is_clustered"));
    builder.setIsAscending(res.getBoolean("is_ascending"));
    return builder.build();
  }

  /**
   * INDEXS table doesn't store type_length, so we need another resultToColumnProto method
   */
  private ColumnProto indexResultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setColumnName(res.getString("column_name").trim());

    Type type = getDataType(res.getString("data_type").trim());
    builder.setDataType(CatalogUtil.newSimpleDataType(type));

    return builder.build();
  }

  private ColumnProto resultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setColumnName(res.getString("column_name").trim());

    Type type = getDataType(res.getString("data_type").trim());
    int typeLength = res.getInt("type_length");
    if(typeLength > 0 ) {
      builder.setDataType(CatalogUtil.newDataTypeWithLen(type, typeLength));
    } else {
      builder.setDataType(CatalogUtil.newSimpleDataType(type));
    }

    return builder.build();
  }

  private CatalogProtos.KeyValueSetProto resultToKeyValueSetProto(final ResultSet res) throws SQLException {
    CatalogProtos.KeyValueSetProto.Builder setBuilder = CatalogProtos.KeyValueSetProto.newBuilder();
    CatalogProtos.KeyValueProto.Builder builder = CatalogProtos.KeyValueProto.newBuilder();
    while (res.next()) {
      builder.setKey(res.getString("key_"));
      builder.setValue(res.getString("value_"));
      setBuilder.addKeyval(builder.build());
    }
    return setBuilder.build();
  }

  private ColumnProto resultToQualifiedColumnProto(String tableName, final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();

    String columnName = tableName + "."
        + res.getString("column_name").trim();
    builder.setColumnName(columnName);

    Type type = getDataType(res.getString("data_type").trim());
    int typeLength = res.getInt("type_length");
    if(typeLength > 0 ) {
      builder.setDataType(CatalogUtil.newDataTypeWithLen(type, typeLength));
    } else {
      builder.setDataType(CatalogUtil.newSimpleDataType(type));
    }

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

  private CatalogProtos.PartitionMethodProto resultToPartitionMethodProto(final String tableName, final ResultSet res)
      throws SQLException, InvalidProtocolBufferException {
    CatalogProtos.PartitionMethodProto.Builder partBuilder = CatalogProtos.PartitionMethodProto.newBuilder();
    partBuilder.setTableId(tableName);
    partBuilder.setPartitionType(CatalogProtos.PartitionType.valueOf(res.getString("partition_type")));
    partBuilder.setExpression(res.getString("expression"));
    partBuilder.setExpressionSchema(SchemaProto.parseFrom(res.getBytes("expression_schema")));
    return partBuilder.build();
  }

  private CatalogProtos.PartitionDescProto resultToPartitionDescProto(ResultSet res) throws SQLException {
    CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();
    builder.setTableId(res.getString(1));
    builder.setPartitionName(res.getString(2));
    builder.setOrdinalPosition(res.getInt(3));
    builder.setPartitionValue(res.getString(4));
    builder.setPath(res.getString(5));
    return builder.build();
  }

  @Override
  public void close() {
    CatalogUtil.closeSQLWrapper(conn);
    LOG.info("Shutdown database (" + catalogUri + ")");
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
