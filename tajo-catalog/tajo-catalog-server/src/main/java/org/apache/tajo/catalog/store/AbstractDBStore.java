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
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  protected abstract boolean isInitialized() throws CatalogException;

  protected abstract void createBaseTable() throws CatalogException;

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
      throw new CatalogException("Cannot load Catalog driver " + catalogDriver, e);
    }

    try {
      LOG.info("Trying to connect database (" + catalogUri + ")");
      conn = createConnection(conf);
      LOG.info("Connected to database (" + catalogUri + ")");
    } catch (SQLException e) {
      throw new CatalogException("Cannot connect to database (" + catalogUri
          + ")", e);
    }

    try {
      if (!isInitialized()) {
        LOG.info("The base tables of CatalogServer are created.");
        createBaseTable();
      } else {
        LOG.info("The base tables of CatalogServer already is initialized.");
      }
    } catch (Exception se) {
      throw new CatalogException(
          "Cannot initialize the persistent storage of Catalog", se);
    }

//    int dbVersion = 0;
    try {
//      dbVersion = needUpgrade();
      needUpgrade();
    } catch (Exception e) {
      throw new CatalogException(
          "Cannot check if the DB need to be upgraded.", e);
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

  public Connection getConnection() {
    try {
      boolean isValid = conn.isValid(100);
      if (!isValid) {
        CatalogUtil.closeQuietly(conn);
        conn = createConnection(conf);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return conn;
  }

  private int needUpgrade() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    int retValue = -1;

    try {
      StringBuilder sql = new StringBuilder();
      sql.delete(0, sql.length());
      sql.append("SELECT VERSION FROM ");
      sql.append(TB_META);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      res = pstmt.executeQuery();

      if (!res.next()) { // if this db version is 0
        insertVersion();
        retValue = 0;
      } else {
        retValue =  res.getInt(1);
      }
    } catch (SQLException e) {
      throw new CatalogException(e);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return retValue;
  }

  private void insertVersion() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT INTO ");
      sql.append(TB_META);
      sql.append(" values (0)");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.executeUpdate();
    } catch (SQLException e) {
      throw new CatalogException(e);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  private void upgrade(int from, int to) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      if (from == 0) {
        if (to == 1) {
          StringBuilder sql = new StringBuilder();
          sql.append(" DROP INDEX idx_options_key");

          if (LOG.isDebugEnabled()) {
            LOG.debug(sql.toString());
          }

          conn = getConnection();
          pstmt = conn.prepareStatement(sql.toString());
          pstmt.executeUpdate();
          pstmt.close();

          sql.delete(0, sql.length());
          sql.append(" CREATE INDEX idx_options_key on ").append(TB_OPTIONS);
          sql.append(" (").append(C_TABLE_ID).append(")");

          if (LOG.isDebugEnabled()) {
            LOG.debug(sql.toString());
          }

          pstmt = conn.prepareStatement(sql.toString());
          pstmt.executeUpdate();

          LOG.info("DB Upgraded from " + from + " to " + to);
        } else {
          LOG.info("DB Upgraded from " + from + " to " + to);
        }
      }
    } catch (SQLException e) {
      throw new CatalogException(e);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }


  @Override
  public void addTable(final CatalogProtos.TableDescProto table) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      String tableName = table.getId().toLowerCase();

      String sql = String.format("INSERT INTO %s (%s, path, store_type) VALUES(?, ?, ?) ", TB_TABLES, C_TABLE_ID);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      pstmt.setString(2, table.getPath());
      pstmt.setString(3, table.getMeta().getStoreType().name());
      pstmt.executeUpdate();
      pstmt.close();

      String tidSql = String.format("SELECT TID from %s WHERE %s = ?", TB_TABLES, C_TABLE_ID);
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new CatalogException("ERROR: there is no tid matched to " + table.getId());
      }

      int tid = res.getInt("TID");
      res.close();
      pstmt.close();

      String colSql = String.format("INSERT INTO %s (TID, %s, column_id, column_name, data_type, type_length)"
          + " VALUES(?, ?, ?, ?, ?, ?) ", TB_COLUMNS, C_TABLE_ID);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(colSql);
      for(int i = 0; i < table.getSchema().getFieldsCount(); i++) {
        ColumnProto col = table.getSchema().getFields(i);
        pstmt.setInt(1, tid);
        pstmt.setString(2, tableName);
        pstmt.setInt(3, i);
        pstmt.setString(4, CatalogUtil.extractSimpleName(col.getName()));
        pstmt.setString(5, col.getDataType().getType().name());
        pstmt.setInt(6, (col.getDataType().hasLength() ? col.getDataType().getLength() : 0));
        pstmt.addBatch();
        pstmt.clearParameters();
      }
      pstmt.executeBatch();
      pstmt.close();

      if(table.getMeta().hasParams()) {
        String optSql = String.format("INSERT INTO %s (%s, key_, value_) VALUES(?, ?, ?)", TB_OPTIONS, C_TABLE_ID);

        if (LOG.isDebugEnabled()) {
          LOG.debug(optSql);
        }

        pstmt = conn.prepareStatement(optSql);
        for (CatalogProtos.KeyValueProto entry : table.getMeta().getParams().getKeyvalList()) {
          pstmt.setString(1, tableName);
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          pstmt.addBatch();
          pstmt.clearParameters();
        }
        pstmt.executeBatch();
        pstmt.close();
      }

      if (table.hasStats()) {

        String statSql =
            String.format("INSERT INTO %s (%s, num_rows, num_bytes) VALUES(?, ?, ?)", TB_STATISTICS, C_TABLE_ID);

        if (LOG.isDebugEnabled()) {
          LOG.debug(statSql);
        }

        pstmt = conn.prepareStatement(statSql);
        pstmt.setString(1, tableName);
        pstmt.setLong(2, table.getStats().getNumRows());
        pstmt.setLong(3, table.getStats().getNumBytes());
        pstmt.executeUpdate();
        pstmt.close();
      }

      if(table.hasPartition()) {
        String partSql =
            String.format("INSERT INTO %s (%s, partition_type, expression, expression_schema) VALUES(?, ?, ?, ?)",
                TB_PARTITION_METHODS, C_TABLE_ID);

        if (LOG.isDebugEnabled()) {
          LOG.debug(partSql);
        }

        pstmt = conn.prepareStatement(partSql);
        pstmt.setString(1, tableName);
        pstmt.setString(2, table.getPartition().getPartitionType().name());
        pstmt.setString(3, table.getPartition().getExpression());
        pstmt.setBytes(4, table.getPartition().getExpressionSchema().toByteArray());
        pstmt.executeUpdate();
      }

      // If there is no error, commit the changes.
      conn.commit();
    } catch (SQLException se) {
      try {
        // If there is any error, rollback the changes.
        conn.rollback();
      } catch (SQLException se2) {
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
  }

  @Override
  public boolean existTable(final String name) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" SELECT ").append(C_TABLE_ID);
      sql.append(" FROM ").append(TB_TABLES);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      pstmt.setString(1, name);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  @Override
  public void deleteTable(final String name) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      StringBuilder sql = new StringBuilder();
      sql.append("DELETE FROM ").append(TB_COLUMNS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      pstmt.executeUpdate();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append("DELETE FROM ").append(TB_OPTIONS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      pstmt.executeUpdate();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append("DELETE FROM ").append(TB_STATISTICS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      pstmt.executeUpdate();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append("DELETE FROM ").append(TB_PARTTIONS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      pstmt.executeUpdate();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append("DELETE FROM ").append(TB_TABLES);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      pstmt.executeUpdate();

      // If there is no error, commit the changes.
      conn.commit();
    } catch (SQLException se) {
      try {
        // If there is any error, rollback the changes.
        conn.rollback();
      } catch (SQLException se2) {
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public CatalogProtos.TableDescProto getTable(final String name) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    CatalogProtos.TableDescProto.Builder tableBuilder = null;
    StoreType storeType = null;

    try {
      tableBuilder = CatalogProtos.TableDescProto.newBuilder();

      StringBuilder sql = new StringBuilder();
      sql.append(" SELECT ").append(C_TABLE_ID).append(", path, store_type");
      sql.append(" from ").append(TB_TABLES);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      res = pstmt.executeQuery();

      if (!res.next()) { // there is no table of the given name.
        return null;
      }

      tableBuilder.setId(res.getString(C_TABLE_ID).trim());
      tableBuilder.setPath(res.getString("path").trim());
      storeType = CatalogUtil.getStoreType(res.getString("store_type").trim());

      res.close();
      pstmt.close();

      CatalogProtos.SchemaProto.Builder schemaBuilder = CatalogProtos.SchemaProto.newBuilder();
      sql.delete(0, sql.length());
      sql.append(" SELECT column_name, data_type, type_length ");
      sql.append(" from ").append(TB_COLUMNS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");
      sql.append("ORDER by column_id asc");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      res = pstmt.executeQuery();

      while (res.next()) {
        schemaBuilder.addFields(resultToColumnProto(res));
      }

      tableBuilder.setSchema(CatalogUtil.getQualfiedSchema(name, schemaBuilder.build()));

      res.close();
      pstmt.close();

      CatalogProtos.TableProto.Builder metaBuilder = CatalogProtos.TableProto.newBuilder();
      metaBuilder.setStoreType(storeType);
      sql.delete(0, sql.length());
      sql.append(" SELECT key_, value_ ");
      sql.append(" from ").append(TB_OPTIONS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      res = pstmt.executeQuery();
      metaBuilder.setParams(resultToKeyValueSetProto(res));

      tableBuilder.setMeta(metaBuilder);

      res.close();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append(" SELECT num_rows, num_bytes ");
      sql.append(" from ").append(TB_STATISTICS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      res = pstmt.executeQuery();

      if (res.next()) {
        TableStatsProto.Builder statBuilder = TableStatsProto.newBuilder();
        statBuilder.setNumRows(res.getLong("num_rows"));
        statBuilder.setNumBytes(res.getLong("num_bytes"));
        tableBuilder.setStats(statBuilder);
      }

      res.close();
      pstmt.close();

      sql.delete(0, sql.length());
      sql.append(" SELECT partition_type, expression, expression_schema ");
      sql.append(" from ").append(TB_PARTITION_METHODS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, name);
      res = pstmt.executeQuery();

      if (res.next()) {
        tableBuilder.setPartition(resultToPartitionMethodProto(name, res));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return tableBuilder.build();
  }

  private ColumnProto getColumn(String tableName, int tid, String columnId) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" SELECT column_name, data_type, type_length");
      sql.append(" FROM ").append(TB_COLUMNS);
      sql.append(" WHERE TID = ? ");
      sql.append(" AND column_id = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setInt(1, tid);
      pstmt.setInt(2, Integer.parseInt(columnId));
      res = pstmt.executeQuery();

      if (res.next()) {
        return resultToColumnProto(res);
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
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
  public List<String> getAllTableNames() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    List<String> tables = new ArrayList<String>();

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT ");
      sql.append(C_TABLE_ID);
      sql.append(" from ");
      sql.append(TB_TABLES);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      res = pstmt.executeQuery();
      while (res.next()) {
        tables.add(res.getString(C_TABLE_ID).trim());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return tables;
  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT INTO ");
      sql.append(TB_PARTTIONS);
      sql.append(" (");
      sql.append(C_TABLE_ID);
      sql.append(",  partition_name, ordinal_position, path, cache_nodes) ");
      sql.append("VALUES (?, ?, ?, ?, ?, ?) ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      for (CatalogProtos.PartitionDescProto proto : partitionsProto.getPartitionList()) {
        pstmt.setString(1, proto.getTableId());
        pstmt.setString(2, proto.getPartitionName());
        pstmt.setInt(3, proto.getOrdinalPosition());
        pstmt.setString(4, proto.getPartitionValue());
        pstmt.setString(5, proto.getPath());
        pstmt.addBatch();
        pstmt.clearParameters();
      }
      pstmt.executeBatch();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT INTO ");
      sql.append(TB_PARTITION_METHODS);
      sql.append( " (");
      sql.append(C_TABLE_ID);
      sql.append(", partition_type,  expression, expression_schema) ");
      sql.append("VALUES (?, ?, ?, ?) ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, proto.getTableId().toLowerCase());
      pstmt.setString(2, proto.getPartitionType().name());
      pstmt.setString(3, proto.getExpression());
      pstmt.setBytes(4, proto.getExpressionSchema().toByteArray());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void delPartitionMethod(String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" DELETE FROM ").append(TB_PARTITION_METHODS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" SELECT partition_type, expression, expression_schema");
      sql.append(" FROM ").append(TB_PARTITION_METHODS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();

      if (res.next()) {
        return resultToPartitionMethodProto(tableName, res);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return null;
  }

  @Override
  public boolean existPartitionMethod(String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" SELECT partition_type, expression, expression_schema");
      sql.append(" FROM ").append(TB_PARTITION_METHODS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();

      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {                           
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return exist;
  }

  @Override
  public void addPartition(CatalogProtos.PartitionDescProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT INTO ");
      sql.append(TB_PARTTIONS);
      sql.append(" (");
      sql.append(C_TABLE_ID);
      sql.append(", partition_method_id,  partition_name, ordinal_position, path, cache_nodes) ");
      sql.append("VALUES (?, ?, ?, ?, ?, ?, ?) ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, proto.getTableId().toLowerCase());
      pstmt.setString(2, proto.getPartitionName().toLowerCase());
      pstmt.setInt(3, proto.getOrdinalPosition());
      pstmt.setString(4, proto.getPartitionValue());
      pstmt.setString(5, proto.getPath());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    CatalogProtos.PartitionDescProto proto = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT ");
      sql.append(C_TABLE_ID);
      sql.append(", partition_name, ordinal_position, partition_value, path, cache_nodes FROM ");
      sql.append(TB_PARTTIONS);
      sql.append("where partition_name = ?");


      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, partitionName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + partitionName);
      }

      proto = resultToPartitionDescProto(res);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }


  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    CatalogProtos.PartitionsProto proto = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT ");
      sql.append(C_TABLE_ID);
      sql.append(", partition_name, ordinal_position, partition_value, path, cache_nodes FROM ");
      sql.append(TB_PARTTIONS);
      sql.append("where ");
      sql.append(C_TABLE_ID);
      sql.append(" = ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      // PARTITIONS
      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();
      CatalogProtos.PartitionsProto.Builder builder = CatalogProtos.PartitionsProto.newBuilder();
      while(res.next()) {
        builder.addPartition(resultToPartitionDescProto(res));
      }
      proto = builder.build();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }


  @Override
  public void delPartition(String partitionName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" DELETE FROM ").append(TB_PARTTIONS);
      sql.append(" WHERE partition_name = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, partitionName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void delPartitions(String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" DELETE FROM ").append(TB_PARTTIONS);
      sql.append(" WHERE ").append(C_TABLE_ID).append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }


  @Override
  public void addIndex(final IndexDescProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("INSERT INTO indexes (index_name, ");
      sql.append(C_TABLE_ID);
      sql.append(", column_name, ");
      sql.append("data_type, index_type, is_unique, is_clustered, is_ascending) VALUES ");
      sql.append("(?,?,?,?,?,?,?,?)");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      pstmt.setString(1, proto.getName());
      pstmt.setString(2, proto.getTableId());
      pstmt.setString(3, CatalogUtil.extractSimpleName(proto.getColumn().getName()));
      pstmt.setString(4, proto.getColumn().getDataType().getType().name());
      pstmt.setString(5, proto.getIndexMethod().toString());
      pstmt.setBoolean(6, proto.hasIsUnique() && proto.getIsUnique());
      pstmt.setBoolean(7, proto.hasIsClustered() && proto.getIsClustered());
      pstmt.setBoolean(8, proto.hasIsAscending() && proto.getIsAscending());
      pstmt.executeUpdate();

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void delIndex(final String indexName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append(" DELETE FROM ").append(TB_INDEXES);
      sql.append(" WHERE index_name ").append(" = ? ");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, indexName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public IndexDescProto getIndex(final String indexName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append( "SELECT index_name, ");
      sql.append(C_TABLE_ID);
      sql.append(", column_name, data_type, ");
      sql.append("index_type, is_unique, is_clustered, is_ascending FROM indexes ");
      sql.append("where index_name = ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, indexName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + indexName);
      }
      proto = resultToIndexDescProto(res);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }

  @Override
  public IndexDescProto getIndex(final String tableName,
                                 final String columnName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT index_name, ");
      sql.append(C_TABLE_ID);
      sql.append(", column_name, data_type, ");
      sql.append("index_type, is_unique, is_clustered, is_ascending FROM indexes ");
      sql.append("where ");
      sql.append(C_TABLE_ID);
      sql.append(" = ? AND column_name = ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      pstmt.setString(2, columnName);

      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to "
            + tableName + "." + columnName);
      }
      proto = resultToIndexDescProto(res);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }

  @Override
  public boolean existIndex(final String indexName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT index_name from ");
      sql.append(TB_INDEXES);
      sql.append(" WHERE index_name = ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, indexName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {

    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  @Override
  public boolean existIndex(String tableName, String columnName)
      throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    boolean exist = false;
    ResultSet res = null;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT index_name from ");
      sql.append(TB_INDEXES);
      sql.append(" WHERE ");
      sql.append(C_TABLE_ID);
      sql.append(" = ? AND COLUMN_NAME = ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      pstmt.setString(2, columnName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  @Override
  public IndexDescProto[] getIndexes(final String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    List<IndexDescProto> protos = null;

    try {
      protos = new ArrayList<IndexDescProto>();

      StringBuilder sql = new StringBuilder();
      sql.append("SELECT index_name, ");
      sql.append(C_TABLE_ID);
      sql.append(", column_name, data_type, ");
      sql.append("index_type, is_unique, is_clustered, is_ascending FROM indexes ");
      sql.append("where ");
      sql.append(C_TABLE_ID);
      sql.append("= ?");

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();
      while (res.next()) {
        protos.add(resultToIndexDescProto(res));
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
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
    builder.setName(res.getString("column_name").trim());

    Type type = getDataType(res.getString("data_type").trim());
    builder.setDataType(CatalogUtil.newSimpleDataType(type));

    return builder.build();
  }

  private ColumnProto resultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setName(res.getString("column_name").trim());

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
    CatalogUtil.closeQuietly(conn);
    LOG.info("Shutdown database (" + catalogUri + ")");
  }

  @Override
  public final void addFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void existFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final List<String> getAllFunctionNames() throws CatalogException {
    // TODO - not implemented yet
    return null;
  }
}
