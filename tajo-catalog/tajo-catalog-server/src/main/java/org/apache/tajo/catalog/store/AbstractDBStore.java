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
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public abstract class AbstractDBStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());
  protected final Configuration conf;
  protected final String connectionId;
  protected final String connectionPassword;
  protected final String catalogUri;

  private Connection conn;
  
  protected Map<String, Boolean> baseTableMaps = new HashMap<String, Boolean>();
  
  protected XMLCatalogSchemaManager catalogSchemaManager;

  protected abstract String getCatalogDriverName();
  
  protected String getCatalogSchemaPath() {
    return "";
  }

  protected abstract Connection createConnection(final Configuration conf) throws SQLException;
  
  protected void createDatabaseDependants() throws CatalogException {
    
  }
  
  protected boolean isInitialized() throws CatalogException {
    return catalogSchemaManager.isInitialized(getConnection());
  }

  protected void createBaseTable() throws CatalogException {
    createDatabaseDependants();
    
    catalogSchemaManager.createBaseSchema(getConnection());
    
    insertSchemaVersion();
  }

  protected void dropBaseTable() throws CatalogException {
    catalogSchemaManager.dropBaseSchema(getConnection());
  }

  public AbstractDBStore(Configuration conf) throws InternalException {
    this.conf = conf;

    if (conf.get(CatalogConstants.DEPRECATED_CATALOG_URI) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CATALOG_URI + " " +
          "is deprecated. Use " + CatalogConstants.CATALOG_URI + " instead.");
      this.catalogUri = conf.get(CatalogConstants.DEPRECATED_CATALOG_URI);
    } else {
      this.catalogUri = conf.get(CatalogConstants.CATALOG_URI);
    }

    if (conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CONNECTION_ID + " " +
          "is deprecated. Use " + CatalogConstants.CONNECTION_ID + " instead.");
      this.connectionId = conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID);
    } else {
      this.connectionId = conf.get(CatalogConstants.CONNECTION_ID);
    }

    if (conf.get(CatalogConstants.DEPRECATED_CONNECTION_PASSWORD) != null) {
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

    String schemaPath = getCatalogSchemaPath();
    if (schemaPath != null && !schemaPath.isEmpty()) {
      this.catalogSchemaManager = new XMLCatalogSchemaManager(schemaPath);
    }
    
    try {
      if (isInitialized()) {
        LOG.info("The base tables of CatalogServer already is initialized.");
        verifySchemaVersion();
      } else {
        try {
          createBaseTable();
          LOG.info("The base tables of CatalogServer are created.");
        } catch (CatalogException ce) {
          try {
            dropBaseTable();
          } catch (Throwable t) {
            LOG.error(t, t);
          }
          throw ce;
        }
      }
    } catch (Exception se) {
      throw new CatalogException("Cannot initialize the persistent storage of Catalog", se);
    }
  }

  public int getDriverVersion() {
    return catalogSchemaManager.getCatalogStore().getSchema().getVersion();
  }

  public String readSchemaFile(String path) throws CatalogException {
    try {
      return FileUtil.readTextFileFromResource("schemas/" + path);
    } catch (IOException e) {
      throw new CatalogException(e);
    }
  }

  protected String getCatalogUri() {
    return catalogUri;
  }

  protected boolean isConnValid(int timeout) throws CatalogException {
    boolean isValid = false;

    try {
      isValid = conn.isValid(timeout);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return isValid;
  }

  public Connection getConnection() {
    try {
      boolean isValid = isConnValid(100);
      if (!isValid) {
        CatalogUtil.closeQuietly(conn);
        conn = createConnection(conf);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return conn;
  }

  private int getSchemaVersion() {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet result = null;
    int schemaVersion = -1;
    
    String sql = "SELECT version FROM META";
    if (LOG.isDebugEnabled()) {
      LOG.debug(sql.toString());
    }

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      result = pstmt.executeQuery();

      if (result.next()) {
        schemaVersion = result.getInt("VERSION");
      }
    } catch (SQLException e) {
      throw new CatalogException(e.getMessage(), e);
    } finally {
      CatalogUtil.closeQuietly(pstmt, result);
    }
    
    return schemaVersion;
  }

  private void verifySchemaVersion() throws CatalogException {
    int schemaVersion = -1;

    schemaVersion = getSchemaVersion();

    if (schemaVersion == -1 || schemaVersion != getDriverVersion()) {
      LOG.error(String.format("Catalog version (%d) and current driver version (%d) are mismatch to each other",
          schemaVersion, getDriverVersion()));
      LOG.error("=========================================================================");
      LOG.error("| Catalog Store Migration Is Needed |");
      LOG.error("=========================================================================");
      LOG.error("| You might downgrade or upgrade Apache Tajo. Downgrading or upgrading |");
      LOG.error("| Tajo without migration process is only available in some versions. |");
      LOG.error("| In order to learn how to migration Apache Tajo instance, |");
      LOG.error("| please refer http://s.apache.org/0_8_migration. |");
      LOG.error("=========================================================================");
      throw new CatalogException("Migration Needed. Please refer http://s.apache.org/0_8_migration.");
    }

    LOG.info(String.format("The compatibility of the catalog schema (version: %d) has been verified.",
        getDriverVersion()));
  }

  /**
   * Insert the version of the current catalog schema
   */
  protected void insertSchemaVersion() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement("INSERT INTO META VALUES (?)");
      pstmt.setInt(1, getDriverVersion());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException("cannot insert catalog schema version", se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void createTablespace(String spaceName, String spaceUri) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      String sql = String.format("INSERT INTO %s (SPACE_NAME, SPACE_URI) VALUES (?, ?)", TB_SPACES);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, spaceName);
      pstmt.setString(2, spaceUri);
      pstmt.executeUpdate();
      pstmt.close();
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existTablespace(String tableSpaceName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT SPACE_NAME FROM " + TB_SPACES + " WHERE SPACE_NAME = ?");
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, tableSpaceName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public void dropTablespace(String tableSpaceName) throws CatalogException {


    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      TableSpaceInternal tableSpace = getTableSpaceInfo(tableSpaceName);
      Collection<String> databaseNames = getAllDatabaseNamesInternal(COL_TABLESPACE_PK + " = " + tableSpace.spaceId);

      conn = getConnection();
      conn.setAutoCommit(false);

      for (String databaseName : databaseNames) {
        dropDatabase(databaseName);
      }

      String sql = "DELETE FROM " + TB_SPACES + " WHERE " + COL_TABLESPACE_PK + "= ?";
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableSpace.getSpaceId());
      pstmt.executeUpdate();
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(String.format("Failed to drop tablespace \"%s\"", tableSpaceName), se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public Collection<String> getAllTablespaceNames() throws CatalogException {
    return getAllTablespaceNamesInternal(null);
  }

  private Collection<String> getAllTablespaceNamesInternal(@Nullable String whereCondition) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    List<String> tablespaceNames = new ArrayList<String>();

    try {
      String sql = "SELECT SPACE_NAME FROM " + TB_SPACES;

      if (whereCondition != null) {
        sql += " WHERE " + whereCondition;
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      resultSet = pstmt.executeQuery();
      while (resultSet.next()) {
        tablespaceNames.add(resultSet.getString(1));
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }

    return tablespaceNames;
  }
  
  @Override
  public List<TablespaceProto> getTablespaces() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    List<TablespaceProto> tablespaces = TUtil.newList();

    try {
      String sql = "SELECT SPACE_ID, SPACE_NAME, SPACE_HANDLER, SPACE_URI FROM " + TB_SPACES ;
      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);

      while (resultSet.next()) {
        TablespaceProto.Builder builder = TablespaceProto.newBuilder();
        builder.setId(resultSet.getInt("SPACE_ID"));
        builder.setSpaceName(resultSet.getString("SPACE_NAME"));
        builder.setHandler(resultSet.getString("SPACE_HANDLER"));
        builder.setUri(resultSet.getString("SPACE_URI"));
        
        tablespaces.add(builder.build());
      }
      return tablespaces;

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
  }

  @Override
  public TablespaceProto getTablespace(String spaceName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    try {
      String sql = "SELECT SPACE_NAME, SPACE_URI FROM " + TB_SPACES + " WHERE SPACE_NAME=?";
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, spaceName);
      resultSet = pstmt.executeQuery();

      if (!resultSet.next()) {
        throw new NoSuchTablespaceException(spaceName);
      }

      String retrieveSpaceName = resultSet.getString("SPACE_NAME");
      String uri = resultSet.getString("SPACE_URI");

      TablespaceProto.Builder builder = TablespaceProto.newBuilder();
      builder.setSpaceName(retrieveSpaceName);
      builder.setUri(uri);
      return builder.build();

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }
  }

  @Override
  public void alterTablespace(AlterTablespaceProto alterProto) throws CatalogException {
    Connection conn;
    PreparedStatement pstmt = null;

    if (alterProto.getCommandList().size() == 1) {
      AlterTablespaceCommand command = alterProto.getCommand(0);
      if (command.getType() == AlterTablespaceProto.AlterTablespaceType.LOCATION) {
        AlterTablespaceProto.SetLocation setLocation = command.getLocation();
        try {
          String sql = "UPDATE " + TB_SPACES + " SET SPACE_URI=? WHERE SPACE_NAME=?";

          conn = getConnection();
          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, setLocation.getUri());
          pstmt.setString(2, alterProto.getSpaceName());
          pstmt.executeUpdate();
        } catch (SQLException se) {
          throw new CatalogException(se);
        } finally {
          CatalogUtil.closeQuietly(pstmt);
        }
      }
    }
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      TableSpaceInternal spaceInfo = getTableSpaceInfo(tablespaceName);

      String sql = "INSERT INTO " + TB_DATABASES + " (DB_NAME, SPACE_ID) VALUES (?, ?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      conn.setAutoCommit(false);
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      pstmt.setInt(2, spaceInfo.getSpaceId());
      pstmt.executeUpdate();
      pstmt.close();
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existDatabase(String databaseName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT DB_NAME FROM " + TB_DATABASES + " WHERE DB_NAME = ?");
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, databaseName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public void dropDatabase(String databaseName) throws CatalogException {
    Collection<String> tableNames = getAllTableNames(databaseName);

    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      for (String tableName : tableNames) {
        dropTableInternal(conn, databaseName, tableName);
      }

      String sql = "DELETE FROM " + TB_DATABASES + " WHERE DB_NAME = ?";
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      pstmt.executeUpdate();
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(String.format("Failed to drop database \"%s\"", databaseName), se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    return getAllDatabaseNamesInternal(null);
  }

  private Collection<String> getAllDatabaseNamesInternal(@Nullable String whereCondition) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    List<String> databaseNames = new ArrayList<String>();

    try {
      String sql = "SELECT DB_NAME FROM " + TB_DATABASES;

      if (whereCondition != null) {
        sql += " WHERE " + whereCondition;
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      resultSet = pstmt.executeQuery();
      while (resultSet.next()) {
        databaseNames.add(resultSet.getString(1));
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }

    return databaseNames;
  }
  
  @Override
  public List<DatabaseProto> getAllDatabases() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<DatabaseProto> databases = new ArrayList<DatabaseProto>();

    try {
      String sql = "SELECT DB_ID, DB_NAME, SPACE_ID FROM " + TB_DATABASES;

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        DatabaseProto.Builder builder = DatabaseProto.newBuilder();
        
        builder.setId(resultSet.getInt("DB_ID"));
        builder.setName(resultSet.getString("DB_NAME"));
        builder.setSpaceId(resultSet.getInt("SPACE_ID"));
        
        databases.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return databases;
  }

  private static class TableSpaceInternal {
    private final int spaceId;
    private final String spaceURI;
    private final String handler;

    TableSpaceInternal(int spaceId, String spaceURI, String handler) {
      this.spaceId = spaceId;
      this.spaceURI = spaceURI;
      this.handler = handler;
    }

    public int getSpaceId() {
      return spaceId;
    }

    public String getSpaceURI() {
      return spaceURI;
    }

    public String getHandler() {
      return handler;
    }
  }

  private TableSpaceInternal getTableSpaceInfo(String spaceName) {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      String sql = "SELECT SPACE_ID, SPACE_URI, SPACE_HANDLER from " + TB_SPACES + " WHERE SPACE_NAME = ?";
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, spaceName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no SPACE_ID matched to the space name \"" + spaceName + "\"");
      }
      return new TableSpaceInternal(res.getInt(1), res.getString(2), res.getString(3));
    } catch (SQLException se) {
      throw new NoSuchTablespaceException(spaceName);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  private int getTableId(int databaseId, String databaseName, String tableName) {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      String tidSql = "SELECT TID from TABLES WHERE db_id = ? AND " + COL_TABLES_NAME + "=?";
      conn = getConnection();
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no tid matched to " + tableName);
      }
      return res.getInt(1);
    } catch (SQLException se) {
      throw new NoSuchTableException(databaseName, tableName);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  enum TableType {
    BASE_TABLE,
    EXTERNAL_TABLE
  }

  @Override
  public void createTable(final CatalogProtos.TableDescProto table) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String[] splitted = CatalogUtil.splitTableName(table.getTableName());
      if (splitted.length == 1) {
        throw new IllegalArgumentException("createTable() requires a qualified table name, but it is \""
            + table.getTableName() + "\".");
      }
      String databaseName = splitted[0];
      String tableName = splitted[1];

      int dbid = getDatabaseId(databaseName);

      if (table.getIsExternal()) {
        String sql = "INSERT INTO TABLES (DB_ID, " + COL_TABLES_NAME + ", TABLE_TYPE, PATH, STORE_TYPE) VALUES(?, ?, ?, ?, ?) ";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, dbid);
        pstmt.setString(2, tableName);
        pstmt.setString(3, TableType.EXTERNAL_TABLE.name());
        pstmt.setString(4, table.getPath());
        pstmt.setString(5, CatalogUtil.getStoreTypeString(table.getMeta().getStoreType()));
        pstmt.executeUpdate();
        pstmt.close();
      } else {
        String sql = "INSERT INTO TABLES (DB_ID, " + COL_TABLES_NAME + ", TABLE_TYPE, STORE_TYPE) VALUES(?, ?, ?, ?) ";

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, dbid);
        pstmt.setString(2, tableName);
        pstmt.setString(3, TableType.BASE_TABLE.name());
        pstmt.setString(4, CatalogUtil.getStoreTypeString(table.getMeta().getStoreType()));
        pstmt.executeUpdate();
        pstmt.close();
      }

      String tidSql =
          "SELECT TID from " + TB_TABLES + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_NAME + "=?";
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new CatalogException("ERROR: there is no TID matched to " + table.getTableName());
      }

      int tableId = res.getInt("TID");
      res.close();
      pstmt.close();

      String colSql =
          "INSERT INTO " + TB_COLUMNS +
              // 1    2            3                 4                 5          6
              " (TID, COLUMN_NAME, ORDINAL_POSITION, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH)" +
              " VALUES(?, ?, ?, ?, ?, ?) ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(colSql);
      }

      pstmt = conn.prepareStatement(colSql);
      for (int i = 0; i < table.getSchema().getFieldsCount(); i++) {
        ColumnProto col = table.getSchema().getFields(i);
        TajoDataTypes.DataType dataType = col.getDataType();

        pstmt.setInt(1, tableId);
        pstmt.setString(2, CatalogUtil.extractSimpleName(col.getName()));
        pstmt.setInt(3, i);
        // the default number of nested fields is 0.
        pstmt.setInt(4, dataType.hasNumNestedFields() ? dataType.getNumNestedFields() : 0);
        pstmt.setString(5, dataType.getType().name());
        pstmt.setInt(6, (col.getDataType().hasLength() ? col.getDataType().getLength() : 0));
        pstmt.addBatch();
        pstmt.clearParameters();
      }
      pstmt.executeBatch();
      pstmt.close();

      if (table.getMeta().hasParams()) {
        String propSQL = "INSERT INTO " + TB_OPTIONS + "(TID, KEY_, VALUE_) VALUES(?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(propSQL);
        }

        pstmt = conn.prepareStatement(propSQL);
        for (KeyValueProto entry : table.getMeta().getParams().getKeyvalList()) {
          pstmt.setInt(1, tableId);
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          pstmt.addBatch();
          pstmt.clearParameters();
        }
        pstmt.executeBatch();
        pstmt.close();
      }

      if (table.hasStats()) {

        String statSql = "INSERT INTO " + TB_STATISTICS + " (TID, NUM_ROWS, NUM_BYTES) VALUES(?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(statSql);
        }

        pstmt = conn.prepareStatement(statSql);
        pstmt.setInt(1, tableId);
        pstmt.setLong(2, table.getStats().getNumRows());
        pstmt.setLong(3, table.getStats().getNumBytes());
        pstmt.executeUpdate();
        pstmt.close();
      }

      if (table.hasPartition()) {
        String partSql =
            "INSERT INTO PARTITION_METHODS (TID, PARTITION_TYPE, EXPRESSION, EXPRESSION_SCHEMA) VALUES(?, ?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(partSql);
        }

        pstmt = conn.prepareStatement(partSql);
        pstmt.setInt(1, tableId);
        pstmt.setString(2, table.getPartition().getPartitionType().name());
        pstmt.setString(3, table.getPartition().getExpression());
        pstmt.setBytes(4, table.getPartition().getExpressionSchema().toByteArray());
        pstmt.executeUpdate();
      }

      // If there is no error, commit the changes.
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public void updateTableStats(final CatalogProtos.UpdateTableStatsProto statsProto) throws
    CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String[] splitted = CatalogUtil.splitTableName(statsProto.getTableName());
      if (splitted.length == 1) {
        throw new IllegalArgumentException("updateTableStats() requires a qualified table name, but it is \""
          + statsProto.getTableName() + "\".");
      }
      String databaseName = splitted[0];
      String tableName = splitted[1];

      int dbid = getDatabaseId(databaseName);

      String tidSql =
        "SELECT TID from " + TB_TABLES + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_NAME + "=?";
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new CatalogException("ERROR: there is no TID matched to " + statsProto.getTableName());
      }

      int tableId = res.getInt("TID");
      res.close();
      pstmt.close();

      if (statsProto.hasStats()) {

        String statSql = "UPDATE " + TB_STATISTICS + " SET NUM_ROWS = ?, " +
          "NUM_BYTES = ? WHERE TID = ?";

        if (LOG.isDebugEnabled()) {
          LOG.debug(statSql);
        }

        pstmt = conn.prepareStatement(statSql);
        pstmt.setInt(1, tableId);
        pstmt.setLong(2, statsProto.getStats().getNumRows());
        pstmt.setLong(3, statsProto.getStats().getNumBytes());
        pstmt.executeUpdate();
      }

      // If there is no error, commit the changes.
      conn.commit();
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto) throws CatalogException {

    String[] splitted = CatalogUtil.splitTableName(alterTableDescProto.getTableName());
    if (splitted.length == 1) {
      throw new IllegalArgumentException("alterTable() requires a qualified table name, but it is \""
          + alterTableDescProto.getTableName() + "\".");
    }
    String databaseName = splitted[0];
    String tableName = splitted[1];
    String partitionName = null;
    CatalogProtos.PartitionDescProto partitionDesc = null;
    try {

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      switch (alterTableDescProto.getAlterTableType()) {
        case RENAME_TABLE:
          if (existTable(databaseName,alterTableDescProto.getNewTableName())) {
            throw new AlreadyExistsTableException(alterTableDescProto.getNewTableName());
          }
          renameTable(tableId, alterTableDescProto.getNewTableName());
          break;
        case RENAME_COLUMN:
          if (existColumn(tableId, alterTableDescProto.getAlterColumnName().getNewColumnName())) {
            throw new ColumnNameAlreadyExistException(alterTableDescProto.getAlterColumnName().getNewColumnName());
          }
          renameColumn(tableId, alterTableDescProto.getAlterColumnName());
          break;
        case ADD_COLUMN:
          if (existColumn(tableId, alterTableDescProto.getAddColumn().getName())) {
            throw new ColumnNameAlreadyExistException(alterTableDescProto.getAddColumn().getName());
          }
          addNewColumn(tableId, alterTableDescProto.getAddColumn());
          break;
        case ADD_PARTITION:
          partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
          partitionDesc = getPartition(databaseName, tableName, partitionName);
          if(partitionDesc != null) {
            throw new AlreadyExistsPartitionException(databaseName, tableName, partitionName);
          }
          addPartition(tableId, alterTableDescProto.getPartitionDesc());
          break;
        case DROP_PARTITION:
          partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
          partitionDesc = getPartition(databaseName, tableName, partitionName);
          if(partitionDesc == null) {
            throw new NoSuchPartitionException(databaseName, tableName, partitionName);
          }
          dropPartition(tableId, alterTableDescProto.getPartitionDesc().getPartitionName());
          break;
        default:
      }
    } catch (SQLException sqlException) {
      throw new CatalogException(sqlException);
    }

  }

  private void renameTable(final int tableId, final String tableName) throws CatalogException {

    final String updtaeRenameTableSql = "UPDATE " + TB_TABLES + " SET " + COL_TABLES_NAME + " = ? " + " WHERE TID = ?";

    if (LOG.isDebugEnabled()) {
      LOG.debug(updtaeRenameTableSql);
    }

    Connection conn;
    PreparedStatement pstmt = null;

    try {

      conn = getConnection();
      pstmt = conn.prepareStatement(updtaeRenameTableSql);
      pstmt.setString(1, tableName);
      pstmt.setInt(2, tableId);
      pstmt.executeUpdate();

    } catch (SQLException sqlException) {
      throw new CatalogException(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  private void renameColumn(final int tableId, final CatalogProtos.AlterColumnProto alterColumnProto)
      throws CatalogException {

    final String selectColumnSql =
        "SELECT COLUMN_NAME, DATA_TYPE, TYPE_LENGTH, ORDINAL_POSITION, NESTED_FIELD_NUM from " + TB_COLUMNS +
            " WHERE " + COL_TABLES_PK + " = ?" + " AND COLUMN_NAME = ?" ;
    final String deleteColumnNameSql =
        "DELETE FROM " + TB_COLUMNS + " WHERE TID = ? AND COLUMN_NAME = ?";
    final String insertNewColumnSql =
        "INSERT INTO " + TB_COLUMNS +
            " (TID, COLUMN_NAME, ORDINAL_POSITION, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH) VALUES(?, ?, ?, ?, ?, ?) ";

    if (LOG.isDebugEnabled()) {
      LOG.debug(selectColumnSql);
      LOG.debug(deleteColumnNameSql);
      LOG.debug(insertNewColumnSql);
    }

    Connection conn;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    try {

      conn = getConnection();
      conn.setAutoCommit(false);

      //SELECT COLUMN
      pstmt = conn.prepareStatement(selectColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, alterColumnProto.getOldColumnName());
      resultSet = pstmt.executeQuery();

      CatalogProtos.ColumnProto columnProto = null;
      int ordinalPosition = 0;
      int nestedFieldNum = 0;

      if (resultSet.next()) {
        columnProto = resultToColumnProto(resultSet);
        //NOTE ==> Setting new column Name
        columnProto = columnProto.toBuilder().setName(alterColumnProto.getNewColumnName()).build();
        ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
        nestedFieldNum = resultSet.getInt("NESTED_FIELD_NUM");
      } else {
        throw new NoSuchColumnException(alterColumnProto.getOldColumnName());
      }

      resultSet.close();
      pstmt.close();
      resultSet = null;

      //DELETE COLUMN
      pstmt = conn.prepareStatement(deleteColumnNameSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, alterColumnProto.getOldColumnName());
      pstmt.executeUpdate();
      pstmt.close();

      //INSERT COLUMN
      pstmt = conn.prepareStatement(insertNewColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, CatalogUtil.extractSimpleName(columnProto.getName()));
      pstmt.setInt(3, ordinalPosition);
      pstmt.setInt(4, nestedFieldNum);
      pstmt.setString(5, columnProto.getDataType().getType().name());
      pstmt.setInt(6, (columnProto.getDataType().hasLength() ? columnProto.getDataType().getLength() : 0));
      pstmt.executeUpdate();

      conn.commit();


    } catch (SQLException sqlException) {
      throw new CatalogException(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt,resultSet);
    }
  }

  private void addNewColumn(int tableId, CatalogProtos.ColumnProto columnProto) throws CatalogException {

    final String insertNewColumnSql =
        "INSERT INTO " + TB_COLUMNS +
            " (TID, COLUMN_NAME, ORDINAL_POSITION, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH) VALUES(?, ?, ?, ?, ?, ?) ";
    final String columnCountSql =
        "SELECT MAX(ORDINAL_POSITION) AS POSITION FROM " + TB_COLUMNS + " WHERE TID = ?";

    if (LOG.isDebugEnabled()) {
      LOG.debug(insertNewColumnSql);
      LOG.debug(columnCountSql);
    }

    Connection conn;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(columnCountSql);
      pstmt.setInt(1 , tableId);
      resultSet =  pstmt.executeQuery();

      // get the last the ordinal position.
      int position = resultSet.next() ? resultSet.getInt("POSITION") : 0;

      resultSet.close();
      pstmt.close();
      resultSet = null;

      TajoDataTypes.DataType dataType = columnProto.getDataType();

      pstmt = conn.prepareStatement(insertNewColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, CatalogUtil.extractSimpleName(columnProto.getName()));
      pstmt.setInt(3, position + 1);
      pstmt.setInt(4, dataType.hasNumNestedFields() ? dataType.getNumNestedFields() : 0);
      pstmt.setString(5, dataType.getType().name());
      pstmt.setInt(6, (columnProto.getDataType().hasLength() ? columnProto.getDataType().getLength() : 0));
      pstmt.executeUpdate();

    } catch (SQLException sqlException) {
      throw new CatalogException(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt,resultSet);
    }
  }

  public void addPartition(int tableId, CatalogProtos.PartitionDescProto partition) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    final String ADD_PARTITION_SQL =
      "INSERT INTO " + TB_PARTTIONS
        + " (" + COL_TABLES_PK + ", PARTITION_NAME, PATH) VALUES (?,?,?)";

    final String ADD_PARTITION_KEYS_SQL =
      "INSERT INTO " + TB_PARTTION_KEYS + " (" + COL_PARTITIONS_PK + ", " + COL_COLUMN_NAME + ", "
      + COL_PARTITION_VALUE + ") VALUES (?,?,?)";

    try {

      if (LOG.isDebugEnabled()) {
        LOG.debug(ADD_PARTITION_SQL);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(ADD_PARTITION_SQL);

      pstmt.setInt(1, tableId);
      pstmt.setString(2, partition.getPartitionName());
      pstmt.setString(3, partition.getPath());
      pstmt.executeUpdate();

      if (partition.getPartitionKeysCount() > 0) {
        pstmt = conn.prepareStatement(ADD_PARTITION_KEYS_SQL);
        int partitionId = getPartitionId(tableId, partition.getPartitionName());
        addPartitionKeys(pstmt, partitionId, partition);
        pstmt.executeBatch();
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  public int getPartitionId(int tableId, String partitionName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    int retValue = -1;

    try {
      String sql = "SELECT " + COL_PARTITIONS_PK + " FROM " + TB_PARTTIONS +
        " WHERE " + COL_TABLES_PK + " = ? AND PARTITION_NAME = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, partitionName);
      res = pstmt.executeQuery();

      if (res.next()) {
        retValue = res.getInt(1);
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return retValue;
  }

  private void addPartitionKeys(PreparedStatement pstmt, int partitionId, PartitionDescProto partition) throws
    SQLException {
    for (int i = 0; i < partition.getPartitionKeysCount(); i++) {
      PartitionKeyProto partitionKey = partition.getPartitionKeys(i);

      pstmt.setInt(1, partitionId);
      pstmt.setString(2, partitionKey.getColumnName());
      pstmt.setString(3, partitionKey.getPartitionValue());

      pstmt.addBatch();
      pstmt.clearParameters();
    }
  }


  private void dropPartition(int tableId, String partitionName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      int partitionId = getPartitionId(tableId, partitionName);

      String sqlDeletePartitionKeys = "DELETE FROM " + TB_PARTTION_KEYS + " WHERE " + COL_PARTITIONS_PK + " = ? ";
      String sqlDeletePartition = "DELETE FROM " + TB_PARTTIONS + " WHERE " + COL_PARTITIONS_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sqlDeletePartitionKeys);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sqlDeletePartitionKeys);
      pstmt.setInt(1, partitionId);
      pstmt.executeUpdate();

      pstmt = conn.prepareStatement(sqlDeletePartition);
      pstmt.setInt(1, partitionId);
      pstmt.executeUpdate();

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  private int getDatabaseId(String databaseName) throws SQLException {
    String sql = String.format("SELECT DB_ID from %s WHERE DB_NAME = ?", TB_DATABASES);

    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new NoSuchDatabaseException(databaseName);
      }

      return res.getInt("DB_ID");
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existTable(String databaseName, final String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      int dbid = getDatabaseId(databaseName);

      String sql = "SELECT TID FROM TABLES WHERE DB_ID = ? AND " + COL_TABLES_NAME + "=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  public void dropTableInternal(Connection conn, String databaseName, final String tableName)
      throws SQLException {

    PreparedStatement pstmt = null;

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      String sql = "DELETE FROM " + TB_COLUMNS + " WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();


      sql = "DELETE FROM " + TB_OPTIONS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();


      sql = "DELETE FROM " + TB_STATISTICS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM " + TB_PARTTION_KEYS
        + " WHERE " + COL_PARTITIONS_PK
        + " IN (SELECT " + COL_PARTITIONS_PK + " FROM " + TB_PARTTIONS + " WHERE " + COL_TABLES_PK + "= ? )";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM " + TB_PARTTIONS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM " + TB_PARTITION_METHODS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM TABLES WHERE DB_ID = ? AND " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.executeUpdate();

    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void dropTable(String databaseName, final String tableName) throws CatalogException {
    Connection conn = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      dropTableInternal(conn, databaseName, tableName);
      conn.commit();
    } catch (SQLException se) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        LOG.error(e, e);
      }
    } finally {
      CatalogUtil.closeQuietly(conn);
    }
  }

  public Pair<Integer, String> getDatabaseIdAndUri(String databaseName) throws SQLException {
    String sql =
        "SELECT DB_ID, SPACE_URI from " + TB_DATABASES + " natural join " + TB_SPACES + " WHERE db_name = ?";

    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new NoSuchDatabaseException(databaseName);
      }

      return new Pair<Integer, String>(res.getInt(1), res.getString(2) + "/" + databaseName);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    CatalogProtos.TableDescProto.Builder tableBuilder = null;
    StoreType storeType;

    try {
      tableBuilder = CatalogProtos.TableDescProto.newBuilder();

      Pair<Integer, String> databaseIdAndUri = getDatabaseIdAndUri(databaseName);

      //////////////////////////////////////////
      // Geting Table Description
      //////////////////////////////////////////
      String sql =
          "SELECT TID, " + COL_TABLES_NAME + ", TABLE_TYPE, PATH, STORE_TYPE FROM TABLES " +
              "WHERE DB_ID = ? AND " + COL_TABLES_NAME + "=?";


      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseIdAndUri.getFirst());
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) { // there is no table of the given name.
        return null;
      }

      int tableId = res.getInt(1);
      tableBuilder.setTableName(CatalogUtil.buildFQName(databaseName, res.getString(2).trim()));
      TableType tableType = TableType.valueOf(res.getString(3));
      if (tableType == TableType.EXTERNAL_TABLE) {
        tableBuilder.setIsExternal(true);
      }

      if (tableType == TableType.BASE_TABLE) {
        tableBuilder.setPath(databaseIdAndUri.getSecond() + "/" + tableName);
      } else {
        tableBuilder.setPath(res.getString(4).trim());
      }

      storeType = CatalogUtil.getStoreType(res.getString(5).trim());

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Column Descriptions
      //////////////////////////////////////////
      CatalogProtos.SchemaProto.Builder schemaBuilder = CatalogProtos.SchemaProto.newBuilder();
      sql = "SELECT COLUMN_NAME, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH from " + TB_COLUMNS +
          " WHERE " + COL_TABLES_PK + " = ? ORDER BY ORDINAL_POSITION ASC";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        schemaBuilder.addFields(resultToColumnProto(res));
      }

      tableBuilder.setSchema(
          CatalogUtil.getQualfiedSchema(databaseName + "." + tableName, schemaBuilder.build()));

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Table Properties
      //////////////////////////////////////////
      CatalogProtos.TableProto.Builder metaBuilder = CatalogProtos.TableProto.newBuilder();

      metaBuilder.setStoreType(storeType);
      sql = "SELECT key_, value_ FROM " + TB_OPTIONS + " WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();
      metaBuilder.setParams(resultToKeyValueSetProto(res));
      tableBuilder.setMeta(metaBuilder);

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Table Stats
      //////////////////////////////////////////
      sql = "SELECT num_rows, num_bytes FROM " + TB_STATISTICS + " WHERE " + COL_TABLES_PK + " = ?";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        TableStatsProto.Builder statBuilder = TableStatsProto.newBuilder();
        statBuilder.setNumRows(res.getLong("num_rows"));
        statBuilder.setNumBytes(res.getLong("num_bytes"));
        tableBuilder.setStats(statBuilder);
      }
      res.close();
      pstmt.close();


      //////////////////////////////////////////
      // Getting Table Partition Method
      //////////////////////////////////////////
      sql = " SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        tableBuilder.setPartition(resultToPartitionMethodProto(databaseName, tableName, res));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return tableBuilder.build();
  }

  private Type getDataType(final String typeStr) {
    try {
      return Enum.valueOf(Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot find a matched type against from '" + typeStr + "'");
      return null;
    }
  }

  @Override
  public List<String> getAllTableNames(String databaseName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    List<String> tables = new ArrayList<String>();

    try {

      int dbid = getDatabaseId(databaseName);

      String sql = "SELECT " + COL_TABLES_NAME + " FROM TABLES WHERE DB_ID = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dbid);
      res = pstmt.executeQuery();
      while (res.next()) {
        tables.add(res.getString(COL_TABLES_NAME).trim());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return tables;
  }
  
  @Override
  public List<TableDescriptorProto> getAllTables() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<TableDescriptorProto> tables = new ArrayList<TableDescriptorProto>();

    try {
      String sql = "SELECT t.TID, t.DB_ID, t." + COL_TABLES_NAME + ", t.TABLE_TYPE, t.PATH, t.STORE_TYPE, " +
          " s.SPACE_URI FROM " + TB_TABLES + " t, " + TB_DATABASES + " d, " + TB_SPACES +
          " s WHERE t.DB_ID = d.DB_ID AND d.SPACE_ID = s.SPACE_ID";

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        TableDescriptorProto.Builder builder = TableDescriptorProto.newBuilder();
        
        builder.setTid(resultSet.getInt("TID"));
        builder.setDbId(resultSet.getInt("DB_ID"));
        String tableName = resultSet.getString(COL_TABLES_NAME);
        builder.setName(tableName);
        String tableTypeString = resultSet.getString("TABLE_TYPE");
        TableType tableType = TableType.valueOf(tableTypeString);
        builder.setTableType(tableTypeString);

        if (tableType == TableType.BASE_TABLE) {
          builder.setPath(resultSet.getString("SPACE_URI") + "/" + tableName);
        } else {
          builder.setPath(resultSet.getString("PATH"));
        }
        String storeType = resultSet.getString("STORE_TYPE");
        if (storeType != null) {
          storeType = storeType.trim();
          builder.setStoreType(storeType);
        }
        
        tables.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return tables;
  }
  
  @Override
  public List<TableOptionProto> getAllTableOptions() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<TableOptionProto> options = new ArrayList<TableOptionProto>();

    try {
      String sql = "SELECT tid, key_, value_ FROM " + TB_OPTIONS;

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        TableOptionProto.Builder builder = TableOptionProto.newBuilder();
        
        builder.setTid(resultSet.getInt("TID"));
        
        KeyValueProto.Builder keyValueBuilder = KeyValueProto.newBuilder();
        keyValueBuilder.setKey(resultSet.getString("KEY_"));
        keyValueBuilder.setValue(resultSet.getString("VALUE_"));
        builder.setKeyval(keyValueBuilder.build());
        
        options.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return options;
  }
  
  @Override
  public List<TableStatsProto> getAllTableStats() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<TableStatsProto> stats = new ArrayList<TableStatsProto>();

    try {
      String sql = "SELECT tid, num_rows, num_bytes FROM " + TB_STATISTICS;

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        TableStatsProto.Builder builder = TableStatsProto.newBuilder();
        
        builder.setTid(resultSet.getInt("TID"));
        builder.setNumRows(resultSet.getLong("NUM_ROWS"));
        builder.setNumBytes(resultSet.getLong("NUM_BYTES"));
        
        stats.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return stats;
  }
  
  @Override
  public List<ColumnProto> getAllColumns() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<ColumnProto> columns = new ArrayList<ColumnProto>();

    try {
      String sql =
          "SELECT TID, COLUMN_NAME, ORDINAL_POSITION, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH FROM " + TB_COLUMNS +
          " ORDER BY TID ASC, ORDINAL_POSITION ASC";

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        ColumnProto.Builder builder = ColumnProto.newBuilder();
        
        builder.setTid(resultSet.getInt("TID"));
        builder.setName(resultSet.getString("COLUMN_NAME"));

        int nestedFieldNum = resultSet.getInt("NESTED_FIELD_NUM");

        Type type = getDataType(resultSet.getString("DATA_TYPE").trim());
        int typeLength = resultSet.getInt("TYPE_LENGTH");

        if (nestedFieldNum > 0) {
          builder.setDataType(CatalogUtil.newRecordType(nestedFieldNum));
        } else if (typeLength > 0) {
          builder.setDataType(CatalogUtil.newDataTypeWithLen(type, typeLength));
        } else {
          builder.setDataType(CatalogUtil.newSimpleDataType(type));
        }
        
        columns.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return columns;
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "INSERT INTO " + TB_PARTITION_METHODS
        + " (" + COL_TABLES_PK + ", PARTITION_TYPE,  EXPRESSION, EXPRESSION_SCHEMA) VALUES (?,?,?,?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      String databaseName = proto.getTableIdentifier().getDatabaseName();
      String tableName = proto.getTableIdentifier().getTableName();

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, proto.getPartitionType().name());
      pstmt.setString(3, proto.getExpression());
      pstmt.setBytes(4, proto.getExpressionSchema().toByteArray());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void dropPartitionMethod(String databaseName, String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "DELETE FROM " + TB_PARTITION_METHODS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        return resultToPartitionMethodProto(databaseName, tableName, res);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return null;
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    boolean exist = false;

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + "= ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return exist;
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                       String partitionName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    PartitionDescProto.Builder builder = null;

    try {
      String sql = "SELECT PATH, " + COL_PARTITIONS_PK + " FROM " + TB_PARTTIONS +
        " WHERE " + COL_TABLES_PK + " = ? AND PARTITION_NAME = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, partitionName);
      res = pstmt.executeQuery();

      if (res.next()) {
        builder = PartitionDescProto.newBuilder();
        builder.setPath(res.getString("PATH"));
        builder.setPartitionName(partitionName);
        setPartitionKeys(res.getInt(COL_PARTITIONS_PK), builder);
      } else {
        return null;
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return builder.build();
  }

  private void setPartitionKeys(int pid, PartitionDescProto.Builder partitionDesc) throws
    CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "SELECT "+ COL_COLUMN_NAME + " , "+ COL_PARTITION_VALUE
        + " FROM " + TB_PARTTION_KEYS + " WHERE " + COL_PARTITIONS_PK + " = ? ";

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, pid);
      res = pstmt.executeQuery();

      while (res.next()) {
        PartitionKeyProto.Builder builder = PartitionKeyProto.newBuilder();
        builder.setColumnName(res.getString(COL_COLUMN_NAME));
        builder.setPartitionValue(res.getString(COL_PARTITION_VALUE));
        partitionDesc.addPartitionKeys(builder);
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public List<PartitionDescProto> getPartitions(String databaseName, String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    PartitionDescProto.Builder builder = null;
    List<PartitionDescProto> partitions = new ArrayList<PartitionDescProto>();

    try {
      String sql = "SELECT PATH, PARTITION_NAME, " + COL_PARTITIONS_PK + " FROM "
        + TB_PARTTIONS +" WHERE " + COL_TABLES_PK + " = ?  ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        builder = PartitionDescProto.newBuilder();
        builder.setPath(res.getString("PATH"));
        builder.setPartitionName(res.getString("PARTITION_NAME"));
        setPartitionKeys(res.getInt(COL_PARTITIONS_PK), builder);
        partitions.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return partitions;
  }

  @Override
  public List<TablePartitionProto> getAllPartitions() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<TablePartitionProto> partitions = new ArrayList<TablePartitionProto>();

    try {
      String sql = "SELECT " + COL_PARTITIONS_PK + ", " + COL_TABLES_PK + ", PARTITION_NAME, " +
        " PATH FROM " + TB_PARTTIONS;

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        TablePartitionProto.Builder builder = TablePartitionProto.newBuilder();

        builder.setPartitionId(resultSet.getInt(COL_PARTITIONS_PK));
        builder.setTid(resultSet.getInt(COL_TABLES_PK));
        builder.setPartitionName(resultSet.getString("PARTITION_NAME"));
        builder.setPath(resultSet.getString("PATH"));

        partitions.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }

    return partitions;
  }


  @Override
  public void createIndex(final IndexDescProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    String databaseName = proto.getTableIdentifier().getDatabaseName();
    String tableName = proto.getTableIdentifier().getTableName();
    String columnName = CatalogUtil.extractSimpleName(proto.getColumn().getName());

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      String sql = "INSERT INTO " + TB_INDEXES +
          " (" + COL_DATABASES_PK + ", " + COL_TABLES_PK + ", INDEX_NAME, " +
          "" + COL_COLUMN_NAME + ", DATA_TYPE, INDEX_TYPE, IS_UNIQUE, IS_CLUSTERED, IS_ASCENDING) " +
        "VALUES (?,?,?,?,?,?,?,?,?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      conn.setAutoCommit(false);

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.setString(3, proto.getIndexName());
      pstmt.setString(4, columnName);
      pstmt.setString(5, proto.getColumn().getDataType().getType().name());
      pstmt.setString(6, proto.getIndexMethod().toString());
      pstmt.setBoolean(7, proto.hasIsUnique() && proto.getIsUnique());
      pstmt.setBoolean(8, proto.hasIsClustered() && proto.getIsClustered());
      pstmt.setBoolean(9, proto.hasIsAscending() && proto.getIsAscending());
      pstmt.executeUpdate();
      conn.commit();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void dropIndex(String databaseName, final String indexName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      int databaseId = getDatabaseId(databaseName);
      String sql = "DELETE FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  public static String getTableName(Connection conn, int tableId) throws SQLException {
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      pstmt =
          conn.prepareStatement("SELECT " + COL_TABLES_NAME + " FROM " + TB_TABLES + " WHERE " + COL_TABLES_PK + "=?");
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("Cannot get any table name from TID");
      }
      return res.getString(1);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  final static String GET_INDEXES_SQL =
      "SELECT " + COL_TABLES_PK + ", INDEX_NAME, COLUMN_NAME, DATA_TYPE, INDEX_TYPE, IS_UNIQUE, " +
          "IS_CLUSTERED, IS_ASCENDING FROM " + TB_INDEXES;


  @Override
  public IndexDescProto getIndexByName(String databaseName, final String indexName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + indexName);
      }
      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      resultToIndexDescProtoBuilder(builder, res);
      String tableName = getTableName(conn, res.getInt(COL_TABLES_PK));
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      proto = builder.build();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return proto;
  }

  @Override
  public IndexDescProto getIndexByColumn(final String databaseName,
                                         final String tableName,
                                         final String columnName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND COLUMN_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      ;
      pstmt.setString(2, columnName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + columnName);
      }
      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      resultToIndexDescProtoBuilder(builder, res);
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      proto = builder.build();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return proto;
  }

  @Override
  public boolean existIndexByName(String databaseName, final String indexName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    boolean exist = false;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql =
          "SELECT INDEX_NAME FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    boolean exist = false;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql =
          "SELECT INDEX_NAME FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND COLUMN_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, columnName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return exist;
  }

  @Override
  public IndexDescProto[] getIndexes(String databaseName, final String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    final List<IndexDescProto> protos = new ArrayList<IndexDescProto>();

    try {
      final int databaseId = getDatabaseId(databaseName);
      final int tableId = getTableId(databaseId, databaseName, tableName);
      final TableIdentifierProto tableIdentifier = CatalogUtil.buildTableIdentifier(databaseName, tableName);


      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_PK + "=?";


      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        IndexDescProto.Builder builder = IndexDescProto.newBuilder();
        resultToIndexDescProtoBuilder(builder, res);
        builder.setTableIdentifier(tableIdentifier);
        protos.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
  }
  
  @Override
  public List<IndexProto> getAllIndexes() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    List<IndexProto> indexes = new ArrayList<IndexProto>();

    try {
      String sql = "SELECT " + COL_DATABASES_PK + ", " + COL_TABLES_PK + ", INDEX_NAME, " +
        "COLUMN_NAME, DATA_TYPE, INDEX_TYPE, IS_UNIQUE, IS_CLUSTERED, IS_ASCENDING FROM " + TB_INDEXES;

      conn = getConnection();
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      while (resultSet.next()) {
        IndexProto.Builder builder = IndexProto.newBuilder();
        
        builder.setDbId(resultSet.getInt(COL_DATABASES_PK));
        builder.setTId(resultSet.getInt(COL_TABLES_PK));
        builder.setIndexName(resultSet.getString("INDEX_NAME"));
        builder.setColumnName(resultSet.getString("COLUMN_NAME"));
        builder.setDataType(resultSet.getString("DATA_TYPE"));
        builder.setIndexType(resultSet.getString("INDEX_TYPE"));
        builder.setIsUnique(resultSet.getBoolean("IS_UNIQUE"));
        builder.setIsClustered(resultSet.getBoolean("IS_CLUSTERED"));
        builder.setIsAscending(resultSet.getBoolean("IS_ASCENDING"));
        
        indexes.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return indexes;
  }

  private void resultToIndexDescProtoBuilder(IndexDescProto.Builder builder,
                                             final ResultSet res) throws SQLException {
    builder.setIndexName(res.getString("index_name"));
    builder.setColumn(indexResultToColumnProto(res));
    builder.setIndexMethod(getIndexMethod(res.getString("index_type").trim()));
    builder.setIsUnique(res.getBoolean("is_unique"));
    builder.setIsClustered(res.getBoolean("is_clustered"));
    builder.setIsAscending(res.getBoolean("is_ascending"));
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

    int nestedFieldNum = res.getInt("NESTED_FIELD_NUM");

    Type type = getDataType(res.getString("data_type").trim());
    int typeLength = res.getInt("type_length");

    if (nestedFieldNum > 0) {
      builder.setDataType(CatalogUtil.newRecordType(nestedFieldNum));
    } else if (typeLength > 0) {
      builder.setDataType(CatalogUtil.newDataTypeWithLen(type, typeLength));
    } else {
      builder.setDataType(CatalogUtil.newSimpleDataType(type));
    }

    return builder.build();
  }

  private KeyValueSetProto resultToKeyValueSetProto(final ResultSet res) throws SQLException {
    KeyValueSetProto.Builder setBuilder = KeyValueSetProto.newBuilder();
    KeyValueProto.Builder builder = KeyValueProto.newBuilder();
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
      LOG.error("Cannot find a matched type against from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }

  private CatalogProtos.PartitionMethodProto resultToPartitionMethodProto(final String databaseName,
                                                                          final String tableName,
                                                                          final ResultSet res)
      throws SQLException, InvalidProtocolBufferException {
    CatalogProtos.PartitionMethodProto.Builder partBuilder = CatalogProtos.PartitionMethodProto.newBuilder();
    partBuilder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
    partBuilder.setPartitionType(CatalogProtos.PartitionType.valueOf(res.getString("partition_type")));
    partBuilder.setExpression(res.getString("expression"));
    partBuilder.setExpressionSchema(SchemaProto.parseFrom(res.getBytes("expression_schema")));
    return partBuilder.build();
  }


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

  private boolean existColumn(final int tableId, final String columnName) throws CatalogException {
    Connection conn ;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {

      String sql = "SELECT COLUMN_NAME FROM " + TB_COLUMNS + " WHERE TID = ? AND COLUMN_NAME = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      pstmt.setInt(1, tableId);
      pstmt.setString(2, columnName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }
}
