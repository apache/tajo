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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.util.JavaResourceUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public abstract class AbstractDBStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());
  protected final Configuration conf;
  protected final String connectionId;
  protected final String connectionPassword;
  protected final String catalogUri;

  protected final String insertPartitionSql = "INSERT INTO " + TB_PARTTIONS
    + "(" + COL_TABLES_PK + ", PARTITION_NAME, PATH) VALUES (?, ? , ?)";

  protected final String insertPartitionKeysSql = "INSERT INTO " + TB_PARTTION_KEYS  + "("
    + COL_PARTITIONS_PK + ", " + COL_TABLES_PK + ", "
    + COL_COLUMN_NAME + ", " + COL_PARTITION_VALUE + ")"
    + " VALUES ( ("
    + " SELECT " + COL_PARTITIONS_PK + " FROM " + TB_PARTTIONS
    + " WHERE " + COL_TABLES_PK + " = ? AND PARTITION_NAME = ? ) "
    + " , ?, ?, ?)";

  protected final String deletePartitionSql = "DELETE FROM " + TB_PARTTIONS
    + " WHERE " + COL_PARTITIONS_PK + " = ? ";

  protected final String deletePartitionKeysSql = "DELETE FROM " + TB_PARTTIONS
    + " WHERE " + COL_PARTITIONS_PK + " = ? ";

  private Connection conn;
  
  protected XMLCatalogSchemaManager catalogSchemaManager;

  public static boolean needShutdown(String catalogUri) {
    URI uri = URI.create(catalogUri);
    // If the current catalog is embedded in-memory derby, shutdown is required.
    if (uri.getHost() == null) {
      String schemeSpecificPart = uri.getSchemeSpecificPart();
      if (schemeSpecificPart != null) {
        return schemeSpecificPart.contains("memory");
      }
    }
    return false;
  }

  protected abstract String getCatalogDriverName();
  
  protected String getCatalogSchemaPath() {
    return "";
  }

  protected abstract Connection createConnection(final Configuration conf) throws SQLException;
  
  protected void createDatabaseDependants() {
  }
  
  protected boolean isInitialized() {
    return catalogSchemaManager.isInitialized(getConnection());
  }

  protected boolean catalogAlreadyExists() {
    return catalogSchemaManager.catalogAlreadyExists(getConnection());
  }

  protected void createBaseTable() {
    createDatabaseDependants();
    
    catalogSchemaManager.createBaseSchema(getConnection());
    
    insertSchemaVersion();
  }

  protected void dropBaseTable() {
    catalogSchemaManager.dropBaseSchema(getConnection());
  }

  public AbstractDBStore(Configuration conf) {
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
      throw new TajoInternalError(e);
    }

    try {
      LOG.info("Trying to connect database (" + catalogUri + ")");
      conn = createConnection(conf);
      LOG.info("Connected to database (" + catalogUri + ")");
    } catch (SQLException e) {
      throw new MetadataConnectionException(catalogUri , e);
    }

    String schemaPath = getCatalogSchemaPath();
    if (schemaPath != null && !schemaPath.isEmpty()) {
      this.catalogSchemaManager = new XMLCatalogSchemaManager(schemaPath);
    }
    
    try {
      if (catalogAlreadyExists()) {
        LOG.info("The meta table of CatalogServer already is created.");
        verifySchemaVersion();
      } else {
        if (isInitialized()) {
          LOG.info("The base tables of CatalogServer already is initialized.");
          verifySchemaVersion();
        } else {
          try {
            createBaseTable();
            LOG.info("The base tables of CatalogServer are created.");
          } catch (Throwable e) {
            dropBaseTable();
            throw e;
          }
        }
     }
    } catch (Throwable se) {
      throw new TajoInternalError(se);
    }
  }

  public String getUri() {
    return catalogUri;
  }

  public int getDriverVersion() {
    return catalogSchemaManager.getCatalogStore().getSchema().getVersion();
  }

  public String readSchemaFile(String path) {
    try {
      return JavaResourceUtil.readTextFromResource("schemas/" + path);
    } catch (IOException e) {
      throw new TajoInternalError(e);
    }
  }

  protected String getCatalogUri() {
    return catalogUri;
  }

  protected boolean isConnValid(int timeout) {
    boolean isValid = false;

    try {
      isValid = conn.isValid(timeout);
    } catch (SQLException e) {
      LOG.warn(e);
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
      throw new TajoInternalError(e);
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
      throw new TajoInternalError(e);
    } finally {
      CatalogUtil.closeQuietly(pstmt, result);
    }
    
    return schemaVersion;
  }

  private void verifySchemaVersion() throws CatalogUpgradeRequiredException {
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
      LOG.error("| please refer http://tajo.apache.org/docs/current/backup_and_restore/catalog.html |");
      LOG.error("=========================================================================");
      throw new CatalogUpgradeRequiredException();
    }

    LOG.info(String.format("The compatibility of the catalog schema (version: %d) has been verified.",
        getDriverVersion()));
  }

  /**
   * Insert the version of the current catalog schema
   */
  protected void insertSchemaVersion() {
    Connection conn;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement("INSERT INTO META VALUES (?)");
      pstmt.setInt(1, getDriverVersion());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void createTablespace(String spaceName, String spaceUri) throws DuplicateTablespaceException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String sql = String.format("SELECT SPACE_ID FROM %s WHERE SPACE_NAME=(?)", TB_SPACES);
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, spaceName);
      res = pstmt.executeQuery();
      if (res.next()) {
        throw new DuplicateTablespaceException(spaceName);
      }
      res.close();
      pstmt.close();

      sql = String.format("INSERT INTO %s (SPACE_NAME, SPACE_URI) VALUES (?, ?)", TB_SPACES);

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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existTablespace(String tableSpaceName) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public void dropTablespace(String tableSpaceName) throws UndefinedTablespaceException {

    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      TableSpaceInternal tableSpace = getTableSpaceInfo(tableSpaceName);
      Collection<String> databaseNames = getAllDatabaseNamesInternal(COL_TABLESPACE_PK + " = " + tableSpace.spaceId);

      conn = getConnection();
      conn.setAutoCommit(false);

      for (String databaseName : databaseNames) {
        try {
          dropDatabase(databaseName);
        } catch (UndefinedDatabaseException e) {
          LOG.warn(e);
          continue;
        }
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public Collection<String> getAllTablespaceNames() {
    return getAllTablespaceNamesInternal(null);
  }

  private Collection<String> getAllTablespaceNamesInternal(@Nullable String whereCondition) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }

    return tablespaceNames;
  }
  
  @Override
  public List<TablespaceProto> getTablespaces() {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
  }

  @Override
  public TablespaceProto getTablespace(String spaceName) throws UndefinedTablespaceException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    try {
      String sql = "SELECT * FROM " + TB_SPACES + " WHERE SPACE_NAME=?";
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, spaceName);
      resultSet = pstmt.executeQuery();

      if (!resultSet.next()) {
        throw new UndefinedTablespaceException(spaceName);
      }

      int spaceId = resultSet.getInt(COL_TABLESPACE_PK);
      String retrieveSpaceName = resultSet.getString("SPACE_NAME");
      String handler = resultSet.getString("SPACE_HANDLER");
      String uri = resultSet.getString("SPACE_URI");

      return TablespaceProto.newBuilder().
          setId(spaceId).
          setSpaceName(retrieveSpaceName).
          setHandler(handler).
          setUri(uri).
          build();


    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }
  }

  @Override
  public void alterTablespace(AlterTablespaceProto alterProto) throws UndefinedTablespaceException {
    Connection conn;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      String sql = String.format("SELECT SPACE_NAME FROM %s WHERE SPACE_NAME=?", TB_SPACES);
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, alterProto.getSpaceName());
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new UndefinedTablespaceException(alterProto.getSpaceName());
      }
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    if (alterProto.getCommandList().size() == 1) {
      AlterTablespaceCommand command = alterProto.getCommand(0);
      if (command.getType() == AlterTablespaceProto.AlterTablespaceType.LOCATION) {
        final String uri = command.getLocation();
        try {
          String sql = "UPDATE " + TB_SPACES + " SET SPACE_URI=? WHERE SPACE_NAME=?";

          pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, uri);
          pstmt.setString(2, alterProto.getSpaceName());
          pstmt.executeUpdate();
        } catch (SQLException se) {
          throw new TajoInternalError(se);
        } finally {
          CatalogUtil.closeQuietly(pstmt);
        }
      }
    }
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName)
      throws UndefinedTablespaceException, DuplicateDatabaseException {

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    if (existDatabase(databaseName)) {
      throw new DuplicateDatabaseException(databaseName);
    }

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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existDatabase(String databaseName) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public void dropDatabase(String databaseName) throws UndefinedDatabaseException {
    Collection<String> tableNames = getAllTableNames(databaseName);

    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      for (String tableName : tableNames) {
        try {
          dropTableInternal(conn, databaseName, tableName);
        } catch (UndefinedTableException e) {
          LOG.warn(e);
        }
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public Collection<String> getAllDatabaseNames() {
    return getAllDatabaseNamesInternal(null);
  }

  private Collection<String> getAllDatabaseNamesInternal(@Nullable String whereCondition) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, resultSet);
    }

    return databaseNames;
  }
  
  @Override
  public List<DatabaseProto> getAllDatabases() {
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
      throw new TajoInternalError(se);
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

  private TableSpaceInternal getTableSpaceInfo(String spaceName) throws UndefinedTablespaceException {
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
        throw new UndefinedTablespaceException(spaceName);
      }
      return new TableSpaceInternal(res.getInt(1), res.getString(2), res.getString(3));
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  private int getTableId(int databaseId, String databaseName, String tableName) throws UndefinedTableException {
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
        throw new UndefinedTableException(databaseName, tableName);
      }
      return res.getInt(1);
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  enum TableType {
    MANAGED,
    EXTERNAL
  }

  @Override
  public void createTable(final CatalogProtos.TableDescProto table)
      throws UndefinedDatabaseException, DuplicateTableException {

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    String[] splitted = CatalogUtil.splitTableName(table.getTableName());
    if (splitted.length == 1) {
      throw new TajoInternalError(
          "createTable() requires a qualified table name, but it is '" + table.getTableName() + "'");
    }
    final String databaseName = splitted[0];
    final String tableName = splitted[1];

    if (existTable(databaseName, tableName)) {
      throw new DuplicateTableException(tableName);
    }
    final int dbid = getDatabaseId(databaseName);

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String sql = "INSERT INTO TABLES (DB_ID, " + COL_TABLES_NAME +
          ", TABLE_TYPE, PATH, STORE_TYPE) VALUES(?, ?, ?, ?, ?) ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      if (table.getIsExternal()) {
        pstmt.setString(3, TableType.EXTERNAL.name());
      } else {
        pstmt.setString(3, TableType.MANAGED.name());
      }
      pstmt.setString(4, table.getPath());
      pstmt.setString(5, table.getMeta().getStoreType());
      pstmt.executeUpdate();
      pstmt.close();

      String tidSql =
          "SELECT TID from " + TB_TABLES + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_NAME + "=?";
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new TajoInternalError("There is no TID matched to '" + table.getTableName() + '"');
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public void updateTableStats(final CatalogProtos.UpdateTableStatsProto statsProto)
      throws UndefinedDatabaseException, UndefinedTableException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    String[] splitted = CatalogUtil.splitTableName(statsProto.getTableName());
    if (splitted.length == 1) {
      throw new IllegalArgumentException("updateTableStats() requires a qualified table name, but it is \""
          + statsProto.getTableName() + "\".");
    }
    final String databaseName = splitted[0];
    final String tableName = splitted[1];

    final int dbid = getDatabaseId(databaseName);

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String tidSql =
        "SELECT TID from " + TB_TABLES + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_NAME + "=?";
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new UndefinedTableException(statsProto.getTableName());
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
        pstmt.setLong(1, statsProto.getStats().getNumRows());
        pstmt.setLong(2, statsProto.getStats().getNumBytes());
        pstmt.setInt(3, tableId);
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto)
      throws UndefinedDatabaseException, DuplicateTableException, DuplicateColumnException,
      DuplicatePartitionException, UndefinedPartitionException, UndefinedColumnException, UndefinedTableException,
      UndefinedPartitionMethodException, AmbiguousTableException {

    String[] splitted = CatalogUtil.splitTableName(alterTableDescProto.getTableName());
    if (splitted.length == 1) {
      throw new IllegalArgumentException("alterTable() requires a qualified table name, but it is \""
          + alterTableDescProto.getTableName() + "\".");
    }
    final String databaseName = splitted[0];
    final String tableName = splitted[1];
    String partitionName = null;
    CatalogProtos.PartitionDescProto partitionDesc = null;

    int databaseId = getDatabaseId(databaseName);
    int tableId = getTableId(databaseId, databaseName, tableName);

    switch (alterTableDescProto.getAlterTableType()) {
    case RENAME_TABLE:
      String simpleNewTableName = CatalogUtil.extractSimpleName(alterTableDescProto.getNewTableName());
      if (existTable(databaseName, simpleNewTableName)) {
        throw new DuplicateTableException(alterTableDescProto.getNewTableName());
      }
      if (alterTableDescProto.hasNewTablePath()) {
        renameManagedTable(tableId, simpleNewTableName, alterTableDescProto.getNewTablePath());
      } else {
        renameExternalTable(tableId, simpleNewTableName);
      }
      break;
    case RENAME_COLUMN:
      if (existColumn(tableId, alterTableDescProto.getAlterColumnName().getNewColumnName())) {
        throw new DuplicateColumnException(alterTableDescProto.getAlterColumnName().getNewColumnName());
      }
      renameColumn(tableId, alterTableDescProto.getAlterColumnName());
      break;
    case ADD_COLUMN:
      if (existColumn(tableId, alterTableDescProto.getAddColumn().getName())) {
        throw new DuplicateColumnException(alterTableDescProto.getAddColumn().getName());
      }
      addNewColumn(tableId, alterTableDescProto.getAddColumn());
      break;
    case ADD_PARTITION:
      partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
      try {
        // check if it exists
        getPartition(databaseName, tableName, partitionName);
        throw new DuplicatePartitionException(partitionName);
      } catch (UndefinedPartitionException e) {
      }
      addPartition(tableId, alterTableDescProto.getPartitionDesc());
      break;
    case DROP_PARTITION:
      partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
      partitionDesc = getPartition(databaseName, tableName, partitionName);
      if (partitionDesc == null) {
        throw new UndefinedPartitionException(partitionName);
      }
      dropPartition(partitionDesc.getId());
      break;
    case SET_PROPERTY:
      setProperties(tableId, alterTableDescProto.getParams());
      break;
    default:
    }
  }

  private Map<String, String> getTableOptions(final int tableId) {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    Map<String, String> options = TUtil.newHashMap();

    try {
      String tidSql = "SELECT key_, value_ FROM " + TB_OPTIONS + " WHERE TID=?";

      conn = getConnection();
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        options.put(res.getString("KEY_"), res.getString("VALUE_"));
      }
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return options;
  }

  private void setProperties(final int tableId, final KeyValueSetProto properties) {
    final String updateSql = "UPDATE " + TB_OPTIONS + " SET VALUE_=? WHERE TID=? AND KEY_=?";
    final String insertSql = "INSERT INTO " + TB_OPTIONS + " (TID, KEY_, VALUE_) VALUES(?, ?, ?)";

    Connection conn;
    PreparedStatement pstmt = null;

    Map<String, String> oldProperties = getTableOptions(tableId);

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      for (KeyValueProto entry : properties.getKeyvalList()) {
        if (oldProperties.containsKey(entry.getKey())) {
          // replace old property with new one
          pstmt = conn.prepareStatement(updateSql);

          pstmt.setString(1, entry.getValue());
          pstmt.setInt(2, tableId);
          pstmt.setString(3, entry.getKey());
          pstmt.executeUpdate();
          pstmt.close();
         } else {
          // insert new property
          pstmt = conn.prepareStatement(insertSql);

          pstmt.setInt(1, tableId);
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          pstmt.executeUpdate();
          pstmt.close();
        }
      }

      conn.commit();
    } catch (Throwable sqlException) {
      throw new TajoInternalError(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  private void renameExternalTable(final int tableId, final String tableName) {

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

    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  private void renameManagedTable(final int tableId, final String tableName, final String newTablePath) {

    final String updtaeRenameTableSql = "UPDATE " + TB_TABLES + " SET " + COL_TABLES_NAME + " = ? , PATH = ?"
        + " WHERE TID = ?";

    if (LOG.isDebugEnabled()) {
      LOG.debug(updtaeRenameTableSql);
    }

    Connection conn;
    PreparedStatement pstmt = null;

    try {

      conn = getConnection();
      pstmt = conn.prepareStatement(updtaeRenameTableSql);
      pstmt.setString(1, tableName);
      pstmt.setString(2, newTablePath);
      pstmt.setInt(3, tableId);
      pstmt.executeUpdate();

    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  private void renameColumn(final int tableId, final CatalogProtos.AlterColumnProto alterColumnProto)
      throws UndefinedColumnException, AmbiguousTableException {

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

      String tableName = CatalogUtil.extractQualifier(alterColumnProto.getOldColumnName());
      String simpleOldColumnName = CatalogUtil.extractSimpleName(alterColumnProto.getOldColumnName());
      String simpleNewColumnName = CatalogUtil.extractSimpleName(alterColumnProto.getNewColumnName());

      if (!tableName.equals(CatalogUtil.extractQualifier(alterColumnProto.getNewColumnName()))) {
        throw new AmbiguousTableException(
            tableName + ", " + CatalogUtil.extractQualifier(alterColumnProto.getNewColumnName()));
      }

      //SELECT COLUMN
      pstmt = conn.prepareStatement(selectColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, simpleOldColumnName);
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
        throw new UndefinedColumnException(alterColumnProto.getOldColumnName());
      }

      resultSet.close();
      pstmt.close();
      resultSet = null;

      //DELETE COLUMN
      pstmt = conn.prepareStatement(deleteColumnNameSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, simpleOldColumnName);
      pstmt.executeUpdate();
      pstmt.close();

      //INSERT COLUMN
      pstmt = conn.prepareStatement(insertNewColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, simpleNewColumnName);
      pstmt.setInt(3, ordinalPosition);
      pstmt.setInt(4, nestedFieldNum);
      pstmt.setString(5, columnProto.getDataType().getType().name());
      pstmt.setInt(6, (columnProto.getDataType().hasLength() ? columnProto.getDataType().getLength() : 0));
      pstmt.executeUpdate();

      conn.commit();


    } catch (SQLException sqlException) {
      throw new TajoInternalError(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt,resultSet);
    }
  }

  private void addNewColumn(int tableId, CatalogProtos.ColumnProto columnProto) throws DuplicateColumnException {

    Connection conn;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    final String existColumnSql =
        String.format("SELECT COLUMN_NAME FROM %s WHERE TID=? AND COLUMN_NAME=?", TB_COLUMNS);

    final String insertNewColumnSql =
        "INSERT INTO " + TB_COLUMNS +
            " (TID, COLUMN_NAME, ORDINAL_POSITION, NESTED_FIELD_NUM, DATA_TYPE, TYPE_LENGTH) VALUES(?, ?, ?, ?, ?, ?) ";
    final String columnCountSql =
        "SELECT MAX(ORDINAL_POSITION) AS POSITION FROM " + TB_COLUMNS + " WHERE TID = ?";

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(existColumnSql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, CatalogUtil.extractSimpleName(columnProto.getName()));
      resultSet =  pstmt.executeQuery();

      if (resultSet.next()) {
        throw new DuplicateColumnException(columnProto.getName());
      }
      pstmt.close();
      resultSet.close();

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
      throw new TajoInternalError(sqlException);
    } finally {
      CatalogUtil.closeQuietly(pstmt,resultSet);
    }
  }

  private void addPartition(int tableId, CatalogProtos.PartitionDescProto partition) {
    Connection conn = null;
    PreparedStatement pstmt1 = null, pstmt2 = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      pstmt1 = conn.prepareStatement(insertPartitionSql);
      pstmt1.setInt(1, tableId);
      pstmt1.setString(2, partition.getPartitionName());
      pstmt1.setString(3, partition.getPath());
      pstmt1.executeUpdate();

      pstmt2 = conn.prepareStatement(insertPartitionKeysSql);

      for (int i = 0; i < partition.getPartitionKeysCount(); i++) {
        PartitionKeyProto partitionKey = partition.getPartitionKeys(i);
        pstmt2.setInt(1, tableId);
        pstmt2.setString(2, partition.getPartitionName());
        pstmt2.setInt(3, tableId);
        pstmt2.setString(4, partitionKey.getColumnName());
        pstmt2.setString(5, partitionKey.getPartitionValue());
        pstmt2.addBatch();
        pstmt2.clearParameters();
      }
      pstmt2.executeBatch();

      if (conn != null) {
        conn.commit();
      }
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt1);
      CatalogUtil.closeQuietly(pstmt2);
    }
  }

  private void dropPartition(int partitionId) {
    Connection conn = null;
    PreparedStatement pstmt1 = null, pstmt2 = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      pstmt1 = conn.prepareStatement(deletePartitionKeysSql);
      pstmt1.setInt(1, partitionId);
      pstmt1.executeUpdate();

      pstmt2 = conn.prepareStatement(deletePartitionSql);
      pstmt2.setInt(1, partitionId);
      pstmt2.executeUpdate();

      if (conn != null) {
        conn.commit();
      }
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt1);
      CatalogUtil.closeQuietly(pstmt2);
    }
  }

  private int getDatabaseId(String databaseName) throws UndefinedDatabaseException {
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
        throw new UndefinedDatabaseException(databaseName);
      }

      return res.getInt("DB_ID");
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existTable(String databaseName, final String tableName) throws UndefinedDatabaseException {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  public void dropTableInternal(Connection conn, String databaseName, final String tableName)
      throws SQLException, UndefinedDatabaseException, UndefinedTableException {

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
  public void dropTable(String databaseName, final String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

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

  public Pair<Integer, String> getDatabaseIdAndUri(String databaseName) throws UndefinedDatabaseException {

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
        throw new UndefinedDatabaseException(databaseName);
      }

      return new Pair<>(res.getInt(1), res.getString(2) + "/" + databaseName);
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

    Connection conn;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    CatalogProtos.TableDescProto.Builder tableBuilder = null;
    String storeType;

    Pair<Integer, String> databaseIdAndUri = getDatabaseIdAndUri(databaseName);

    try {
      tableBuilder = CatalogProtos.TableDescProto.newBuilder();

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
         throw new UndefinedTableException(tableName);
      }

      int tableId = res.getInt(1);
      tableBuilder.setTableName(CatalogUtil.buildFQName(databaseName, res.getString(2).trim()));
      TableType tableType = TableType.valueOf(res.getString(3));
      if (tableType == TableType.EXTERNAL) {
        tableBuilder.setIsExternal(true);
      }

      tableBuilder.setPath(res.getString(4).trim());
      storeType = res.getString(5).trim();

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
    } catch (SQLException se) {
      throw new TajoInternalError(se);
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
  public List<String> getAllTableNames(String databaseName) throws UndefinedDatabaseException {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return tables;
  }
  
  @Override
  public List<TableDescriptorProto> getAllTables() {
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

        if (tableType == TableType.MANAGED) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return tables;
  }
  
  @Override
  public List<TableOptionProto> getAllTableProperties() {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return options;
  }
  
  @Override
  public List<TableStatsProto> getAllTableStats() {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return stats;
  }
  
  @Override
  public List<ColumnProto> getAllColumns() {
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

        int tid = resultSet.getInt("TID");
        String databaseName = getDatabaseNameOfTable(conn, tid);
        String tableName = getTableName(conn, tid);
        builder.setTid(tid);
        builder.setName(CatalogUtil.buildFQName(databaseName, tableName, resultSet.getString("COLUMN_NAME")));

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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }
    
    return columns;
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String tableName) throws
      UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException {

    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    final int databaseId = getDatabaseId(databaseName);
    final int tableId = getTableId(databaseId, databaseName, tableName);
    ensurePartitionTable(tableName, tableId);

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        return resultToPartitionMethodProto(databaseName, tableName, res);
      } else {
        throw new UndefinedPartitionMethodException(tableName);
      }

    } catch (Throwable se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return exist;
  }

  /**
   * Ensure if the table is partitioned table.
   *
   * @param tbName Table name
   * @param tableId Table id
   * @throws UndefinedTableException
   * @throws UndefinedDatabaseException
   * @throws UndefinedPartitionMethodException
   */
  private void ensurePartitionTable(String tbName, int tableId)
      throws UndefinedTableException, UndefinedDatabaseException, UndefinedPartitionMethodException {

    Connection conn;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + "= ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new UndefinedPartitionMethodException(tbName);
      }
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                       String partitionName)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
      UndefinedPartitionException {

    final int databaseId = getDatabaseId(databaseName);
    final int tableId = getTableId(databaseId, databaseName, tableName);
    ensurePartitionTable(tableName, tableId);

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

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, partitionName);
      res = pstmt.executeQuery();

      if (res.next()) {
        builder = PartitionDescProto.newBuilder();
        builder.setId(res.getInt(COL_PARTITIONS_PK));
        builder.setPath(res.getString("PATH"));
        builder.setPartitionName(partitionName);
        setPartitionKeys(res.getInt(COL_PARTITIONS_PK), builder);
      } else {
        throw new UndefinedPartitionException(partitionName);
      }
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return builder.build();
  }

  private void setPartitionKeys(int pid, PartitionDescProto.Builder partitionDesc) {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "SELECT "+ COL_COLUMN_NAME  + " , "+ COL_PARTITION_VALUE
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public List<PartitionDescProto> getPartitions(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException {

    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    PartitionDescProto.Builder builder = null;
    List<PartitionDescProto> partitions = new ArrayList<PartitionDescProto>();

    final int databaseId = getDatabaseId(databaseName);
    final int tableId = getTableId(databaseId, databaseName, tableName);
    ensurePartitionTable(tableName, tableId);

    try {
      String sql = "SELECT PATH, PARTITION_NAME, " + COL_PARTITIONS_PK + " FROM "
        + TB_PARTTIONS +" WHERE " + COL_TABLES_PK + " = ?  ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return partitions;
  }

  @Override
  public List<TablePartitionProto> getAllPartitions() {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(stmt, resultSet);
    }

    return partitions;
  }

  @Override
  public void addPartitions(String databaseName, String tableName, List<CatalogProtos.PartitionDescProto> partitions
    , boolean ifNotExists) throws UndefinedDatabaseException, UndefinedTableException,
      UndefinedPartitionMethodException {

    final int databaseId = getDatabaseId(databaseName);
    final int tableId = getTableId(databaseId, databaseName, tableName);
    ensurePartitionTable(tableName, tableId);


    Connection conn = null;
    // To delete existing partition keys
    PreparedStatement pstmt1 = null;
    // To delete existing partition;
    PreparedStatement pstmt2 = null;
    // To insert a partition
    PreparedStatement pstmt3 = null;
    // To insert partition keys
    PreparedStatement pstmt4 = null;

    PartitionDescProto partitionDesc = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      int currentIndex = 0, lastIndex = 0;

      pstmt1 = conn.prepareStatement(deletePartitionKeysSql);
      pstmt2 = conn.prepareStatement(deletePartitionSql);
      pstmt3 = conn.prepareStatement(insertPartitionSql);
      pstmt4 = conn.prepareStatement(insertPartitionKeysSql);

      // Set a batch size like 1000. This avoids SQL injection and also takes care of out of memory issue.
      int batchSize = conf.getInt(TajoConf.ConfVars.PARTITION_DYNAMIC_BULK_INSERT_BATCH_SIZE.varname, 1000);
      for(currentIndex = 0; currentIndex < partitions.size(); currentIndex++) {
        PartitionDescProto partition = partitions.get(currentIndex);

        try {
          partitionDesc = getPartition(databaseName, tableName, partition.getPartitionName());
          // Delete existing partition and partition keys
          if (ifNotExists) {
            pstmt1.setInt(1, partitionDesc.getId());
            pstmt1.addBatch();
            pstmt1.clearParameters();

            pstmt2.setInt(1, partitionDesc.getId());
            pstmt2.addBatch();
            pstmt2.clearParameters();
          }
        } catch (UndefinedPartitionException e) {
        }

        // Insert partition
        pstmt3.setInt(1, tableId);
        pstmt3.setString(2, partition.getPartitionName());
        pstmt3.setString(3, partition.getPath());
        pstmt3.addBatch();
        pstmt3.clearParameters();

        // Insert partition keys
        for (int i = 0; i < partition.getPartitionKeysCount(); i++) {
          PartitionKeyProto partitionKey = partition.getPartitionKeys(i);
          pstmt4.setInt(1, tableId);
          pstmt4.setString(2, partition.getPartitionName());
          pstmt4.setInt(3, tableId);
          pstmt4.setString(4, partitionKey.getColumnName());
          pstmt4.setString(5, partitionKey.getPartitionValue());

          pstmt4.addBatch();
          pstmt4.clearParameters();
        }

        // Execute batch
        if (currentIndex >= lastIndex + batchSize && lastIndex != currentIndex) {
          pstmt1.executeBatch();
          pstmt1.clearBatch();
          pstmt2.executeBatch();
          pstmt2.clearBatch();
          pstmt3.executeBatch();
          pstmt3.clearBatch();
          pstmt4.executeBatch();
          pstmt4.clearBatch();
          lastIndex = currentIndex;
        }
      }

      // Execute existing batch queries
      if (lastIndex != currentIndex) {
        pstmt1.executeBatch();
        pstmt2.executeBatch();
        pstmt3.executeBatch();
        pstmt4.executeBatch();
      }

      if (conn != null) {
        conn.commit();
      }
    } catch (SQLException se) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException e) {
          LOG.error(e, e);
        }
      }
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt1);
      CatalogUtil.closeQuietly(pstmt2);
      CatalogUtil.closeQuietly(pstmt3);
      CatalogUtil.closeQuietly(pstmt4);
    }
  }

  @Override
  public void createIndex(final IndexDescProto proto)
      throws UndefinedDatabaseException, UndefinedTableException, DuplicateIndexException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    final String databaseName = proto.getTableIdentifier().getDatabaseName();
    final String tableName = CatalogUtil.extractSimpleName(proto.getTableIdentifier().getTableName());

    try {

      // indexes table
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);

      String sql = String.format("SELECT INDEX_NAME FROM %s WHERE DB_ID=? AND INDEX_NAME=?", TB_INDEXES);

      conn = getConnection();
      conn.setAutoCommit(false);

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, proto.getIndexName());
      ResultSet res = pstmt.executeQuery();
      if (res.next()) {
        throw new DuplicateIndexException(proto.getIndexName());
      }
      pstmt.close();
      res.close();

      sql = "INSERT INTO " + TB_INDEXES +
          " (" + COL_DATABASES_PK + ", " + COL_TABLES_PK + ", INDEX_NAME, " +
          "INDEX_TYPE, PATH, COLUMN_NAMES, DATA_TYPES, ORDERS, NULL_ORDERS, IS_UNIQUE, IS_CLUSTERED) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      final SortSpec[] keySortSpecs = new SortSpec[proto.getKeySortSpecsCount()];
      for (int i = 0; i < keySortSpecs.length; i++) {
        keySortSpecs[i] = new SortSpec(proto.getKeySortSpecs(i));
      }

      StringBuilder columnNamesBuilder = new StringBuilder();
      StringBuilder dataTypesBuilder= new StringBuilder();
      StringBuilder ordersBuilder = new StringBuilder();
      StringBuilder nullOrdersBuilder = new StringBuilder();
      for (SortSpec columnSpec : keySortSpecs) {
        // Since the key columns are always sorted in order of their occurrence position in the relation schema,
        // the concatenated name can be uniquely identified.
        columnNamesBuilder.append(columnSpec.getSortKey().getSimpleName()).append(",");
        dataTypesBuilder.append(columnSpec.getSortKey().getDataType().getType().name()).append(",");
        ordersBuilder.append(columnSpec.isAscending()).append(",");
        nullOrdersBuilder.append(columnSpec.isNullFirst()).append(",");
      }
      columnNamesBuilder.deleteCharAt(columnNamesBuilder.length()-1);
      dataTypesBuilder.deleteCharAt(dataTypesBuilder.length()-1);
      ordersBuilder.deleteCharAt(ordersBuilder.length() - 1);
      nullOrdersBuilder.deleteCharAt(nullOrdersBuilder.length()-1);

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.setString(3, proto.getIndexName()); // index name
      pstmt.setString(4, proto.getIndexMethod().toString()); // index type
      pstmt.setString(5, proto.getIndexPath()); // index path
      pstmt.setString(6, columnNamesBuilder.toString());
      pstmt.setString(7, dataTypesBuilder.toString());
      pstmt.setString(8, ordersBuilder.toString());
      pstmt.setString(9, nullOrdersBuilder.toString());
      pstmt.setBoolean(10, proto.hasIsUnique() && proto.getIsUnique());
      pstmt.setBoolean(11, proto.hasIsClustered() && proto.getIsClustered());
      pstmt.executeUpdate();
      conn.commit();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt);
    }
  }

  @Override
  public void dropIndex(String databaseName, final String indexName)
      throws UndefinedDatabaseException, UndefinedIndexException {
    Connection conn;
    PreparedStatement pstmt = null;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql = String.format("SELECT INDEX_NAME FROM %s WHERE %s=? AND INDEX_NAME=?", TB_INDEXES, COL_DATABASES_PK);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      ResultSet res = pstmt.executeQuery();
      if (!res.next()) {
        throw new UndefinedIndexException(CatalogUtil.buildFQName(databaseName, indexName));
      }
      pstmt.close();
      res.close();

      sql = "DELETE FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }


      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
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
        throw new TajoInternalError("Inconsistent data: no table corresponding to TID " + tableId);
      }
      return res.getString(1);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  public static String getDatabaseNameOfTable(Connection conn, int tid) throws SQLException {
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      pstmt =
          conn.prepareStatement("SELECT " + COL_DATABASES_PK + " FROM " + TB_TABLES + " WHERE " + COL_TABLES_PK + "=?");
      pstmt.setInt(1, tid);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new TajoInternalError("Inconsistent data: no table corresponding to TID " + tid);
      }
      int dbId = res.getInt(1);
      res.close();
      pstmt.close();

      pstmt = conn.prepareStatement("SELECT DB_NAME FROM " + TB_DATABASES + " WHERE " + COL_DATABASES_PK + "=?");
      pstmt.setInt(1, dbId);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new TajoInternalError("Inconsistent data: no database corresponding to DB_ID " + dbId);
      }

      return res.getString(1);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  final static String GET_INDEXES_SQL =
      "SELECT * FROM " + TB_INDEXES;

  @Override
  public IndexDescProto getIndexByName(String databaseName, final String indexName)
      throws UndefinedDatabaseException, UndefinedIndexException {

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
        throw new UndefinedIndexException(indexName);
      }
      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      String tableName = getTableName(conn, res.getInt(COL_TABLES_PK));
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      resultToIndexDescProtoBuilder(CatalogUtil.buildFQName(databaseName, tableName), builder, res);

      try {
        builder.setTargetRelationSchema(getTable(databaseName, tableName).getSchema());
      } catch (UndefinedTableException e) {
        throw new TajoInternalError(
            "Inconsistent table and index information: table " + tableName + " does not exists.");
      }

      proto = builder.build();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return proto;
  }

  @Override
  public IndexDescProto getIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedIndexException {

    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);
      TableDescProto tableDescProto = getTable(databaseName, tableName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND " +
          COL_TABLES_PK + "=? AND COLUMN_NAMES=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      // Since the column names in the unified name are always sorted
      // in order of occurrence position in the relation schema,
      // they can be uniquely identified.
      String unifiedName = CatalogUtil.getUnifiedSimpleColumnName(new Schema(tableDescProto.getSchema()), columnNames);
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.setString(3, unifiedName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new UndefinedIndexException(unifiedName);
      }

      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      resultToIndexDescProtoBuilder(CatalogUtil.buildFQName(databaseName, tableName), builder, res);
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      builder.setTargetRelationSchema(tableDescProto.getSchema());
      proto = builder.build();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return proto;
  }

  @Override
  public boolean existIndexByName(String databaseName, final String indexName) throws UndefinedDatabaseException {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }

  @Override
  public boolean existIndexByColumns(String databaseName, String tableName, String[] columnNames)
      throws UndefinedDatabaseException, UndefinedTableException {

    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    boolean exist = false;

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, tableName);
      Schema relationSchema = new Schema(getTable(databaseName, tableName).getSchema());

      String sql =
          "SELECT " + COL_INDEXES_PK + " FROM " + TB_INDEXES +
              " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_PK + "=? AND COLUMN_NAMES=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      // Since the column names in the unified name are always sorted
      // in order of occurrence position in the relation schema,
      // they can be uniquely identified.
      String unifiedName = CatalogUtil.getUnifiedSimpleColumnName(new Schema(relationSchema), columnNames);
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.setString(3, unifiedName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
    return exist;
  }

  @Override
  public List<String> getAllIndexNamesByTable(final String databaseName, final String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

    ResultSet res = null;
    PreparedStatement pstmt = null;
    final List<String> indexNames = new ArrayList<String>();

    try {
      final int databaseId = getDatabaseId(databaseName);
      final int tableId = getTableId(databaseId, databaseName, tableName);

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
        indexNames.add(res.getString("index_name"));
      }
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return indexNames;
  }

  @Override
  public boolean existIndexesByTable(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      final int databaseId = getDatabaseId(databaseName);
      final int tableId = getTableId(databaseId, databaseName, tableName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_PK + "=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      res = pstmt.executeQuery();

      return res.next();
    } catch (SQLException se) {
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }
  }

  @Override
  public List<IndexDescProto> getAllIndexes() throws UndefinedDatabaseException {
    List<IndexDescProto> indexDescProtos = TUtil.newList();
    for (String databaseName : getAllDatabaseNames()) {
      for (String tableName : getAllTableNames(databaseName)) {
        try {
          for (String indexName: getAllIndexNamesByTable(databaseName, tableName)) {
            indexDescProtos.add(getIndexByName(databaseName, indexName));
          }
        } catch (UndefinedTableException e) {
          LOG.warn(e);
        } catch (UndefinedIndexException e) {
          throw new TajoInternalError(e);
        }
      }
    }
    return indexDescProtos;
  }

  private void resultToIndexDescProtoBuilder(final String qualifier,
                                             final IndexDescProto.Builder builder,
                                             final ResultSet res) throws SQLException {
    builder.setIndexName(res.getString("index_name"));
    builder.setIndexMethod(getIndexMethod(res.getString("index_type").trim()));
    builder.setIndexPath(res.getString("path"));
    String[] columnNames, dataTypes, orders, nullOrders;
    columnNames = res.getString("column_names").trim().split(",");
    dataTypes = res.getString("data_types").trim().split(",");
    orders = res.getString("orders").trim().split(",");
    nullOrders = res.getString("null_orders").trim().split(",");
    int columnNum = columnNames.length;
    for (int i = 0; i < columnNum; i++) {
      SortSpecProto.Builder colSpecBuilder = SortSpecProto.newBuilder();
      colSpecBuilder.setColumn(ColumnProto.newBuilder().setName(CatalogUtil.buildFQName(qualifier, columnNames[i]))
          .setDataType(CatalogUtil.newSimpleDataType(getDataType(dataTypes[i]))).build());
      colSpecBuilder.setAscending(orders[i].equals("true"));
      colSpecBuilder.setNullFirst(nullOrders[i].equals("true"));
      builder.addKeySortSpecs(colSpecBuilder.build());
    }
    builder.setIsUnique(res.getBoolean("is_unique"));
    builder.setIsClustered(res.getBoolean("is_clustered"));
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
      throws SQLException {

    CatalogProtos.PartitionMethodProto.Builder partBuilder;
    try {
      partBuilder = CatalogProtos.PartitionMethodProto.newBuilder();
      partBuilder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      partBuilder.setPartitionType(CatalogProtos.PartitionType.valueOf(res.getString("partition_type")));
      partBuilder.setExpression(res.getString("expression"));
      partBuilder.setExpressionSchema(SchemaProto.parseFrom(res.getBytes("expression_schema")));
    } catch (InvalidProtocolBufferException e) {
      throw new TajoInternalError(e);
    }
    return partBuilder.build();
  }


  public void close() {
    CatalogUtil.closeQuietly(conn);
    LOG.info("Close database (" + catalogUri + ")");
  }

  @Override
  public final void addFunction(final FunctionDesc func) {
    // TODO - not implemented yet
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) {
    // TODO - not implemented yet
  }

  @Override
  public final void existFunction(final FunctionDesc func) {
    // TODO - not implemented yet
  }

  @Override
  public final List<String> getAllFunctionNames() {
    // TODO - not implemented yet
    return null;
  }

  private boolean existColumn(final int tableId, final String columnName) {
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
      throw new TajoInternalError(se);
    } finally {
      CatalogUtil.closeQuietly(pstmt, res);
    }

    return exist;
  }
}
