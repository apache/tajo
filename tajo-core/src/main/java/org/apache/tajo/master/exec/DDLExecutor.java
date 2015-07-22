/*
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

package org.apache.tajo.master.exec;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.AlterTableOpType;
import org.apache.tajo.algebra.AlterTablespaceSetType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;

/**
 * Executor for DDL statements. They are executed on only TajoMaster.
 */
public class DDLExecutor {
  private static final Log LOG = LogFactory.getLog(DDLExecutor.class);

  private final TajoMaster.MasterContext context;
  private final CatalogService catalog;

  public DDLExecutor(TajoMaster.MasterContext context) {
    this.context = context;
    this.catalog = context.getCatalog();
  }

  public boolean execute(QueryContext queryContext, LogicalPlan plan) throws IOException {
    LogicalNode root = ((LogicalRootNode) plan.getRootBlock().getRoot()).getChild();

    switch (root.getType()) {

      case ALTER_TABLESPACE:
        AlterTablespaceNode alterTablespace = (AlterTablespaceNode) root;
        alterTablespace(context, queryContext, alterTablespace);
        return true;


      case CREATE_DATABASE:
        CreateDatabaseNode createDatabase = (CreateDatabaseNode) root;
        createDatabase(queryContext, createDatabase.getDatabaseName(), null, createDatabase.isIfNotExists());
        return true;
      case DROP_DATABASE:
        DropDatabaseNode dropDatabaseNode = (DropDatabaseNode) root;
        dropDatabase(queryContext, dropDatabaseNode.getDatabaseName(), dropDatabaseNode.isIfExists());
        return true;


      case CREATE_TABLE:
        CreateTableNode createTable = (CreateTableNode) root;
        createTable(queryContext, createTable, createTable.isIfNotExists());
        return true;
      case DROP_TABLE:
        DropTableNode dropTable = (DropTableNode) root;
        dropTable(queryContext, dropTable.getTableName(), dropTable.isIfExists(), dropTable.isPurge());
        return true;
      case TRUNCATE_TABLE:
        TruncateTableNode truncateTable = (TruncateTableNode) root;
        truncateTable(queryContext, truncateTable);
        return true;

      case ALTER_TABLE:
        AlterTableNode alterTable = (AlterTableNode) root;
        alterTable(context, queryContext, alterTable);
        return true;

      case CREATE_INDEX:
        CreateIndexNode createIndex = (CreateIndexNode) root;
        createIndex(queryContext, createIndex);
        return true;

      case DROP_INDEX:
        DropIndexNode dropIndexNode = (DropIndexNode) root;
        dropIndex(queryContext, dropIndexNode);
        return true;

    default:
      throw new InternalError("updateQuery cannot handle such query: \n" + root.toJson());
    }
  }

  public void createIndex(final QueryContext queryContext, final CreateIndexNode createIndexNode) {
    String databaseName, simpleIndexName, qualifiedIndexName;
    if (CatalogUtil.isFQTableName(createIndexNode.getIndexName())) {
      String [] splits = CatalogUtil.splitFQTableName(createIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
      qualifiedIndexName = createIndexNode.getIndexName();
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = createIndexNode.getIndexName();
      qualifiedIndexName = CatalogUtil.buildFQName(databaseName, simpleIndexName);
    }

    if (catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new DuplicateIndexException(simpleIndexName);
    }

    ScanNode scanNode = PlannerUtil.findTopNode(createIndexNode, NodeType.SCAN);
    if (scanNode == null) {
      throw new InternalError("Cannot find the table of the relation");
    }

    IndexDesc indexDesc = new IndexDesc(databaseName, CatalogUtil.extractSimpleName(scanNode.getTableName()),
        simpleIndexName, createIndexNode.getIndexPath(),
        createIndexNode.getKeySortSpecs(), createIndexNode.getIndexMethod(),
        createIndexNode.isUnique(), false, scanNode.getLogicalSchema());

    if (catalog.createIndex(indexDesc)) {
      LOG.info("Index " + qualifiedIndexName + " is created for the table " + scanNode.getTableName() + ".");
    } else {
      LOG.info("Index creation " + qualifiedIndexName + " is failed.");
      throw new TajoInternalError("Cannot create index \"" + qualifiedIndexName + "\".");
    }
  }

  public void dropIndex(final QueryContext queryContext, final DropIndexNode dropIndexNode) {
    String databaseName, simpleIndexName;
    if (CatalogUtil.isFQTableName(dropIndexNode.getIndexName())) {
      String [] splits = CatalogUtil.splitFQTableName(dropIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = dropIndexNode.getIndexName();
    }

    if (!catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new UndefinedIndexException(simpleIndexName);
    }

    IndexDesc desc = catalog.getIndexByName(databaseName, simpleIndexName);

    if (!catalog.dropIndex(databaseName, simpleIndexName)) {
      LOG.info("Cannot drop index \"" + simpleIndexName + "\".");
      throw new TajoInternalError("Cannot drop index \"" + simpleIndexName + "\".");
    }

    Path indexPath = new Path(desc.getIndexPath());
    try {
      FileSystem fs = indexPath.getFileSystem(context.getConf());
      fs.delete(indexPath, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }

    LOG.info("Index " + simpleIndexName + " is dropped.");
  }

  /**
   * Alter a given table
   */
  public static void alterTablespace(final TajoMaster.MasterContext context, final QueryContext queryContext,
                                     final AlterTablespaceNode alterTablespace) {

    final CatalogService catalog = context.getCatalog();
    final String spaceName = alterTablespace.getTablespaceName();

    AlterTablespaceProto.Builder builder = AlterTablespaceProto.newBuilder();
    builder.setSpaceName(spaceName);
    if (alterTablespace.getSetType() == AlterTablespaceSetType.LOCATION) {
      AlterTablespaceProto.AlterTablespaceCommand.Builder commandBuilder =
          AlterTablespaceProto.AlterTablespaceCommand.newBuilder();
      commandBuilder.setType(AlterTablespaceProto.AlterTablespaceType.LOCATION);
      commandBuilder.setLocation(AlterTablespaceProto.SetLocation.newBuilder().setUri(alterTablespace.getLocation()));
      commandBuilder.build();
      builder.addCommand(commandBuilder);
    } else {
      throw new RuntimeException("This 'ALTER TABLESPACE' is not supported yet.");
    }

    catalog.alterTablespace(builder.build());
  }

  //--------------------------------------------------------------------------

  // Database Section
  //--------------------------------------------------------------------------
  public boolean createDatabase(@Nullable QueryContext queryContext, String databaseName,
                                @Nullable String tablespace,
                                boolean ifNotExists) throws IOException {

    String tablespaceName;
    if (tablespace == null) {
      tablespaceName = DEFAULT_TABLESPACE_NAME;
    } else {
      tablespaceName = tablespace;
    }

    // CREATE DATABASE IF NOT EXISTS
    boolean exists = catalog.existDatabase(databaseName);
    if (exists) {
      if (ifNotExists) {
        LOG.info("database \"" + databaseName + "\" is already exists." );
        return true;
      } else {
        throw new DuplicateDatabaseException(databaseName);
      }
    }

    if (catalog.createDatabase(databaseName, tablespaceName)) {
      String normalized = databaseName;
      Path databaseDir = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR), normalized);
      FileSystem fs = databaseDir.getFileSystem(context.getConf());
      fs.mkdirs(databaseDir);
    }

    return true;
  }

  public boolean dropDatabase(QueryContext queryContext, String databaseName, boolean ifExists) {
    boolean exists = catalog.existDatabase(databaseName);
    if(!exists) {
      if (ifExists) { // DROP DATABASE IF EXISTS
        LOG.info("database \"" + databaseName + "\" does not exists." );
        return true;
      } else { // Otherwise, it causes an exception.
        throw new UndefinedDatabaseException(databaseName);
      }
    }

    if (queryContext.getCurrentDatabase().equals(databaseName)) {
      throw new RuntimeException("ERROR: Cannot drop the current open database");
    }

    boolean result = catalog.dropDatabase(databaseName);
    LOG.info("database " + databaseName + " is dropped.");
    return result;
  }

  //--------------------------------------------------------------------------

  // Table Section
  //--------------------------------------------------------------------------
  private TableDesc createTable(QueryContext queryContext, CreateTableNode createTable, boolean ifNotExists)
      throws IOException {

    TableMeta meta;
    if (createTable.hasOptions()) {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType(), createTable.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(createTable.getStorageType());
    }

    if(PlannerUtil.isFileStorageType(createTable.getStorageType()) && createTable.isExternal()){
      Preconditions.checkState(createTable.hasUri(), "ERROR: LOCATION must be given.");
    }

    return createTable(
        queryContext,
        createTable.getTableName(),
        createTable.getTableSpaceName(),
        createTable.getStorageType(),createTable.getTableSchema(),
        meta,
        createTable.getUri(),
        createTable.isExternal(),
        createTable.getPartitionMethod(),
        ifNotExists);
  }

  public TableDesc createTable(QueryContext queryContext,
                               String tableName,
                               @Nullable String tableSpaceName,
                               @Nullable String storeType,
                               Schema schema,
                               TableMeta meta,
                               @Nullable URI uri,
                               boolean isExternal,
                               @Nullable PartitionMethodDesc partitionDesc,
                               boolean ifNotExists) throws IOException {

    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String [] splitted = CatalogUtil.splitFQTableName(tableName);
      databaseName = splitted[0];
      simpleTableName = splitted[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    boolean exists = catalog.existsTable(databaseName, simpleTableName);

    if (exists) {
      if (ifNotExists) {
        LOG.info("relation \"" + qualifiedName + "\" is already exists." );
        return catalog.getTableDesc(databaseName, simpleTableName);
      } else {
        throw new DuplicateTableException(qualifiedName);
      }
    }

    Tablespace tableSpace;
    if (tableSpaceName != null) {
      Optional<Tablespace> ts = (Optional<Tablespace>) TablespaceManager.getByName(tableSpaceName);
      if (ts.isPresent()) {
        tableSpace = ts.get();
      } else {
        throw new IOException("Tablespace '" + tableSpaceName + "' does not exist");
      }
    } else if (uri != null) {
      Optional<Tablespace> ts = TablespaceManager.get(uri);
      if (ts.isPresent()) {
        tableSpace = ts.get();
      } else {
        throw new IOException("Unknown tablespace URI: " + uri);
      }
    } else {
      tableSpace = TablespaceManager.getDefault();
    }

    TableDesc desc;
    URI tableUri = isExternal ? uri : tableSpace.getTableUri(databaseName, simpleTableName);
    desc = new TableDesc(qualifiedName, schema, meta, tableUri, isExternal);

    if (partitionDesc != null) {
      desc.setPartitionMethod(partitionDesc);
    }

    tableSpace.createTable(desc, ifNotExists);

    if (catalog.createTable(desc)) {
      LOG.info("Table " + desc.getName() + " is created (" + desc.getStats().getNumBytes() + ")");
      return desc;
    } else {
      LOG.info("Table creation " + tableName + " is failed.");
      throw new TajoInternalError("Cannot create table \"" + tableName + "\"");
    }
  }

  /**
   * Drop a given named table
   *
   * @param tableName to be dropped
   * @param purge     Remove all data if purge is true.
   */
  public boolean dropTable(QueryContext queryContext, String tableName, boolean ifExists, boolean purge) {

    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String [] splitted = CatalogUtil.splitFQTableName(tableName);
      databaseName = splitted[0];
      simpleTableName = splitted[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    boolean exists = catalog.existsTable(qualifiedName);
    if(!exists) {
      if (ifExists) { // DROP TABLE IF EXISTS
        LOG.info("relation \"" + qualifiedName + "\" is already exists." );
        return true;
      } else { // Otherwise, it causes an exception.
        throw new UndefinedTableException(qualifiedName);
      }
    }

    TableDesc tableDesc = catalog.getTableDesc(qualifiedName);
    catalog.dropTable(qualifiedName);

    if (purge) {
      try {
        TablespaceManager.get(tableDesc.getUri()).get().purgeTable(tableDesc);
      } catch (IOException e) {
        throw new InternalError(e.getMessage());
      }
    }

    LOG.info(String.format("relation \"%s\" is " + (purge ? " purged." : " dropped."), qualifiedName));
    return true;
  }

  /**
   * Truncate table a given table
   */
  public void truncateTable(final QueryContext queryContext, final TruncateTableNode truncateTableNode)
      throws IOException {
    List<String> tableNames = truncateTableNode.getTableNames();
    final CatalogService catalog = context.getCatalog();

    String databaseName;
    String simpleTableName;

    List<TableDesc> tableDescList = new ArrayList<TableDesc>();
    for (String eachTableName: tableNames) {
      if (CatalogUtil.isFQTableName(eachTableName)) {
        String[] split = CatalogUtil.splitFQTableName(eachTableName);
        databaseName = split[0];
        simpleTableName = split[1];
      } else {
        databaseName = queryContext.getCurrentDatabase();
        simpleTableName = eachTableName;
      }
      final String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

      if (!catalog.existsTable(databaseName, simpleTableName)) {
        throw new UndefinedTableException(qualifiedName);
      }

      Path warehousePath = new Path(TajoConf.getWarehouseDir(context.getConf()), databaseName);
      TableDesc tableDesc = catalog.getTableDesc(databaseName, simpleTableName);
      Path tablePath = new Path(tableDesc.getUri());
      if (tablePath.getParent() == null ||
          !tablePath.getParent().toUri().getPath().equals(warehousePath.toUri().getPath())) {
        throw new IOException("Can't truncate external table:" + eachTableName + ", data dir=" + tablePath +
            ", warehouse dir=" + warehousePath);
      }
      tableDescList.add(tableDesc);
    }

    for (TableDesc eachTable: tableDescList) {
      Path path = new Path(eachTable.getUri());
      LOG.info("Truncate table: " + eachTable.getName() + ", delete all data files in " + path);
      FileSystem fs = path.getFileSystem(context.getConf());

      FileStatus[] files = fs.listStatus(path);
      if (files != null) {
        for (FileStatus eachFile: files) {
          fs.delete(eachFile.getPath(), true);
        }
      }
    }
  }


  /**
   * Execute alter table statement using catalog api.
   *
   * @param context
   * @param queryContext
   * @param alterTable
   * @throws IOException
   */
  public void alterTable(TajoMaster.MasterContext context, final QueryContext queryContext,
                         final AlterTableNode alterTable) throws IOException {
    final CatalogService catalog = context.getCatalog();
    final String tableName = alterTable.getTableName();

    String databaseName;
    String simpleTableName;
    if (CatalogUtil.isFQTableName(tableName)) {
      String[] split = CatalogUtil.splitFQTableName(tableName);
      databaseName = split[0];
      simpleTableName = split[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    final String qualifiedName = CatalogUtil.buildFQName(databaseName, simpleTableName);

    if (!catalog.existsTable(databaseName, simpleTableName)) {
      throw new UndefinedTableException(qualifiedName);
    }

    Path partitionPath = null;
    TableDesc desc = null;
    Pair<List<PartitionKeyProto>, String> pair = null;
    CatalogProtos.PartitionDescProto partitionDescProto = null;

    if (alterTable.getAlterTableOpType() == AlterTableOpType.RENAME_TABLE
      || alterTable.getAlterTableOpType() == AlterTableOpType.ADD_PARTITION
      || alterTable.getAlterTableOpType() == AlterTableOpType.DROP_PARTITION) {
      desc = catalog.getTableDesc(databaseName, simpleTableName);
    }

    switch (alterTable.getAlterTableOpType()) {
    case RENAME_TABLE:
      if (!catalog.existsTable(databaseName, simpleTableName)) {
        throw new UndefinedTableException(alterTable.getTableName());
      }
      if (catalog.existsTable(databaseName, alterTable.getNewTableName())) {
        throw new DuplicateTableException(alterTable.getNewTableName());
      }

      if (!desc.isExternal()) { // if the table is the managed table
        Path oldPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
            databaseName, simpleTableName);
        Path newPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
            databaseName, alterTable.getNewTableName());
        FileSystem fs = oldPath.getFileSystem(context.getConf());

        if (!fs.exists(oldPath)) {
          throw new IOException("No such a table directory: " + oldPath);
        }
        if (fs.exists(newPath)) {
          throw new IOException("Already table directory exists: " + newPath);
        }

        fs.rename(oldPath, newPath);
      }
      catalog.alterTable(CatalogUtil.renameTable(qualifiedName, alterTable.getNewTableName(),
          AlterTableType.RENAME_TABLE));
      break;
    case RENAME_COLUMN:
      if (ensureColumnExistance(qualifiedName, alterTable.getNewColumnName())) {
        throw new DuplicateColumnException(alterTable.getNewColumnName());
      }
      catalog.alterTable(CatalogUtil.renameColumn(qualifiedName, alterTable.getColumnName(),
          alterTable.getNewColumnName(), AlterTableType.RENAME_COLUMN));
      break;
    case ADD_COLUMN:
      if (ensureColumnExistance(qualifiedName, alterTable.getAddNewColumn().getSimpleName())) {
        throw new DuplicateColumnException(alterTable.getAddNewColumn().getSimpleName());
      }
      catalog.alterTable(CatalogUtil.addNewColumn(qualifiedName, alterTable.getAddNewColumn(), AlterTableType.ADD_COLUMN));
      break;
    case SET_PROPERTY:
      catalog.alterTable(CatalogUtil.setProperty(qualifiedName, alterTable.getProperties(), AlterTableType.SET_PROPERTY));
      break;
    case ADD_PARTITION:
      pair = CatalogUtil.getPartitionKeyNamePair(alterTable.getPartitionColumns(), alterTable.getPartitionValues());
      ensureColumnPartitionKeys(qualifiedName, alterTable.getPartitionColumns());

      // checking a duplicated partition
      boolean duplicatedPartition = true;
      try {
        catalog.getPartition(databaseName, simpleTableName, pair.getSecond());
      } catch (UndefinedPartitionException npe) {
        duplicatedPartition = false;
      }
      if (duplicatedPartition) {
        throw new DuplicatePartitionException(pair.getSecond());
      }

      if (alterTable.getLocation() != null) {
        partitionPath = new Path(alterTable.getLocation());
      } else {
        // If location is not specified, the partition's location will be set using the table location.
        partitionPath = new Path(desc.getUri().toString(), pair.getSecond());
        alterTable.setLocation(partitionPath.toString());
      }

      FileSystem fs = partitionPath.getFileSystem(context.getConf());

      // If there is a directory which was assumed to be a partitioned directory and users don't input another
      // location, this will throw exception.
      Path assumedDirectory = new Path(desc.getUri().toString(), pair.getSecond());

      if (fs.exists(assumedDirectory) && !assumedDirectory.equals(partitionPath)) {
        throw new AlreadyExistsAssumedPartitionDirectoryException(assumedDirectory.toString());
      }

      catalog.alterTable(CatalogUtil.addOrDropPartition(qualifiedName, alterTable.getPartitionColumns(),
        alterTable.getPartitionValues(), alterTable.getLocation(), AlterTableType.ADD_PARTITION));

      // If the partition's path doesn't exist, this would make the directory by force.
      if (!fs.exists(partitionPath)) {
        fs.mkdirs(partitionPath);
      }
      break;
    case DROP_PARTITION:
      ensureColumnPartitionKeys(qualifiedName, alterTable.getPartitionColumns());
      pair = CatalogUtil.getPartitionKeyNamePair(alterTable.getPartitionColumns(), alterTable.getPartitionValues());
      partitionDescProto = catalog.getPartition(databaseName, simpleTableName, pair.getSecond());

      if (partitionDescProto == null) {
        throw new NoSuchPartitionException(tableName, pair.getSecond());
      }

      catalog.alterTable(CatalogUtil.addOrDropPartition(qualifiedName, alterTable.getPartitionColumns(),
        alterTable.getPartitionValues(), alterTable.getLocation(), AlterTableType.DROP_PARTITION));

      // When dropping partition on an managed table, the data will be delete from file system.
      if (!desc.isExternal()) {
        deletePartitionPath(partitionDescProto);
      } else {
        // When dropping partition on an external table, the data in the table will NOT be deleted from the file
        // system. But if PURGE is specified, the partition data will be deleted.
        if (alterTable.isPurge()) {
          deletePartitionPath(partitionDescProto);
        }
      }
      break;
    default:
      //TODO
    }
  }

  private void deletePartitionPath(CatalogProtos.PartitionDescProto partitionDescProto) throws IOException {
    Path partitionPath = new Path(partitionDescProto.getPath());
    FileSystem fs = partitionPath.getFileSystem(context.getConf());
    if (fs.exists(partitionPath)) {
      fs.delete(partitionPath, true);
    }
  }

  private boolean ensureColumnPartitionKeys(String tableName, String[] columnNames) {
    for(String columnName : columnNames) {
      if (!ensureColumnPartitionKeys(tableName, columnName)) {
        throw new NoSuchPartitionKeyException(tableName, columnName);
      }
    }
    return true;
  }

  private boolean ensureColumnPartitionKeys(String tableName, String columnName) {
    final TableDesc tableDesc = catalog.getTableDesc(tableName);
    if (tableDesc.getPartitionMethod().getExpressionSchema().contains(columnName)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean ensureColumnExistance(String tableName, String columnName) {
    final TableDesc tableDesc = catalog.getTableDesc(tableName);
    return tableDesc.getSchema().containsByName(columnName);
  }
}
