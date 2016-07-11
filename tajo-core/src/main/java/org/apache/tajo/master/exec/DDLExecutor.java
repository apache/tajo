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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.tajo.algebra.AlterTableOpType;
import org.apache.tajo.algebra.AlterTablespaceSetType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.*;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;

import java.io.File;
import java.io.IOException;
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

  private final CreateTableExecutor createTableExecutor;

  public DDLExecutor(TajoMaster.MasterContext context) {
    this.context = context;
    this.catalog = context.getCatalog();

    createTableExecutor = new CreateTableExecutor(this.context);
  }

  public CreateTableExecutor getCreateTableExecutor() {
    return createTableExecutor;
  }
  
  public boolean execute(QueryContext queryContext, LogicalPlan plan)
      throws IOException, TajoException {

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
      createTableExecutor.create(queryContext, createTable, createTable.isIfNotExists());
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

  public void createIndex(final QueryContext queryContext, final CreateIndexNode createIndexNode)
      throws DuplicateIndexException, UndefinedTableException, UndefinedDatabaseException {

    String databaseName, simpleIndexName, qualifiedIndexName;
    if (IdentifierUtil.isFQTableName(createIndexNode.getIndexName())) {
      String[] splits = IdentifierUtil.splitFQTableName(createIndexNode.getIndexName());
      databaseName = splits[0];
      simpleIndexName = splits[1];
      qualifiedIndexName = createIndexNode.getIndexName();
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleIndexName = createIndexNode.getIndexName();
      qualifiedIndexName = IdentifierUtil.buildFQName(databaseName, simpleIndexName);
    }

    if (catalog.existIndexByName(databaseName, simpleIndexName)) {
      throw new DuplicateIndexException(simpleIndexName);
    }

    ScanNode scanNode = PlannerUtil.findTopNode(createIndexNode, NodeType.SCAN);
    if (scanNode == null) {
      throw new InternalError("Cannot find the table of the relation");
    }

    IndexDesc indexDesc = new IndexDesc(databaseName, IdentifierUtil.extractSimpleName(scanNode.getTableName()),
        simpleIndexName, createIndexNode.getIndexPath(),
        createIndexNode.getKeySortSpecs(), createIndexNode.getIndexMethod(),
        createIndexNode.isUnique(), false, scanNode.getLogicalSchema());
    catalog.createIndex(indexDesc);
    LOG.info("Index " + qualifiedIndexName + " is created for the table " + scanNode.getTableName() + ".");
  }

  public void dropIndex(final QueryContext queryContext, final DropIndexNode dropIndexNode)
      throws UndefinedIndexException, UndefinedDatabaseException {

    String databaseName, simpleIndexName;
    if (IdentifierUtil.isFQTableName(dropIndexNode.getIndexName())) {
      String[] splits = IdentifierUtil.splitFQTableName(dropIndexNode.getIndexName());
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
    catalog.dropIndex(databaseName, simpleIndexName);

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
                                     final AlterTablespaceNode alterTablespace)
      throws UndefinedTablespaceException, InsufficientPrivilegeException {

    final CatalogService catalog = context.getCatalog();
    final String spaceName = alterTablespace.getTablespaceName();

    AlterTablespaceProto.Builder builder = AlterTablespaceProto.newBuilder();
    builder.setSpaceName(spaceName);
    if (alterTablespace.getSetType() == AlterTablespaceSetType.LOCATION) {
      AlterTablespaceProto.AlterTablespaceCommand.Builder commandBuilder =
          AlterTablespaceProto.AlterTablespaceCommand.newBuilder();
      commandBuilder.setType(AlterTablespaceProto.AlterTablespaceType.LOCATION);
      commandBuilder.setLocation(alterTablespace.getLocation());
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
  public void createDatabase(@Nullable QueryContext queryContext, String databaseName,
                                @Nullable String tablespace,
                                boolean ifNotExists) throws IOException, DuplicateDatabaseException {

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
        LOG.info("database \"" + databaseName + "\" is already exists.");
        return;
      } else {
        throw new DuplicateDatabaseException(databaseName);
      }
    }

    String normalized = databaseName;
    Path databaseDir = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR), normalized);
    FileSystem fs = databaseDir.getFileSystem(context.getConf());
    fs.mkdirs(databaseDir);
    catalog.createDatabase(databaseName, tablespaceName);
    LOG.info("database \"" + databaseName + "\" created.");
  }

  public void dropDatabase(QueryContext queryContext, String databaseName, boolean ifExists)
      throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

    boolean exists = catalog.existDatabase(databaseName);
    if (!exists) {
      if (ifExists) { // DROP DATABASE IF EXISTS
        LOG.info("database \"" + databaseName + "\" does not exists.");
        return;
      } else { // Otherwise, it causes an exception.
        throw new UndefinedDatabaseException(databaseName);
      }
    }

    if (queryContext.getCurrentDatabase().equals(databaseName)) {
      throw new CannotDropCurrentDatabaseException();
    }

    catalog.dropDatabase(databaseName);
  }

  //--------------------------------------------------------------------------

  // Table Section
  //--------------------------------------------------------------------------

  /**
   * Drop a given named table
   *
   * @param tableName to be dropped
   * @param purge     Remove all data if purge is true.
   */
  public void dropTable(QueryContext queryContext, String tableName, boolean ifExists, boolean purge)
      throws TajoException {

    String databaseName;
    String simpleTableName;
    if (IdentifierUtil.isFQTableName(tableName)) {
      String[] splitted = IdentifierUtil.splitFQTableName(tableName);
      databaseName = splitted[0];
      simpleTableName = splitted[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    String qualifiedName = IdentifierUtil.buildFQName(databaseName, simpleTableName);

    boolean exists = catalog.existsTable(qualifiedName);
    if (!exists) {
      if (ifExists) { // DROP TABLE IF EXISTS
        LOG.info("relation \"" + qualifiedName + "\" is already exists.");
        return;
      } else { // Otherwise, it causes an exception.
        throw new UndefinedTableException(qualifiedName);
      }
    }

    TableDesc tableDesc = catalog.getTableDesc(qualifiedName);
    catalog.dropTable(qualifiedName);

    if (purge) {
      try {
        TablespaceManager.get(tableDesc.getUri()).purgeTable(tableDesc);
      } catch (IOException e) {
        throw new InternalError(e.getMessage());
      }
    }
    LOG.info(String.format("relation \"%s\" is " + (purge ? " purged." : " dropped."), qualifiedName));
  }

  /**
   * Truncate table a given table
   */
  public void truncateTable(final QueryContext queryContext, final TruncateTableNode truncateTableNode)
      throws IOException, UndefinedTableException {

    List<String> tableNames = truncateTableNode.getTableNames();
    final CatalogService catalog = context.getCatalog();

    String databaseName;
    String simpleTableName;

    List<TableDesc> tableDescList = new ArrayList<>();
    for (String eachTableName : tableNames) {
      if (IdentifierUtil.isFQTableName(eachTableName)) {
        String[] split = IdentifierUtil.splitFQTableName(eachTableName);
        databaseName = split[0];
        simpleTableName = split[1];
      } else {
        databaseName = queryContext.getCurrentDatabase();
        simpleTableName = eachTableName;
      }
      final String qualifiedName = IdentifierUtil.buildFQName(databaseName, simpleTableName);

      if (!catalog.existsTable(databaseName, simpleTableName)) {
        throw new UndefinedTableException(qualifiedName);
      }

      // only file-based tablespace is supported yet.
      TableDesc tableDesc = catalog.getTableDesc(databaseName, simpleTableName);

      if (tableDesc.isExternal()) {
        throw new TajoRuntimeException(
            new UnsupportedException("table truncation on an external table '" + eachTableName + "'"));
      }

      Tablespace space = TablespaceManager.get(tableDesc.getUri());

      if (space instanceof FileTablespace) {
        tableDescList.add(tableDesc);
      } else {
        throw new TajoRuntimeException(
            new UnsupportedException("table truncation on " + space.getName() + " storage"));
      }
    }

    for (TableDesc eachTable : tableDescList) {
      Path path = new Path(eachTable.getUri());
      LOG.info("Truncate table: " + eachTable.getName() + ", delete all data files in " + path);
      FileSystem fs = path.getFileSystem(context.getConf());

      FileStatus[] files = fs.listStatus(path);
      if (files != null) {
        for (FileStatus eachFile : files) {
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
                         final AlterTableNode alterTable)
      throws IOException, TajoException {

    final CatalogService catalog = context.getCatalog();
    final String tableName = alterTable.getTableName();

    String databaseName;
    String simpleTableName;
    if (IdentifierUtil.isFQTableName(tableName)) {
      String[] split = IdentifierUtil.splitFQTableName(tableName);
      databaseName = split[0];
      simpleTableName = split[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }
    final String qualifiedName = IdentifierUtil.buildFQName(databaseName, simpleTableName);

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

      Path newPath = null;
      if (!desc.isExternal()) { // if the table is the managed table
        Path oldPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
            databaseName, simpleTableName);
        newPath = StorageUtil.concatPath(context.getConf().getVar(TajoConf.ConfVars.WAREHOUSE_DIR),
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
          newPath));
      break;
    case RENAME_COLUMN:
      if (ensureColumnExistance(qualifiedName, alterTable.getNewColumnName())) {
        throw new DuplicateColumnException(alterTable.getNewColumnName());
      }
      catalog.alterTable(CatalogUtil.renameColumn(qualifiedName, alterTable.getColumnName(),
          alterTable.getNewColumnName()));
      break;
    case ADD_COLUMN:
      if (ensureColumnExistance(qualifiedName, alterTable.getAddNewColumn().getSimpleName())) {
        throw new DuplicateColumnException(alterTable.getAddNewColumn().getSimpleName());
      }
      catalog.alterTable(CatalogUtil.addNewColumn(qualifiedName, alterTable.getAddNewColumn()));
      break;
    case SET_PROPERTY:
      catalog.alterTable(CatalogUtil.setProperty(qualifiedName, alterTable.getProperties()));
      break;
    case UNSET_PROPERTY:
      catalog.alterTable(CatalogUtil.unsetProperty(qualifiedName, alterTable.getUnsetPropertyKeys()));
      break;
    case ADD_PARTITION:
      pair = CatalogUtil.getPartitionKeyNamePair(alterTable.getPartitionColumns(), alterTable.getPartitionValues());
      ensureColumnPartitionKeys(qualifiedName, alterTable.getPartitionColumns());

      // checking a duplicated partition
      boolean duplicatedPartition = true;
      try {
        catalog.getPartition(databaseName, simpleTableName, pair.getSecond());
      } catch (UndefinedPartitionException e) {
        duplicatedPartition = false;
      }

      if (duplicatedPartition && !alterTable.isIfNotExists()) {
        throw new DuplicatePartitionException(pair.getSecond());
      } else if (!duplicatedPartition) {
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
          throw new AmbiguousPartitionDirectoryExistException(assumedDirectory.toString());
        }

        long numBytes = 0L;
        if (fs.exists(partitionPath)) {
          ContentSummary summary = fs.getContentSummary(partitionPath);
          numBytes = summary.getLength();
        }

        catalog.alterTable(CatalogUtil.addOrDropPartition(qualifiedName, alterTable.getPartitionColumns(),
          alterTable.getPartitionValues(), alterTable.getLocation(), AlterTableType.ADD_PARTITION, numBytes));

        // If the partition's path doesn't exist, this would make the directory by force.
        if (!fs.exists(partitionPath)) {
          fs.mkdirs(partitionPath);
        }
      }

      break;
    case DROP_PARTITION:
      ensureColumnPartitionKeys(qualifiedName, alterTable.getPartitionColumns());
      pair = CatalogUtil.getPartitionKeyNamePair(alterTable.getPartitionColumns(), alterTable.getPartitionValues());

      boolean undefinedPartition = false;
      try {
        partitionDescProto = catalog.getPartition(databaseName, simpleTableName, pair.getSecond());
      } catch (UndefinedPartitionException e) {
        undefinedPartition = true;
      }

      if (undefinedPartition && !alterTable.isIfExists()) {
        throw new UndefinedPartitionException(pair.getSecond());
      } else if (!undefinedPartition) {
        catalog.alterTable(CatalogUtil.addOrDropPartition(qualifiedName, alterTable.getPartitionColumns(),
            alterTable.getPartitionValues(), alterTable.getLocation(), AlterTableType.DROP_PARTITION));

        // When dropping a partition on a table, its data will NOT be deleted if the 'PURGE' option is not specified.
        if (alterTable.isPurge()) {
          deletePartitionPath(partitionDescProto);
        }
      }
      break;
    case REPAIR_PARTITION:
      repairPartition(context, queryContext, alterTable);
      break;
    default:
      throw new InternalError("alterTable cannot handle such query: \n" + alterTable.toJson());
    }
  }

  /**
   * Run ALTER TABLE table_name REPAIR TABLE  statement.
   * This will recovery all partitions which exists on table directory.
   *
   *
   * @param context
   * @param queryContext
   * @param alterTable
   * @throws IOException
   */
  public void repairPartition(TajoMaster.MasterContext context, final QueryContext queryContext,
                         final AlterTableNode alterTable) throws IOException, TajoException {
    final CatalogService catalog = context.getCatalog();
    final String tableName = alterTable.getTableName();

    String databaseName;
    String simpleTableName;
    if (IdentifierUtil.isFQTableName(tableName)) {
      String[] split = IdentifierUtil.splitFQTableName(tableName);
      databaseName = split[0];
      simpleTableName = split[1];
    } else {
      databaseName = queryContext.getCurrentDatabase();
      simpleTableName = tableName;
    }

    if (!catalog.existsTable(databaseName, simpleTableName)) {
      throw new UndefinedTableException(alterTable.getTableName());
    }

    TableDesc tableDesc = catalog.getTableDesc(databaseName, simpleTableName);

    if(tableDesc.getPartitionMethod() == null) {
      throw new UndefinedPartitionMethodException(simpleTableName);
    }

    Path tablePath = new Path(tableDesc.getUri());
    FileSystem fs = tablePath.getFileSystem(context.getConf());

    PartitionMethodDesc partitionDesc = tableDesc.getPartitionMethod();
    Schema partitionColumns = partitionDesc.getExpressionSchema();

    // Get the array of path filter, accepting all partition paths.
    PathFilter[] filters = PartitionedTableRewriter.buildAllAcceptingPathFilters(partitionColumns);

    // loop from one to the number of partition columns
    Path [] filteredPaths = toPathArray(fs.listStatus(tablePath, filters[0]));

    // Get all file status matched to a ith level path filter.
    for (int i = 1; i < partitionColumns.size(); i++) {
      filteredPaths = toPathArray(fs.listStatus(filteredPaths, filters[i]));
    }

    // Find missing partitions from filesystem
    List<PartitionDescProto> existingPartitions = catalog.getPartitionsOfTable(databaseName, simpleTableName);
    List<String> existingPartitionNames = new ArrayList<>();
    Path existingPartitionPath = null;

    for(PartitionDescProto existingPartition : existingPartitions) {
      existingPartitionPath = new Path(existingPartition.getPath());
      existingPartitionNames.add(existingPartition.getPartitionName());
      if (!fs.exists(existingPartitionPath) && LOG.isDebugEnabled()) {
        LOG.debug("Partitions missing from Filesystem:" + existingPartition.getPartitionName());
      }
    }

    // Find missing partitions from CatalogStore
    List<PartitionDescProto> targetPartitions = new ArrayList<>();
    for(Path filteredPath : filteredPaths) {

      int startIdx = filteredPath.toString().indexOf(PartitionedTableRewriter.getColumnPartitionPathPrefix
        (partitionColumns));

      // if there is partition column in the path
      if (startIdx > -1) {
        PartitionDescProto targetPartition = getPartitionDesc(tablePath, filteredPath, fs);
        if (!existingPartitionNames.contains(targetPartition.getPartitionName())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Partitions not in CatalogStore:" + targetPartition.getPartitionName());
          }
          targetPartitions.add(targetPartition);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Invalid partition path:" + filteredPath.toString());
        }
      }

    }

    catalog.addPartitions(databaseName, simpleTableName, targetPartitions, true);

    if (LOG.isDebugEnabled()) {
      for(PartitionDescProto targetPartition: targetPartitions) {
        LOG.debug("Repair: Added partition to CatalogStore " + tableName + ":" + targetPartition.getPartitionName());
      }
    }

    LOG.info("Total added partitions to CatalogStore: " + targetPartitions.size());
  }

  private PartitionDescProto getPartitionDesc(Path tablePath, Path partitionPath, FileSystem fs) throws IOException {
    String partitionName = StringUtils.unescapePathName(partitionPath.toString());

    int startIndex = partitionName.indexOf(tablePath.toString()) + tablePath.toString().length();
    partitionName = partitionName.substring(startIndex +  File.separator.length());

    CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();
    builder.setPartitionName(partitionName);

    String[] partitionKeyPairs = partitionName.split("/");

    for (String partitionKeyPair : partitionKeyPairs) {
      String[] split = partitionKeyPair.split("=");

      PartitionKeyProto.Builder keyBuilder = PartitionKeyProto.newBuilder();
      keyBuilder.setColumnName(split[0]);
      keyBuilder.setPartitionValue(split[1]);

      builder.addPartitionKeys(keyBuilder.build());
    }

    builder.setPath(partitionPath.toString());

    ContentSummary contentSummary = fs.getContentSummary(partitionPath);
    builder.setNumBytes(contentSummary.getLength());

    return builder.build();
  }


  private void deletePartitionPath(CatalogProtos.PartitionDescProto partitionDescProto) throws IOException {
    Path partitionPath = new Path(partitionDescProto.getPath());
    FileSystem fs = partitionPath.getFileSystem(context.getConf());
    if (fs.exists(partitionPath)) {
      fs.delete(partitionPath, true);
    }
  }

  private boolean ensureColumnPartitionKeys(String tableName, String[] columnNames)
      throws UndefinedPartitionKeyException, UndefinedTableException {

    for(String columnName : columnNames) {
      if (!ensureColumnPartitionKeys(tableName, columnName)) {
        throw new UndefinedPartitionKeyException(columnName);
      }
    }
    return true;
  }

  private boolean ensureColumnPartitionKeys(String tableName, String columnName) throws UndefinedTableException {
    final TableDesc tableDesc = catalog.getTableDesc(tableName);
    if (tableDesc.getPartitionMethod().getExpressionSchema().contains(columnName)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean ensureColumnExistance(String tableName, String columnName) throws UndefinedTableException {
    final TableDesc tableDesc = catalog.getTableDesc(tableName);
    return tableDesc.getSchema().containsByName(columnName);
  }

  private Path [] toPathArray(FileStatus[] fileStatuses) {
    Path [] paths = new Path[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      FileStatus fileStatus = fileStatuses[i];
      paths[i] = fileStatus.getPath();
    }
    return paths;
  }
}
