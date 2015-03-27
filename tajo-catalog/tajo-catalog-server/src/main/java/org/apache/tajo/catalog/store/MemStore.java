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

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.DatabaseProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescriptorProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableOptionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablePartitionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceType;
import static org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;

/**
 * CatalogServer guarantees that all operations are thread-safe.
 * So, we don't need to consider concurrency problem here.
 */
public class MemStore implements CatalogStore {
  private final Map<String, String> tablespaces = Maps.newHashMap();
  private final Map<String, Map<String, CatalogProtos.TableDescProto>> databases = Maps.newHashMap();
  private final Map<String, CatalogProtos.FunctionDescProto> functions = Maps.newHashMap();
  private final Map<String, Map<String, IndexDescProto>> indexes = Maps.newHashMap();
  private final Map<String, Map<String, IndexDescProto>> indexesByColumn = Maps.newHashMap();
  private final Map<String, Map<String, CatalogProtos.PartitionDescProto>> partitions = Maps.newHashMap();

  public MemStore(Configuration conf) {
  }

  
  public void close() throws IOException {
    databases.clear();
    functions.clear();
    indexes.clear();
    partitions.clear();
  }

  @Override
  public void createTablespace(String spaceName, String spaceUri) throws CatalogException {
    if (tablespaces.containsKey(spaceName)) {
      throw new AlreadyExistsTablespaceException(spaceName);
    }

    tablespaces.put(spaceName, spaceUri);
  }

  @Override
  public boolean existTablespace(String spaceName) throws CatalogException {
    return tablespaces.containsKey(spaceName);
  }

  @Override
  public void dropTablespace(String spaceName) throws CatalogException {
    if (!tablespaces.containsKey(spaceName)) {
      throw new NoSuchTablespaceException(spaceName);
    }
    tablespaces.remove(spaceName);
  }

  @Override
  public Collection<String> getAllTablespaceNames() throws CatalogException {
    return tablespaces.keySet();
  }
  
  @Override
  public List<TablespaceProto> getTablespaces() throws CatalogException {
    List<TablespaceProto> tablespaceList = TUtil.newList();
    int tablespaceId = 0;
    
    for (String spaceName: tablespaces.keySet()) {
      TablespaceProto.Builder builder = TablespaceProto.newBuilder();
      builder.setSpaceName(spaceName);
      builder.setUri(tablespaces.get(spaceName));
      builder.setId(tablespaceId++);
      tablespaceList.add(builder.build());
    }
    
    return tablespaceList;
  }

  @Override
  public TablespaceProto getTablespace(String spaceName) throws CatalogException {
    if (!tablespaces.containsKey(spaceName)) {
      throw new NoSuchTablespaceException(spaceName);
    }

    TablespaceProto.Builder builder = TablespaceProto.newBuilder();
    builder.setSpaceName(spaceName);
    builder.setUri(tablespaces.get(spaceName));
    return builder.build();
  }

  @Override
  public void alterTablespace(CatalogProtos.AlterTablespaceProto alterProto) throws CatalogException {
    if (!tablespaces.containsKey(alterProto.getSpaceName())) {
      throw new NoSuchTablespaceException(alterProto.getSpaceName());
    }

    if (alterProto.getCommandList().size() > 0) {
      for (CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand cmd : alterProto.getCommandList()) {
        if(cmd.getType() == AlterTablespaceType.LOCATION) {
          CatalogProtos.AlterTablespaceProto.SetLocation setLocation = cmd.getLocation();
          tablespaces.put(alterProto.getSpaceName(), setLocation.getUri());
        }
      }
    }
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName) throws CatalogException {
    if (databases.containsKey(databaseName)) {
      throw new AlreadyExistsDatabaseException(databaseName);
    }

    databases.put(databaseName, new HashMap<String, CatalogProtos.TableDescProto>());
  }

  @Override
  public boolean existDatabase(String databaseName) throws CatalogException {
    return databases.containsKey(databaseName);
  }

  @Override
  public void dropDatabase(String databaseName) throws CatalogException {
    if (!databases.containsKey(databaseName)) {
      throw new NoSuchDatabaseException(databaseName);
    }
    databases.remove(databaseName);
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    return databases.keySet();
  }
  
  @Override
  public List<DatabaseProto> getAllDatabases() throws CatalogException {
    List<DatabaseProto> databaseList = new ArrayList<DatabaseProto>();
    int dbId = 0;
    
    for (String databaseName: databases.keySet()) {
      DatabaseProto.Builder builder = DatabaseProto.newBuilder();
      
      builder.setId(dbId++);
      builder.setName(databaseName);
      builder.setSpaceId(0);
      
      databaseList.add(builder.build());
    }
    
    return databaseList;
  }

  /**
   * Get a database namespace from a Map instance.
   */
  private <T> Map<String, T> checkAndGetDatabaseNS(final Map<String, Map<String, T>> databaseMap,
                                                   String databaseName) {
    if (databaseMap.containsKey(databaseName)) {
      return databaseMap.get(databaseName);
    } else {
      throw new NoSuchDatabaseException(databaseName);
    }
  }

  @Override
  public void createTable(CatalogProtos.TableDescProto request) throws CatalogException {
    String [] splitted = CatalogUtil.splitTableName(request.getTableName());
    if (splitted.length == 1) {
      throw new IllegalArgumentException("createTable() requires a qualified table name, but it is \""
          + request.getTableName() + "\".");
    }
    String databaseName = splitted[0];
    String tableName = splitted[1];

    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    String tbName = tableName;
    if (database.containsKey(tbName)) {
      throw new AlreadyExistsTableException(tbName);
    }
    database.put(tbName, request);
  }

  @Override
  public void updateTableStats(CatalogProtos.UpdateTableStatsProto request) throws CatalogException {
    String [] splitted = CatalogUtil.splitTableName(request.getTableName());
    if (splitted.length == 1) {
      throw new IllegalArgumentException("createTable() requires a qualified table name, but it is \""
        + request.getTableName() + "\".");
    }
    String databaseName = splitted[0];
    String tableName = splitted[1];

    final Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);
    final CatalogProtos.TableDescProto tableDescProto = database.get(tableName);
    CatalogProtos.TableDescProto newTableDescProto = tableDescProto.toBuilder().setStats(request
      .getStats().toBuilder()).build();
    database.put(tableName, newTableDescProto);
  }

  @Override
  public boolean existTable(String dbName, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, dbName);

    return database.containsKey(tbName);
  }

  @Override
  public void dropTable(String dbName, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, dbName);

    if (database.containsKey(tbName)) {
      database.remove(tbName);
    } else {
      throw new NoSuchTableException(tbName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#alterTable(AlterTableDesc)
   */
  @Override
  public void alterTable(CatalogProtos.AlterTableDescProto alterTableDescProto) throws CatalogException {

    String[] split = CatalogUtil.splitTableName(alterTableDescProto.getTableName());
    if (split.length == 1) {
      throw new IllegalArgumentException("alterTable() requires a qualified table name, but it is \""
          + alterTableDescProto.getTableName() + "\".");
    }
    String databaseName = split[0];
    String tableName = split[1];

    final Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    final CatalogProtos.TableDescProto tableDescProto = database.get(tableName);
    CatalogProtos.TableDescProto newTableDescProto;
    CatalogProtos.SchemaProto schemaProto;
    String partitionName = null;
    CatalogProtos.PartitionDescProto partitionDesc = null;

    switch (alterTableDescProto.getAlterTableType()) {
      case RENAME_TABLE:
        if (database.containsKey(alterTableDescProto.getNewTableName())) {
          throw new AlreadyExistsTableException(alterTableDescProto.getNewTableName());
        }
        // Currently, we only use the default table space (i.e., WAREHOUSE directory).
        String spaceUri = tablespaces.get(TajoConstants.DEFAULT_TABLESPACE_NAME);
        // Create a new table directory.
        String newPath = new Path(spaceUri, new Path(databaseName, alterTableDescProto.getNewTableName())).toString();
        newTableDescProto = tableDescProto.toBuilder()
            .setTableName(alterTableDescProto.getNewTableName())
            .setPath(newPath).build();
        database.remove(tableName);
        database.put(alterTableDescProto.getNewTableName(), newTableDescProto);
        break;
      case RENAME_COLUMN:
        schemaProto = tableDescProto.getSchema();
        final int index = getIndexOfColumnToBeRenamed(schemaProto.getFieldsList(),
            alterTableDescProto.getAlterColumnName().getOldColumnName());
        final CatalogProtos.ColumnProto columnProto = schemaProto.getFields(index);
        final CatalogProtos.ColumnProto newcolumnProto =
            columnProto.toBuilder().setName(alterTableDescProto.getAlterColumnName().getNewColumnName()).build();
        newTableDescProto = tableDescProto.toBuilder().setSchema(schemaProto.toBuilder().
            setFields(index, newcolumnProto).build()).build();
        database.put(tableName, newTableDescProto);
        break;
      case ADD_COLUMN:
        schemaProto = tableDescProto.getSchema();
        CatalogProtos.SchemaProto newSchemaProto =
            schemaProto.toBuilder().addFields(alterTableDescProto.getAddColumn()).build();
        newTableDescProto = tableDescProto.toBuilder().setSchema(newSchemaProto).build();
        database.put(tableName, newTableDescProto);
        break;
      case ADD_PARTITION:
        partitionDesc = alterTableDescProto.getPartitionDesc();
        partitionName = partitionDesc.getPartitionName();

        if (partitions.containsKey(tableName) && partitions.get(tableName).containsKey(partitionName)) {
          throw new AlreadyExistsPartitionException(databaseName, tableName, partitionName);
        } else {
          CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();
          builder.setPartitionName(partitionName);
          builder.setPath(partitionDesc.getPath());

          if (partitionDesc.getPartitionKeysCount() > 0) {
            int i = 0;
            for (CatalogProtos.PartitionKeyProto eachKey : partitionDesc.getPartitionKeysList()) {
              CatalogProtos.PartitionKeyProto.Builder keyBuilder = CatalogProtos.PartitionKeyProto.newBuilder();
              keyBuilder.setColumnName(eachKey.getColumnName());
              keyBuilder.setPartitionValue(eachKey.getPartitionValue());
              builder.setPartitionKeys(i, keyBuilder.build());
              i++;
            }
          }

          Map<String, CatalogProtos.PartitionDescProto> protoMap = null;
          if (!partitions.containsKey(tableName)) {
            protoMap = Maps.newHashMap();
          } else {
            protoMap = partitions.get(tableName);
          }
          protoMap.put(partitionName, builder.build());
          partitions.put(tableName, protoMap);
        }
        break;
      case DROP_PARTITION:
        partitionDesc = alterTableDescProto.getPartitionDesc();
        partitionName = partitionDesc.getPartitionName();
        if(!partitions.containsKey(tableName)) {
          throw new NoSuchPartitionException(databaseName, tableName, partitionName);
        } else {
          partitions.remove(partitionName);
        }
        break;
      default:
    }
  }


  private int getIndexOfColumnToBeRenamed(List<CatalogProtos.ColumnProto> fieldList, String columnName) {
    int fieldCount = fieldList.size();
    for (int index = 0; index < fieldCount; index++) {
      CatalogProtos.ColumnProto columnProto = fieldList.get(index);
      if (null != columnProto && columnProto.getName().equalsIgnoreCase(columnName)) {
        return index;
      }
    }
    return -1;
  }
  /* (non-Javadoc)
   * @see CatalogStore#getTable(java.lang.String)
   */
  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto unqualified = database.get(tableName);
      CatalogProtos.TableDescProto.Builder builder = CatalogProtos.TableDescProto.newBuilder();
      CatalogProtos.SchemaProto schemaProto =
          CatalogUtil.getQualfiedSchema(databaseName + "." + tableName, unqualified.getSchema());
      builder.mergeFrom(unqualified);
      builder.setSchema(schemaProto);
      return builder.build();
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames(String databaseName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);
    return new ArrayList<String>(database.keySet());
  }
  
  @Override
  public List<TableDescriptorProto> getAllTables() throws CatalogException {
    List<TableDescriptorProto> tableList = new ArrayList<CatalogProtos.TableDescriptorProto>();
    int dbId = 0, tableId = 0;
    
    for (String databaseName: databases.keySet()) {
      Map<String, TableDescProto> tables = databases.get(databaseName);
      List<String> tableNameList = TUtil.newList(tables.keySet());
      Collections.sort(tableNameList);
      
      for (String tableName: tableNameList) {
        TableDescProto tableDesc = tables.get(tableName);
        TableDescriptorProto.Builder builder = TableDescriptorProto.newBuilder();
        
        builder.setDbId(dbId);
        builder.setTid(tableId);
        builder.setName(tableName);
        builder.setPath(tableDesc.getPath());
        builder.setTableType(tableDesc.getIsExternal()?"EXTERNAL":"BASE");
        builder.setStoreType(CatalogUtil.getStoreTypeString(tableDesc.getMeta().getStoreType()));
        
        tableList.add(builder.build());
        tableId++;
      }
      dbId++;
    }
    
    return tableList;
  }
  
  @Override
  public List<TableOptionProto> getAllTableOptions() throws CatalogException {
    List<TableOptionProto> optionList = new ArrayList<CatalogProtos.TableOptionProto>();
    int tid = 0;
    
    for (String databaseName: databases.keySet()) {
      Map<String, TableDescProto> tables = databases.get(databaseName);
      List<String> tableNameList = TUtil.newList(tables.keySet());
      Collections.sort(tableNameList);
      
      for (String tableName: tableNameList) {
        TableDescProto table = tables.get(tableName); 
        List<KeyValueProto> keyValueList = table.getMeta().getParams().getKeyvalList();
        
        for (KeyValueProto keyValue: keyValueList) {
          TableOptionProto.Builder builder = TableOptionProto.newBuilder();
          
          builder.setTid(tid);
          builder.setKeyval(keyValue);
          
          optionList.add(builder.build());
        }
      }
      tid++;
    }
    
    return optionList;
  }
  
  @Override
  public List<TableStatsProto> getAllTableStats() throws CatalogException {
    List<TableStatsProto> statList = new ArrayList<CatalogProtos.TableStatsProto>();
    int tid = 0;
    
    for (String databaseName: databases.keySet()) {
      Map<String, TableDescProto> tables = databases.get(databaseName);
      List<String> tableNameList = TUtil.newList(tables.keySet());
      Collections.sort(tableNameList);
      
      for (String tableName: tableNameList) {
        TableDescProto table = tables.get(tableName);
        TableStatsProto.Builder builder = TableStatsProto.newBuilder();
        
        builder.setTid(tid);
        builder.setNumRows(table.getStats().getNumRows());
        builder.setNumBytes(table.getStats().getNumBytes());
        
        statList.add(builder.build());
      }
      tid++;
    }
    
    return statList;
  }
  
  @Override
  public List<ColumnProto> getAllColumns() throws CatalogException {
    List<ColumnProto> columnList = new ArrayList<CatalogProtos.ColumnProto>();
    int tid = 0;
    
    for (String databaseName: databases.keySet()) {
      Map<String, TableDescProto> tables = databases.get(databaseName);
      List<String> tableNameList = TUtil.newList(tables.keySet());
      Collections.sort(tableNameList);
      
      for (String tableName: tableNameList) {
        TableDescProto tableDesc = tables.get(tableName);
        
        for (ColumnProto column: tableDesc.getSchema().getFieldsList()) {
          ColumnProto.Builder builder = ColumnProto.newBuilder();
          builder.setTid(tid);
          builder.setName(column.getName());
          builder.setDataType(column.getDataType());
          columnList.add(builder.build());
        }
      }
      tid++;
    }
    
    return columnList;
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto table = database.get(tableName);
      return table.hasPartition() ? table.getPartition() : null;
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto table = database.get(tableName);
      return table.hasPartition();
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  @Override
  public void dropPartitionMethod(String databaseName, String tableName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public List<CatalogProtos.PartitionDescProto> getPartitions(String databaseName, String tableName) throws CatalogException {
    List<CatalogProtos.PartitionDescProto> protos = new ArrayList<CatalogProtos.PartitionDescProto>();

    if (partitions.containsKey(tableName)) {
      for (CatalogProtos.PartitionDescProto proto : partitions.get(tableName).values()) {
        protos.add(proto);
      }
    }
    return protos;
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                       String partitionName) throws CatalogException {
    if (partitions.containsKey(tableName) && partitions.get(tableName).containsKey(partitionName)) {
      return partitions.get(tableName).get(partitionName);
    } else {
      throw new NoSuchPartitionException(partitionName);
    }
  }

  public List<TablePartitionProto> getAllPartitions() throws CatalogException {
    List<TablePartitionProto> protos = new ArrayList<TablePartitionProto>();
    Set<String> tables = partitions.keySet();
    for (String table : tables) {
      Map<String, CatalogProtos.PartitionDescProto> entryMap = partitions.get(table);
      for (Map.Entry<String, CatalogProtos.PartitionDescProto> proto : entryMap.entrySet()) {
        CatalogProtos.PartitionDescProto partitionDescProto = proto.getValue();

        TablePartitionProto.Builder builder = TablePartitionProto.newBuilder();

        builder.setPartitionName(partitionDescProto.getPartitionName());
        builder.setPath(partitionDescProto.getPath());

        // PARTITION_ID and TID is always necessary variables. In other CatalogStore excepting MemStore,
        // all partitions would have PARTITION_ID and TID. But MemStore doesn't contain these variable values because
        // it is implemented for test purpose. Thus, we need to set each variables to 0.
        builder.setPartitionId(0);
        builder.setTid(0);

        protos.add(builder.build());
      }
    }
    return protos;
  }

  /* (non-Javadoc)
   * @see CatalogStore#createIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void createIndex(IndexDescProto proto) throws CatalogException {
    final String databaseName = proto.getTableIdentifier().getDatabaseName();

    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);

    if (index.containsKey(proto.getIndexName())) {
      throw new AlreadyExistsIndexException(proto.getIndexName());
    }

    index.put(proto.getIndexName(), proto);
    indexByColumn.put(proto.getTableIdentifier().getTableName() + "."
        + CatalogUtil.extractSimpleName(proto.getColumn().getName()), proto);
  }

  /* (non-Javadoc)
   * @see CatalogStore#dropIndex(java.lang.String)
   */
  @Override
  public void dropIndex(String databaseName, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    if (!index.containsKey(indexName)) {
      throw new NoSuchIndexException(indexName);
    }
    index.remove(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexByName(java.lang.String)
   */
  @Override
  public IndexDescProto getIndexByName(String databaseName, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    if (!index.containsKey(indexName)) {
      throw new NoSuchIndexException(indexName);
    }

    return index.get(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexByName(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException {

    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    if (!indexByColumn.containsKey(columnName)) {
      throw new NoSuchIndexException(columnName);
    }

    return indexByColumn.get(columnName);
  }

  @Override
  public boolean existIndexByName(String databaseName, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    return index.containsKey(indexName);
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException {
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    return indexByColumn.containsKey(columnName);
  }

  @Override
  public IndexDescProto[] getIndexes(String databaseName, String tableName) throws CatalogException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    for (IndexDescProto proto : indexByColumn.values()) {
      if (proto.getTableIdentifier().getTableName().equals(tableName)) {
        protos.add(proto);
      }
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
  }
  
  @Override
  public List<IndexProto> getAllIndexes() throws CatalogException {
    List<IndexProto> indexList = new ArrayList<CatalogProtos.IndexProto>();
    Set<String> databases = indexes.keySet();
    
    for (String databaseName: databases) {
      Map<String, IndexDescProto> indexMap = indexes.get(databaseName);
      
      for (Map.Entry<String, IndexDescProto> entry: indexMap.entrySet()) {
        IndexDescProto indexDesc = entry.getValue();
        IndexProto.Builder builder = IndexProto.newBuilder();
        
        builder.setColumnName(indexDesc.getColumn().getName());
        builder.setDataType(indexDesc.getColumn().getDataType().getType().toString());
        builder.setIndexName(entry.getKey());
        builder.setIndexType(indexDesc.getIndexMethod().toString());
        builder.setIsAscending(indexDesc.hasIsAscending() && indexDesc.getIsAscending());
        builder.setIsClustered(indexDesc.hasIsClustered() && indexDesc.getIsClustered());
        builder.setIsUnique(indexDesc.hasIsUnique() && indexDesc.getIsUnique());
        
        indexList.add(builder.build());
      }
    }
    
    return indexList;
  }

  @Override
  public void addFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public void deleteFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public void existFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public List<String> getAllFunctionNames() throws CatalogException {
    // to be implemented
    return null;
  }

}
