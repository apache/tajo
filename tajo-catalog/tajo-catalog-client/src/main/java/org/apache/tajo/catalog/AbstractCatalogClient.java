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

package org.apache.tajo.catalog;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.util.ProtoUtil;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public abstract class AbstractCatalogClient implements CatalogService, Closeable {
  protected final Log LOG = LogFactory.getLog(AbstractCatalogClient.class);

  protected TajoConf conf;

  public AbstractCatalogClient(TajoConf conf) {
    this.conf = conf;
  }

  abstract CatalogProtocolService.BlockingInterface getStub() throws ServiceException;

  @Override
  public final Boolean createTablespace(final String tablespaceName, final String tablespaceUri) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();

      CreateTablespaceRequest.Builder builder = CreateTablespaceRequest.newBuilder();
      builder.setTablespaceName(tablespaceName);
      builder.setTablespaceUri(tablespaceUri);
      return stub.createTablespace(null, builder.build()).getValue();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean dropTablespace(final String tablespaceName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.dropTablespace(null, ProtoUtil.convertString(tablespaceName)).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean existTablespace(final String tablespaceName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existTablespace(null, ProtoUtil.convertString(tablespaceName)).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Collection<String> getAllTablespaceNames() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      PrimitiveProtos.StringListProto response = stub.getAllTablespaceNames(null, ProtoUtil.NULL_PROTO);
      return ProtoUtil.convertStrings(response);
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<String>();
    }
  }
  
  @Override
  public List<TablespaceProto> getAllTablespaces() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      CatalogProtos.GetTablespacesProto response = stub.getAllTablespaces(null, ProtoUtil.NULL_PROTO);
      return response.getTablespaceList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<TablespaceProto>();
    }
  }

  @Override
  public TablespaceProto getTablespace(final String tablespaceName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.getTablespace(null, ProtoUtil.convertString(tablespaceName));
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public Boolean alterTablespace(final AlterTablespaceProto alterTablespace) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.alterTablespace(null, alterTablespace).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final Boolean createDatabase(final String databaseName, @Nullable final String tablespaceName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();

      CreateDatabaseRequest.Builder builder = CreateDatabaseRequest.newBuilder();
      builder.setDatabaseName(databaseName);
      if (tablespaceName != null) {
        builder.setTablespaceName(tablespaceName);
      }
      return stub.createDatabase(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean dropDatabase(final String databaseName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.dropDatabase(null, ProtoUtil.convertString(databaseName)).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean existDatabase(final String databaseName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existDatabase(null, ProtoUtil.convertString(databaseName)).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Collection<String> getAllDatabaseNames() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      PrimitiveProtos.StringListProto response = stub.getAllDatabaseNames(null, ProtoUtil.NULL_PROTO);
      return ProtoUtil.convertStrings(response);
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<String>();
    }
  }
  
  @Override
  public List<DatabaseProto> getAllDatabases() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetDatabasesProto response = stub.getAllDatabases(null, ProtoUtil.NULL_PROTO);
      return response.getDatabaseList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<DatabaseProto>();
    }
  }

  @Override
  public final TableDesc getTableDesc(final String databaseName, final String tableName) {
    try {
      TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return CatalogUtil.newTableDesc(stub.getTableDesc(null, builder.build()));
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public TableDesc getTableDesc(String qualifiedName) {
    String [] splitted = CatalogUtil.splitFQTableName(qualifiedName);
    return getTableDesc(splitted[0], splitted[1]);
  }
  
  @Override
  public List<TableDescriptorProto> getAllTables() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetTablesProto response = stub.getAllTables(null, ProtoUtil.NULL_PROTO);
      return response.getTableList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<TableDescriptorProto>();
    }
  }
  
  @Override
  public List<TableOptionProto> getAllTableOptions() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetTableOptionsProto response = stub.getAllTableOptions(null, ProtoUtil.NULL_PROTO);
      return response.getTableOptionList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<TableOptionProto>();
    }
  }
  
  @Override
  public List<TableStatsProto> getAllTableStats() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetTableStatsProto response = stub.getAllTableStats(null, ProtoUtil.NULL_PROTO);
      return response.getStatList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<TableStatsProto>();
    }
  }
  
  @Override
  public List<ColumnProto> getAllColumns() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetColumnsProto response = stub.getAllColumns(null, ProtoUtil.NULL_PROTO);
      return response.getColumnList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<ColumnProto>();
    }
  }

  @Override
  public final PartitionMethodDesc getPartitionMethod(final String databaseName, final String tableName) {
    try {
      TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return CatalogUtil.newPartitionMethodDesc(stub.getPartitionMethodByTableName(null, builder.build()));
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final boolean existPartitionMethod(final String databaseName, final String tableName) {
    try {
      TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existPartitionMethod(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final PartitionDescProto getPartition(final String databaseName, final String tableName,
                                               final String partitionName) {
    try {
      PartitionIdentifierProto.Builder builder = PartitionIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);
      builder.setPartitionName(partitionName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.getPartitionByPartitionName(null, builder.build());
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final List<PartitionDescProto> getPartitions(final String databaseName, final String tableName) {
    try {
      PartitionIdentifierProto.Builder builder = PartitionIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      PartitionsProto response = stub.getPartitionsByTableName(null, builder.build());
      return response.getPartitionList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<PartitionDescProto>();
    }
  }

  @Override
  public List<TablePartitionProto> getPartitionsByDirectSql(final String databaseName,
                                                                     final String tableName,
                                                                     final String directSql) {
    try {
        PartitionIdentifierProto.Builder builder = PartitionIdentifierProto.newBuilder();
        builder.setDatabaseName(databaseName);
        builder.setTableName(tableName);
        builder.setDirectSql(directSql);

        CatalogProtocolService.BlockingInterface stub = getStub();
        GetTablePartitionsProto response = stub.getPartitionsByDirectSql(null, builder.build());
        return response.getPartList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  @Override
  public List<TablePartitionProto> getAllPartitions() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetTablePartitionsProto response = stub.getAllPartitions(null, ProtoUtil.NULL_PROTO);
      return response.getPartList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<TablePartitionProto>();
    }
  }

  @Override
  public final Collection<String> getAllTableNames(final String databaseName) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      PrimitiveProtos.StringListProto response = stub.getAllTableNames(null, ProtoUtil.convertString(databaseName));
      return ProtoUtil.convertStrings(response);
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<String>();
    }
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    try {
      GetFunctionsResponse response;
      CatalogProtocolService.BlockingInterface stub = getStub();
      response = stub.getFunctions(null, NullProto.newBuilder().build());
      int size = response.getFunctionDescCount();
      for (int i = 0; i < size; i++) {
        try {
          list.add(new FunctionDesc(response.getFunctionDesc(i)));
        } catch (ClassNotFoundException e) {
          LOG.error(e, e);
          return list;
        }
      }
      return list;
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return list;
    }
  }

  @Override
  public final boolean createTable(final TableDesc desc) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.createTable(null, desc.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean dropTable(String tableName) {
    String [] splitted = CatalogUtil.splitFQTableName(tableName);
    final String databaseName = splitted[0];
    final String simpleName = splitted[1];

    try {
      TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(simpleName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.dropTable(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean existsTable(final String databaseName, final String tableName) {
    if (CatalogUtil.isFQTableName(tableName)) {
      throw new IllegalArgumentException(
          "tableName cannot be composed of multiple parts, but it is \"" + tableName + "\"");
    }
    try {
      TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setTableName(tableName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existsTable(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
  @Override
  public final boolean existsTable(final String tableName) {
    String [] splitted = CatalogUtil.splitFQTableName(tableName);
    return existsTable(splitted[0], splitted[1]);
  }

  @Override
  public final boolean createIndex(final IndexDesc index) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.createIndex(null, index.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean existIndexByName(final String databaseName, final String indexName) {
    try {
      IndexNameProto.Builder builder = IndexNameProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setIndexName(indexName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existIndexByName(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean existIndexByColumn(final String databaseName, final String tableName, final String columnName) {
    try {

      GetIndexByColumnRequest.Builder builder = GetIndexByColumnRequest.newBuilder();
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      builder.setColumnName(columnName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.existIndexByColumn(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final IndexDesc getIndexByName(final String databaseName, final String indexName) {
    try {
      IndexNameProto.Builder builder = IndexNameProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setIndexName(indexName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return new IndexDesc(stub.getIndexByName(null, builder.build()));
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final IndexDesc getIndexByColumn(final String databaseName,
                                          final String tableName,
                                          final String columnName) {
    try {
      GetIndexByColumnRequest.Builder builder = GetIndexByColumnRequest.newBuilder();
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      builder.setColumnName(columnName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return new IndexDesc(stub.getIndexByColumn(null, builder.build()));
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public boolean dropIndex(final String databaseName,
                           final String indexName) {
    try {
      IndexNameProto.Builder builder = IndexNameProto.newBuilder();
      builder.setDatabaseName(databaseName);
      builder.setIndexName(indexName);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.dropIndex(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
  
  @Override
  public List<IndexProto> getAllIndexes() {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      GetIndexesProto response = stub.getAllIndexes(null, ProtoUtil.NULL_PROTO);
      return response.getIndexList();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return new ArrayList<IndexProto>();
    }
  }

  @Override
  public final boolean createFunction(final FunctionDesc funcDesc) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.createFunction(null, funcDesc.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean dropFunction(final String signature) {
    try {
      UnregisterFunctionRequest.Builder builder = UnregisterFunctionRequest.newBuilder();
      builder.setSignature(signature);

      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.dropFunction(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final FunctionDesc getFunction(final String signature, DataType... paramTypes) {
    return getFunction(signature, null, paramTypes);
  }

  @Override
  public final FunctionDesc getFunction(final String signature, FunctionType funcType, DataType... paramTypes) {
    final GetFunctionMetaRequest.Builder builder = GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    FunctionDescProto descProto = null;
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      descProto = stub.getFunctionMeta(null, builder.build());
    } catch (NoSuchFunctionException e) {
      LOG.debug(e.getMessage());
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
    }

    if (descProto == null) {
      throw new NoSuchFunctionException(signature, paramTypes);
    }

    try {
      return new FunctionDesc(descProto);
    } catch (ClassNotFoundException e) {
      LOG.error(e, e);
      throw new NoSuchFunctionException(signature, paramTypes);
    }
  }

  @Override
  public final boolean containFunction(final String signature, DataType... paramTypes) {
    return containFunction(signature, null, paramTypes);
  }

  @Override
  public final boolean containFunction(final String signature, FunctionType funcType, DataType... paramTypes) {
    final ContainFunctionRequest.Builder builder =
        ContainFunctionRequest.newBuilder();
    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    builder.setSignature(signature);
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.containFunction(null, builder.build()).getValue();
    } catch (InvalidOperationException e) {
      LOG.error(e.getMessage());
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
    }
    return false;
  }

  @Override
  public final boolean alterTable(final AlterTableDesc desc) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.alterTable(null, desc.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean updateTableStats(final UpdateTableStatsProto updateTableStatsProto) {
    try {
      CatalogProtocolService.BlockingInterface stub = getStub();
      return stub.updateTableStats(null, updateTableStatsProto).getValue();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}
