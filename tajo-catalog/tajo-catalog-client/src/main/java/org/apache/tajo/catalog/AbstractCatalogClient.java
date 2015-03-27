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
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.ProtoUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public abstract class AbstractCatalogClient implements CatalogService {
  private final Log LOG = LogFactory.getLog(AbstractCatalogClient.class);

  protected ServiceTracker serviceTracker;
  protected RpcConnectionPool pool;
  protected InetSocketAddress catalogServerAddr;
  protected TajoConf conf;

  abstract CatalogProtocolService.BlockingInterface getStub(NettyClientBase client);

  public AbstractCatalogClient(TajoConf conf, InetSocketAddress catalogServerAddr) {
    this.pool = RpcConnectionPool.getPool();
    this.catalogServerAddr = catalogServerAddr;
    this.serviceTracker = ServiceTrackerFactory.get(conf);
    this.conf = conf;
  }

  private InetSocketAddress getCatalogServerAddr() {
    if (catalogServerAddr == null) {
      return null;
    } else {

      if (!conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
        return catalogServerAddr;
      } else {
        return serviceTracker.getCatalogAddress();
      }
    }
  }

  @Override
  public final Boolean createTablespace(final String tablespaceName, final String tablespaceUri) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);

          CreateTablespaceRequest.Builder builder = CreateTablespaceRequest.newBuilder();
          builder.setTablespaceName(tablespaceName);
          builder.setTablespaceUri(tablespaceUri);
          return stub.createTablespace(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean dropTablespace(final String tablespaceName) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropTablespace(null, ProtoUtil.convertString(tablespaceName)).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean existTablespace(final String tablespaceName) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existTablespace(null, ProtoUtil.convertString(tablespaceName)).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Collection<String> getAllTablespaceNames() {
    try {
      return new ServerCallable<Collection<String>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Collection<String> call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          PrimitiveProtos.StringListProto response = stub.getAllTablespaceNames(null, ProtoUtil.NULL_PROTO);
          return ProtoUtil.convertStrings(response);
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  
  @Override
  public List<TablespaceProto> getAllTablespaces() {
    try {
      return new ServerCallable<List<TablespaceProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<TablespaceProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          CatalogProtos.GetTablespacesProto response = stub.getAllTablespaces(null, ProtoUtil.NULL_PROTO);
          return response.getTablespaceList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public TablespaceProto getTablespace(final String tablespaceName) {
    try {
      return new ServerCallable<TablespaceProto>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public TablespaceProto call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.getTablespace(null, ProtoUtil.convertString(tablespaceName));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public Boolean alterTablespace(final AlterTablespaceProto alterTablespace) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.alterTablespace(null, alterTablespace).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final Boolean createDatabase(final String databaseName, @Nullable final String tablespaceName) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);

          CreateDatabaseRequest.Builder builder = CreateDatabaseRequest.newBuilder();
          builder.setDatabaseName(databaseName);
          if (tablespaceName != null) {
            builder.setTablespaceName(tablespaceName);
          }
          return stub.createDatabase(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean dropDatabase(final String databaseName) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropDatabase(null, ProtoUtil.convertString(databaseName)).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Boolean existDatabase(final String databaseName) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existDatabase(null, ProtoUtil.convertString(databaseName)).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return Boolean.FALSE;
    }
  }

  @Override
  public final Collection<String> getAllDatabaseNames() {
    try {
      return new ServerCallable<Collection<String>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Collection<String> call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          PrimitiveProtos.StringListProto response = stub.getAllDatabaseNames(null, ProtoUtil.NULL_PROTO);
          return ProtoUtil.convertStrings(response);
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  
  @Override
  public List<DatabaseProto> getAllDatabases() {
    try {
      return new ServerCallable<List<DatabaseProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<DatabaseProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetDatabasesProto response = stub.getAllDatabases(null, ProtoUtil.NULL_PROTO);
          return response.getDatabaseList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final TableDesc getTableDesc(final String databaseName, final String tableName) {
    try {
      return new ServerCallable<TableDesc>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public TableDesc call(NettyClientBase client) throws ServiceException {
          TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return CatalogUtil.newTableDesc(stub.getTableDesc(null, builder.build()));
        }
      }.withRetries();
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
      return new ServerCallable<List<TableDescriptorProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<TableDescriptorProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetTablesProto response = stub.getAllTables(null, ProtoUtil.NULL_PROTO);
          return response.getTableList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  
  @Override
  public List<TableOptionProto> getAllTableOptions() {
    try {
      return new ServerCallable<List<TableOptionProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<TableOptionProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetTableOptionsProto response = stub.getAllTableOptions(null, ProtoUtil.NULL_PROTO);
          return response.getTableOptionList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  
  @Override
  public List<TableStatsProto> getAllTableStats() {
    try {
      return new ServerCallable<List<TableStatsProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<TableStatsProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetTableStatsProto response = stub.getAllTableStats(null, ProtoUtil.NULL_PROTO);
          return response.getStatList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  
  @Override
  public List<ColumnProto> getAllColumns() {
    try {
      return new ServerCallable<List<ColumnProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<ColumnProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetColumnsProto response = stub.getAllColumns(null, ProtoUtil.NULL_PROTO);
          return response.getColumnList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final PartitionMethodDesc getPartitionMethod(final String databaseName, final String tableName) {
    try {
      return new ServerCallable<PartitionMethodDesc>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public PartitionMethodDesc call(NettyClientBase client) throws ServiceException {

          TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return CatalogUtil.newPartitionMethodDesc(stub.getPartitionMethodByTableName(null,  builder.build()));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final boolean existPartitionMethod(final String databaseName, final String tableName) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {

          TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existPartitionMethod(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final PartitionDescProto getPartition(final String databaseName, final String tableName,
                                               final String partitionName) {
    try {
      return new ServerCallable<PartitionDescProto>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public PartitionDescProto call(NettyClientBase client) throws ServiceException {

          PartitionIdentifierProto.Builder builder = PartitionIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);
          builder.setPartitionName(partitionName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.getPartitionByPartitionName(null, builder.build());
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final List<PartitionDescProto> getPartitions(final String databaseName, final String tableName) {
    try {
      return new ServerCallable<List<PartitionDescProto>>(this.pool, getCatalogServerAddr(), CatalogProtocol.class,
        false) {
        public List<PartitionDescProto> call(NettyClientBase client) throws ServiceException {

          PartitionIdentifierProto.Builder builder = PartitionIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          PartitionsProto response = stub.getPartitionsByTableName(null, builder.build());
          return response.getPartitionList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
  @Override
  public List<TablePartitionProto> getAllPartitions() {
    try {
      return new ServerCallable<List<TablePartitionProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<TablePartitionProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetTablePartitionsProto response = stub.getAllPartitions(null, ProtoUtil.NULL_PROTO);
          return response.getPartList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final Collection<String> getAllTableNames(final String databaseName) {
    try {
      return new ServerCallable<Collection<String>>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Collection<String> call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          PrimitiveProtos.StringListProto response = stub.getAllTableNames(null, ProtoUtil.convertString(databaseName));
          return ProtoUtil.convertStrings(response);
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    try {
      return new ServerCallable<Collection<FunctionDesc>>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Collection<FunctionDesc> call(NettyClientBase client) throws ServiceException {
          List<FunctionDesc> list = new ArrayList<FunctionDesc>();
          GetFunctionsResponse response;
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          response = stub.getFunctions(null, NullProto.newBuilder().build());
          int size = response.getFunctionDescCount();
          for (int i = 0; i < size; i++) {
            try {
              list.add(new FunctionDesc(response.getFunctionDesc(i)));
            } catch (ClassNotFoundException e) {
              LOG.error(e, e);
              return null;
            }
          }
          return list;
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final boolean createTable(final TableDesc desc) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.createTable(null, desc.getProto()).getValue();
        }
      }.withRetries();
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
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {

          TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(simpleName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropTable(null, builder.build()).getValue();
        }
      }.withRetries();
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
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {

          TableIdentifierProto.Builder builder = TableIdentifierProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setTableName(tableName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existsTable(null, builder.build()).getValue();
        }
      }.withRetries();
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
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.createIndex(null, index.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean existIndexByName(final String databaseName, final String indexName) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          IndexNameProto.Builder builder = IndexNameProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setIndexName(indexName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existIndexByName(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean existIndexByColumn(final String databaseName, final String tableName, final String columnName) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {

          GetIndexByColumnRequest.Builder builder = GetIndexByColumnRequest.newBuilder();
          builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
          builder.setColumnName(columnName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existIndexByColumn(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final IndexDesc getIndexByName(final String databaseName, final String indexName) {
    try {
      return new ServerCallable<IndexDesc>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public IndexDesc call(NettyClientBase client) throws ServiceException {

          IndexNameProto.Builder builder = IndexNameProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setIndexName(indexName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return new IndexDesc(stub.getIndexByName(null, builder.build()));
        }
      }.withRetries();
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
      return new ServerCallable<IndexDesc>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public IndexDesc call(NettyClientBase client) throws ServiceException {

          GetIndexByColumnRequest.Builder builder = GetIndexByColumnRequest.newBuilder();
          builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
          builder.setColumnName(columnName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return new IndexDesc(stub.getIndexByColumn(null, builder.build()));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public boolean dropIndex(final String databaseName,
                           final String indexName) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {

          IndexNameProto.Builder builder = IndexNameProto.newBuilder();
          builder.setDatabaseName(databaseName);
          builder.setIndexName(indexName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropIndex(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
  
  @Override
  public List<IndexProto> getAllIndexes() {
    try {
      return new ServerCallable<List<IndexProto>>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {

        @Override
        public List<IndexProto> call(NettyClientBase client) throws Exception {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          GetIndexesProto response = stub.getAllIndexes(null, ProtoUtil.NULL_PROTO);
          return response.getIndexList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final boolean createFunction(final FunctionDesc funcDesc) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.createFunction(null, funcDesc.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean dropFunction(final String signature) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          UnregisterFunctionRequest.Builder builder = UnregisterFunctionRequest.newBuilder();
          builder.setSignature(signature);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropFunction(null, builder.build()).getValue();
        }
      }.withRetries();
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
      descProto = new ServerCallable<FunctionDescProto>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public FunctionDescProto call(NettyClientBase client) throws ServiceException {
          try {
            CatalogProtocolService.BlockingInterface stub = getStub(client);
            return stub.getFunctionMeta(null, builder.build());
          } catch (NoSuchFunctionException e) {
            abort();
            throw e;
          }
        }
      }.withRetries();
    } catch(ServiceException e) {
      // this is not good. we need to define user massage exception
      if(e.getCause() instanceof NoSuchFunctionException){
        LOG.debug(e.getMessage());
      } else {
        LOG.error(e.getMessage(), e);
      }
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
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.containFunction(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean alterTable(final AlterTableDesc desc) {
    try {
      return new ServerCallable<Boolean>(this.pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.alterTable(null, desc.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean updateTableStats(final UpdateTableStatsProto updateTableStatsProto) {
    try {
      return new ServerCallable<Boolean>(pool, getCatalogServerAddr(), CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.updateTableStats(null, updateTableStatsProto).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}
