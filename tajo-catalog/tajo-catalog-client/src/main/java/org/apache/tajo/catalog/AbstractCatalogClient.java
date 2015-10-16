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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService.BlockingInterface;
import org.apache.tajo.catalog.CatalogProtocol.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListResponse;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.util.TUtil;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.catalog.CatalogUtil.buildTableIdentifier;
import static org.apache.tajo.error.Errors.ResultCode.*;
import static org.apache.tajo.exception.ExceptionUtil.throwIfError;
import static org.apache.tajo.exception.ExceptionUtil.throwsIfThisError;
import static org.apache.tajo.exception.ReturnStateUtil.*;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public abstract class AbstractCatalogClient implements CatalogService, Closeable {
  protected final Log LOG = LogFactory.getLog(AbstractCatalogClient.class);

  protected TajoConf conf;

  public AbstractCatalogClient(TajoConf conf) {
    this.conf = conf;
  }

  abstract BlockingInterface getStub() throws ServiceException;

  @Override
  public final void createTablespace(final String tablespaceName, final String tablespaceUri)
      throws DuplicateTablespaceException {

    try {
      final BlockingInterface stub = getStub();
      final CreateTablespaceRequest request = CreateTablespaceRequest.newBuilder()
          .setTablespaceName(tablespaceName)
          .setTablespaceUri(tablespaceUri)
          .build();
      final ReturnState state = stub.createTablespace(null, request);

      throwsIfThisError(state, DuplicateTablespaceException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void dropTablespace(final String tablespaceName)
      throws UndefinedTablespaceException, InsufficientPrivilegeException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.dropTablespace(null, ProtoUtil.convertString(tablespaceName));

      throwsIfThisError(state, UndefinedTablespaceException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existTablespace(final String tablespaceName) {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.existTablespace(null, ProtoUtil.convertString(tablespaceName));

      if (isThisError(state, UNDEFINED_TABLESPACE)) {
        return false;
      }
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Collection<String> getAllTablespaceNames() {

    try {
      final BlockingInterface stub = getStub();
      final StringListResponse response = stub.getAllTablespaceNames(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getValuesList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<TablespaceProto> getAllTablespaces() {

    try {
      final BlockingInterface stub = getStub();
      final GetTablespaceListResponse response = stub.getAllTablespaces(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getTablespaceList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TablespaceProto getTablespace(final String tablespaceName) throws UndefinedTablespaceException {

    try {
      final BlockingInterface stub = getStub();
      final GetTablespaceResponse response = stub.getTablespace(null, ProtoUtil.convertString(tablespaceName));

      throwsIfThisError(response.getState(), UndefinedTablespaceException.class);
      ensureOk(response.getState());
      return response.getTablespace();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void alterTablespace(final AlterTablespaceProto alterTablespace) throws UndefinedTablespaceException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.alterTablespace(null, alterTablespace);

      throwsIfThisError(state, UndefinedTablespaceException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void createDatabase(final String databaseName, @Nullable final String tablespaceName)
      throws DuplicateDatabaseException {

    try {

      final BlockingInterface stub = getStub();
      final CreateDatabaseRequest.Builder builder = CreateDatabaseRequest.newBuilder();
      builder.setDatabaseName(databaseName);
      if (tablespaceName != null) {
        builder.setTablespaceName(tablespaceName);
      }
      final ReturnState state = stub.createDatabase(null, builder.build());

      throwsIfThisError(state, DuplicateDatabaseException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void dropDatabase(final String databaseName)
      throws UndefinedDatabaseException, InsufficientPrivilegeException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.dropDatabase(null, ProtoUtil.convertString(databaseName));

      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existDatabase(final String databaseName) {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.existDatabase(null, ProtoUtil.convertString(databaseName));

      if (isThisError(state, UNDEFINED_DATABASE)) {
        return false;
      }
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Collection<String> getAllDatabaseNames() {

    try {
      final BlockingInterface stub = getStub();
      final StringListResponse response = stub.getAllDatabaseNames(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getValuesList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DatabaseProto> getAllDatabases() {

    try {
      final BlockingInterface stub = getStub();
      final GetDatabasesResponse response = stub.getAllDatabases(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getDatabaseList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final TableDesc getTableDesc(final String databaseName, final String tableName)
      throws UndefinedTableException {

    try {
      final BlockingInterface stub = getStub();
      final TableIdentifierProto request = buildTableIdentifier(databaseName, tableName);
      final TableResponse response = stub.getTableDesc(null, request);

      throwsIfThisError(response.getState(), UndefinedTableException.class);
      ensureOk(response.getState());
      return CatalogUtil.newTableDesc(response.getTable());

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TableDesc getTableDesc(String qualifiedName) throws UndefinedTableException {
    String[] splitted = CatalogUtil.splitFQTableName(qualifiedName);
    return getTableDesc(splitted[0], splitted[1]);
  }

  @Override
  public List<TableDescriptorProto> getAllTables() {

    try {
      final BlockingInterface stub = getStub();
      final GetTablesResponse response = stub.getAllTables(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getTableList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<TableOptionProto> getAllTableOptions() {

    try {
      final BlockingInterface stub = getStub();
      final GetTablePropertiesResponse response = stub.getAllTableProperties(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getPropertiesList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<TableStatsProto> getAllTableStats() {

    try {
      final BlockingInterface stub = getStub();
      final GetTableStatsResponse response = stub.getAllTableStats(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getStatsList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ColumnProto> getAllColumns() {

    try {
      final BlockingInterface stub = getStub();
      final GetColumnsResponse response = stub.getAllColumns(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getColumnList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<IndexDescProto> getAllIndexes() {
    try {
      final BlockingInterface stub = getStub();
      final IndexListResponse response = stub.getAllIndexes(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getIndexDescList();

    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final PartitionMethodDesc getPartitionMethod(final String databaseName, final String tableName)
      throws UndefinedPartitionMethodException, UndefinedDatabaseException, UndefinedTableException {

    try {
      final BlockingInterface stub = getStub();
      final TableIdentifierProto request = buildTableIdentifier(databaseName, tableName);
      final GetPartitionMethodResponse response = stub.getPartitionMethodByTableName(null, request);


      throwsIfThisError(response.getState(), UndefinedPartitionMethodException.class);
      throwsIfThisError(response.getState(), UndefinedDatabaseException.class);
      throwsIfThisError(response.getState(), UndefinedTableException.class);
      ensureOk(response.getState());
      return CatalogUtil.newPartitionMethodDesc(response.getPartition());

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existPartitionMethod(final String databaseName, final String tableName)
      throws UndefinedDatabaseException, UndefinedTableException {

    try {
      final BlockingInterface stub = getStub();
      final TableIdentifierProto request = buildTableIdentifier(databaseName, tableName);
      final ReturnState state = stub.existPartitionMethod(null, request);

      if (isThisError(state, UNDEFINED_PARTITION_METHOD)) {
        return false;
      }
      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, UndefinedTableException.class);
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final PartitionDescProto getPartition(final String databaseName, final String tableName,
                                               final String partitionName)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionException,
      UndefinedPartitionMethodException {

    try {
      final BlockingInterface stub = getStub();
      final PartitionIdentifierProto request = PartitionIdentifierProto.newBuilder()
          .setDatabaseName(databaseName)
          .setTableName(tableName)
          .setPartitionName(partitionName)
          .build();
      final GetPartitionDescResponse response = stub.getPartitionByPartitionName(null, request);

      throwsIfThisError(response.getState(), UndefinedDatabaseException.class);
      throwsIfThisError(response.getState(), UndefinedTableException.class);
      throwsIfThisError(response.getState(), UndefinedPartitionMethodException.class);
      throwsIfThisError(response.getState(), UndefinedPartitionException.class);
      ensureOk(response.getState());
      return response.getPartition();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final List<PartitionDescProto> getPartitions(final String databaseName, final String tableName) {
    try {
      final BlockingInterface stub = getStub();
      final PartitionIdentifierProto request = PartitionIdentifierProto.newBuilder()
          .setDatabaseName(databaseName)
          .setTableName(tableName)
          .build();
      final GetPartitionsResponse response = stub.getPartitionsByTableName(null, request);

      ensureOk(response.getState());
      return response.getPartitionList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<TablePartitionProto> getAllPartitions() {
    try {
      final BlockingInterface stub = getStub();
      final GetTablePartitionsResponse response = stub.getAllPartitions(null, ProtoUtil.NULL_PROTO);

      ensureOk(response.getState());
      return response.getPartList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addPartitions(String databaseName, String tableName, List<PartitionDescProto> partitions,
                               boolean ifNotExists)
      throws UndefinedDatabaseException, UndefinedTableException, DuplicatePartitionException,
      UndefinedPartitionMethodException {

    try {
      final BlockingInterface stub = getStub();

      final AddPartitionsProto.Builder builder = AddPartitionsProto.newBuilder();
      final TableIdentifierProto.Builder identifier = TableIdentifierProto.newBuilder()
          .setDatabaseName(databaseName)
          .setTableName(tableName);
      builder.setTableIdentifier(identifier.build());

      for (PartitionDescProto partition: partitions) {
        builder.addPartitionDesc(partition);
      }
      builder.setIfNotExists(ifNotExists);

      ReturnState state = stub.addPartitions(null, builder.build());
      throwsIfThisError(state, UndefinedTableException.class);
      throwsIfThisError(state, UndefinedPartitionMethodException.class);
      throwsIfThisError(state, DuplicatePartitionException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Collection<String> getAllTableNames(final String databaseName) {
    try {
      final BlockingInterface stub = getStub();
      final StringListResponse response = stub.getAllTableNames(null, ProtoUtil.convertString(databaseName));

      ensureOk(response.getState());
      return response.getValuesList();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();

    try {
      final BlockingInterface stub = getStub();
      final GetFunctionsResponse response = stub.getFunctions(null, NullProto.newBuilder().build());

      ensureOk(response.getState());
      for (int i = 0; i < response.getFunctionDescCount(); i++) {
        try {
          list.add(new FunctionDesc(response.getFunctionDesc(i)));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      return list;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void createTable(final TableDesc desc)
      throws UndefinedDatabaseException, DuplicateTableException, InsufficientPrivilegeException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.createTable(null, desc.getProto());

      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, DuplicateTableException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void dropTable(String tableName)
      throws UndefinedDatabaseException, UndefinedTableException, InsufficientPrivilegeException {

    String[] splitted = CatalogUtil.splitFQTableName(tableName);
    final String databaseName = splitted[0];
    final String simpleName = splitted[1];

    try {
      final BlockingInterface stub = getStub();
      final TableIdentifierProto request = buildTableIdentifier(databaseName, simpleName);
      final ReturnState state = stub.dropTable(null, request);

      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, UndefinedTableException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existsTable(final String databaseName, final String tableName) {
    if (CatalogUtil.isFQTableName(tableName)) {
      throw new IllegalArgumentException(
          "tableName cannot be composed of multiple parts, but it is \"" + tableName + "\"");
    }

    try {
      final BlockingInterface stub = getStub();
      final TableIdentifierProto request = buildTableIdentifier(databaseName, tableName);
      final ReturnState state = stub.existsTable(null, request);

      if (isThisError(state, UNDEFINED_TABLE)) {
        return false;
      }
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existsTable(final String tableName) {
    String[] splitted = CatalogUtil.splitFQTableName(tableName);
    return existsTable(splitted[0], splitted[1]);
  }

  @Override
  public final void createIndex(final IndexDesc index)
      throws DuplicateIndexException, UndefinedDatabaseException, UndefinedTableException {

    try {
      final BlockingInterface stub = getStub();

      final ReturnState state = stub.createIndex(null, index.getProto());

      throwsIfThisError(state, DuplicateIndexException.class);
      throwsIfThisError(state, UndefinedTableException.class);
      throwsIfThisError(state, UndefinedDatabaseException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean existIndexByName(final String databaseName, final String indexName) {
    try {
      final IndexNameProto request = IndexNameProto.newBuilder()
          .setDatabaseName(databaseName)
          .setIndexName(indexName)
          .build();

      final BlockingInterface stub = getStub();

      return isSuccess(stub.existIndexByName(null, request));

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean existIndexByColumns(final String databaseName, final String tableName, final Column [] columns) {
    return existIndexByColumnNames(databaseName, tableName, extractColumnNames(columns));
  }

  @Override
  public boolean existIndexByColumnNames(final String databaseName, final String tableName, final String [] columnNames) {
    try {

      GetIndexByColumnNamesRequest.Builder builder = GetIndexByColumnNamesRequest.newBuilder();
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      for (String colunName : columnNames) {
        builder.addColumnNames(colunName);
      }

      final BlockingInterface stub = getStub();

      return isSuccess(stub.existIndexByColumnNames(null, builder.build()));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean existIndexesByTable(final String databaseName, final String tableName) {
    try {
      final BlockingInterface stub = getStub();

      return isSuccess(
          stub.existIndexesByTable(null, CatalogUtil.buildTableIdentifier(databaseName, tableName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final IndexDesc getIndexByName(final String databaseName, final String indexName) {

    try {
      final IndexNameProto request = IndexNameProto.newBuilder()
          .setDatabaseName(databaseName)
          .setIndexName(indexName)
          .build();

      final BlockingInterface stub = getStub();
      final IndexResponse response = stub.getIndexByName(null, request);
      ensureOk(response.getState());

      return new IndexDesc(response.getIndexDesc());

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  private static String[] extractColumnNames(Column[] columns) {
    String[] columnNames = new String [columns.length];
    for (int i = 0; i < columnNames.length; i++) {
      columnNames[i] = columns[i].getSimpleName();
    }
    return columnNames;
  }

  @Override
  public final IndexDesc getIndexByColumns(final String databaseName,
                                               final String tableName,
                                               final Column [] columns) {
    return getIndexByColumnNames(databaseName, tableName, extractColumnNames(columns));
  }

  @Override
  public final IndexDesc getIndexByColumnNames(final String databaseName,
                                           final String tableName,
                                           final String [] columnNames) {
    try {
      GetIndexByColumnNamesRequest.Builder builder = GetIndexByColumnNamesRequest.newBuilder();
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, tableName));
      for (String columnName : columnNames) {
        builder.addColumnNames(columnName);
      }

      final BlockingInterface stub = getStub();
      final IndexResponse response = stub.getIndexByColumnNames(null, builder.build());
      ensureOk(response.getState());

      return new IndexDesc(response.getIndexDesc());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Collection<IndexDesc> getAllIndexesByTable(final String databaseName,
                                                          final String tableName) {
    try {
      TableIdentifierProto proto = CatalogUtil.buildTableIdentifier(databaseName, tableName);

      final BlockingInterface stub = getStub();
      final IndexListResponse response = stub.getAllIndexesByTable(null, proto);
      ensureOk(response.getState());

      List<IndexDesc> indexDescs = TUtil.newList();
      for (IndexDescProto descProto : response.getIndexDescList()) {
        indexDescs.add(new IndexDesc(descProto));
      }
      return indexDescs;
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void dropIndex(final String dbName, final String indexName)
      throws UndefinedIndexException, UndefinedDatabaseException {
    try {
      final IndexNameProto request = IndexNameProto.newBuilder()
          .setDatabaseName(dbName)
          .setIndexName(indexName)
          .build();

      final BlockingInterface stub = getStub();
      final ReturnState state = stub.dropIndex(null, request);

      throwsIfThisError(state, UndefinedIndexException.class);
      throwsIfThisError(state, UndefinedDatabaseException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void createFunction(final FunctionDesc funcDesc) throws DuplicateFunctionException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.createFunction(null, funcDesc.getProto());

      throwsIfThisError(state, DuplicateFunctionException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void dropFunction(final String signature) throws UndefinedFunctionException,
      InsufficientPrivilegeException {

    try {
      final UnregisterFunctionRequest request = UnregisterFunctionRequest.newBuilder()
          .setSignature(signature)
          .build();
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.dropFunction(null, request);

      throwsIfThisError(state, UndefinedFunctionException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final FunctionDesc getFunction(final String signature, DataType... paramTypes)
      throws AmbiguousFunctionException, UndefinedFunctionException {
    return getFunction(signature, null, paramTypes);
  }

  @Override
  public final FunctionDesc getFunction(final String signature, FunctionType funcType, DataType... paramTypes)
      throws AmbiguousFunctionException, UndefinedFunctionException {

    final GetFunctionMetaRequest.Builder builder = GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    try {
      final BlockingInterface stub = getStub();
      final FunctionResponse response = stub.getFunctionMeta(null, builder.build());

      throwsIfThisError(response.getState(), UndefinedFunctionException.class);
      ensureOk(response.getState());
      return new FunctionDesc(response.getFunction());

    } catch (ServiceException se) {
      throw new RuntimeException(se);
    } catch (ClassNotFoundException e) {
      throw new TajoInternalError(e);
    }
  }

  @Override
  public final boolean containFunction(final String signature, DataType... paramTypes) {
    return containFunction(signature, null, paramTypes);
  }

  @Override
  public final boolean containFunction(final String signature, FunctionType funcType, DataType... paramTypes) {

    final ContainFunctionRequest.Builder builder = ContainFunctionRequest.newBuilder();

    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    builder.setSignature(signature);
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state  = stub.containFunction(null, builder.build());

      if (isThisError(state, UNDEFINED_FUNCTION)) {
        return false;
      }
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void alterTable(final AlterTableDesc desc) throws DuplicateDatabaseException,
      DuplicateTableException, DuplicateColumnException, DuplicatePartitionException,
      UndefinedDatabaseException, UndefinedTableException, UndefinedColumnException, UndefinedPartitionMethodException,
      InsufficientPrivilegeException, UndefinedPartitionException, NotImplementedException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.alterTable(null, desc.getProto());

      throwsIfThisError(state, DuplicateTableException.class);
      throwsIfThisError(state, DuplicateColumnException.class);
      throwsIfThisError(state, DuplicatePartitionException.class);
      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, UndefinedTableException.class);
      throwsIfThisError(state, UndefinedColumnException.class);
      throwsIfThisError(state, UndefinedPartitionException.class);
      throwsIfThisError(state, UndefinedPartitionMethodException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      throwsIfThisError(state, NotImplementedException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateTableStats(final UpdateTableStatsProto updateTableStatsProto)
      throws InsufficientPrivilegeException, UndefinedTableException {

    try {
      final BlockingInterface stub = getStub();
      final ReturnState state = stub.updateTableStats(null, updateTableStatsProto);

      throwsIfThisError(state, UndefinedTableException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }
}
