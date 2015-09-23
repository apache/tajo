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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.*;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.DropTableRequest;
import org.apache.tajo.ipc.ClientProtos.GetIndexWithColumnsRequest;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListResponse;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.apache.tajo.exception.ExceptionUtil.throwsIfThisError;
import static org.apache.tajo.exception.ReturnStateUtil.*;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;

public class CatalogAdminClientImpl implements CatalogAdminClient {
  private final SessionConnection conn;

  public CatalogAdminClientImpl(SessionConnection conn) {
    this.conn = conn;
  }

  @Override
  public void createDatabase(final String databaseName) throws DuplicateDatabaseException {



    try {
      final BlockingInterface stub = conn.getTMStub();
      final ReturnState state = stub.createDatabase(null, conn.getSessionedString(databaseName));

      throwsIfThisError(state, DuplicateDatabaseException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean existDatabase(final String databaseName) {

    try {
      final BlockingInterface stub = conn.getTMStub();
      final ReturnState state = stub.existDatabase(null, conn.getSessionedString(databaseName));

      if (isThisError(state, ResultCode.UNDEFINED_DATABASE)) {
        return false;
      }
      ensureOk(state);
      return true;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void dropDatabase(final String databaseName)
      throws UndefinedDatabaseException, InsufficientPrivilegeException {

    try {
      final BlockingInterface stub = conn.getTMStub();
      final ReturnState state = stub.dropDatabase(null, conn.getSessionedString(databaseName));

      throwsIfThisError(state, UndefinedDatabaseException.class);
      throwsIfThisError(state, InsufficientPrivilegeException.class);
      ensureOk(state);

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getAllDatabaseNames() {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return stub.getAllDatabases(null, conn.sessionId).getValuesList();
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean existTable(final String tableName) {

    final BlockingInterface stub = conn.getTMStub();

    ReturnState state;
    try {
      state = stub.existTable(null, conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isThisError(state, ResultCode.UNDEFINED_TABLE)) {
      return false;
    }

    ensureOk(state);
    return true;
  }

  @Override
  public TableDesc createExternalTable(String tableName, @Nullable Schema schema, URI path, TableMeta meta)
      throws DuplicateTableException, UnavailableTableLocationException, InsufficientPrivilegeException {
    return createExternalTable(tableName, schema, path, meta, null);
  }

  @Override
  public TableDesc createExternalTable(final String tableName, @Nullable final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws DuplicateTableException, InsufficientPrivilegeException, UnavailableTableLocationException {

    final NettyClientBase client = conn.getTajoMasterConnection();
    conn.checkSessionAndGet(client);
    final BlockingInterface tajoMasterService = client.getStub();

    final ClientProtos.CreateTableRequest.Builder builder = ClientProtos.CreateTableRequest.newBuilder();
    builder.setSessionId(conn.sessionId);
    builder.setName(tableName);
    if (schema != null) {
      builder.setSchema(schema.getProto());
    }
    builder.setMeta(meta.getProto());
    builder.setPath(path.toString());

    if (partitionMethodDesc != null) {
      builder.setPartition(partitionMethodDesc.getProto());
    }

    TableResponse res;
    try {
      res = tajoMasterService.createExternalTable(null, builder.build());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwsIfThisError(res.getState(), DuplicateTableException.class);
    throwsIfThisError(res.getState(), InsufficientPrivilegeException.class);
    throwsIfThisError(res.getState(), UnavailableTableLocationException.class);

    ensureOk(res.getState());
    return CatalogUtil.newTableDesc(res.getTable());
  }

  @Override
  public void dropTable(String tableName) throws UndefinedTableException, InsufficientPrivilegeException {
    dropTable(tableName, false);
  }

  @Override
  public void dropTable(final String tableName, final boolean purge)
      throws UndefinedTableException, InsufficientPrivilegeException {

    final BlockingInterface stub = conn.getTMStub();
    final DropTableRequest request = DropTableRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setName(tableName)
        .setPurge(purge)
        .build();


    ReturnState state;
    try {
      state = stub.dropTable(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwsIfThisError(state, UndefinedTableException.class);
    throwsIfThisError(state, InsufficientPrivilegeException.class);

    ensureOk(state);
  }

  @Override
  public List<String> getTableList(@Nullable final String databaseName) {

    final BlockingInterface stub = conn.getTMStub();

    StringListResponse response;
    try {
      response = stub.getTableList(null, conn.getSessionedString(databaseName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    return response.getValuesList();
  }

  @Override
  public TableDesc getTableDesc(final String tableName) throws UndefinedTableException {

    final BlockingInterface stub = conn.getTMStub();

    TableResponse res;
    try {
      res = stub.getTableDesc(null, conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isThisError(res.getState(), ResultCode.UNDEFINED_TABLE)) {
      throw new UndefinedTableException(res.getState());
    }

    ensureOk(res.getState());
    return CatalogUtil.newTableDesc(res.getTable());
  }

  @Override
  public List<PartitionDescProto> getAllPartitions(final String tableName) throws UndefinedDatabaseException,
    UndefinedTableException, UndefinedPartitionMethodException {

    final BlockingInterface stub = conn.getTMStub();

    PartitionListResponse response;
    try {
      response = stub.getPartitionsByTableName(null,
        conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwsIfThisError(response.getState(), UndefinedDatabaseException.class);
    throwsIfThisError(response.getState(), UndefinedTableException.class);
    throwsIfThisError(response.getState(), UndefinedPartitionMethodException.class);

    ensureOk(response.getState());
    return response.getPartitionList();
  }

  @Override
  public List<FunctionDescProto> getFunctions(final String functionName) {

    final BlockingInterface stub = conn.getTMStub();

    String paramFunctionName = functionName == null ? "" : functionName;
    CatalogProtos.FunctionListResponse res;
    try {
      res = stub.getFunctionList(null, conn.getSessionedString(paramFunctionName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(res.getState());
    return res.getFunctionList();
  }

  @Override
  public IndexDescProto getIndex(final String indexName) {
    final BlockingInterface stub = conn.getTMStub();

    IndexResponse res;
    try {
      res = stub.getIndexWithName(null, conn.getSessionedString(indexName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(res.getState());
    return res.getIndexDesc();
  }

  @Override
  public boolean existIndex(final String indexName){
    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existIndexWithName(null, conn.getSessionedString(indexName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<IndexDescProto> getIndexes(final String tableName) {
    final BlockingInterface stub = conn.getTMStub();

    IndexListResponse response;
    try {
      response = stub.getIndexesForTable(null,
          conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    return response.getIndexDescList();
  }

  @Override
  public boolean hasIndexes(final String tableName) {
    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existIndexesForTable(null, conn.getSessionedString(tableName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IndexDescProto getIndex(final String tableName, final String[] columnNames) {
    final BlockingInterface stub = conn.getTMStub();

    GetIndexWithColumnsRequest.Builder builder = GetIndexWithColumnsRequest.newBuilder();
    builder.setSessionId(conn.sessionId);
    builder.setTableName(tableName);
    for (String eachColumnName : columnNames) {
      builder.addColumnNames(eachColumnName);
    }

    IndexResponse response;
    try {
      response = stub.getIndexWithColumns(null, builder.build());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    return response.getIndexDesc();
  }

  @Override
  public boolean existIndex(final String tableName, final String[] columnName) {
    final BlockingInterface stub = conn.getTMStub();

    GetIndexWithColumnsRequest.Builder builder = GetIndexWithColumnsRequest.newBuilder();
    builder.setSessionId(conn.sessionId);
    builder.setTableName(tableName);
    for (String eachColumnName : columnName) {
      builder.addColumnNames(eachColumnName);
    }

    try {
      return isSuccess(stub.existIndexWithColumns(null, builder.build()));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropIndex(final String indexName) {
    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.dropIndex(null, conn.getSessionedString(indexName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
