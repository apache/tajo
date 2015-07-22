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
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.DropTableRequest;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListResponse;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

import static org.apache.tajo.exception.ReturnStateUtil.isSuccess;
import static org.apache.tajo.exception.SQLExceptionUtil.throwIfError;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;

public class CatalogAdminClientImpl implements CatalogAdminClient {
  private final SessionConnection conn;

  public CatalogAdminClientImpl(SessionConnection conn) {
    this.conn = conn;
  }

  @Override
  public boolean createDatabase(final String databaseName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.createDatabase(null, conn.getSessionedString(databaseName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean existDatabase(final String databaseName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existDatabase(null, conn.getSessionedString(databaseName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropDatabase(final String databaseName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.dropDatabase(null, conn.getSessionedString(databaseName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getAllDatabaseNames() throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return stub.getAllDatabases(null, conn.sessionId).getValuesList();
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean existTable(final String tableName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existTable(null, conn.getSessionedString(tableName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TableDesc createExternalTable(String tableName, Schema schema, URI path, TableMeta meta)
      throws SQLException {
    return createExternalTable(tableName, schema, path, meta, null);
  }

  public TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws SQLException {

    NettyClientBase client = conn.getTajoMasterConnection();
    conn.checkSessionAndGet(client);
    BlockingInterface tajoMasterService = client.getStub();

    ClientProtos.CreateTableRequest.Builder builder = ClientProtos.CreateTableRequest.newBuilder();
    builder.setSessionId(conn.sessionId);
    builder.setName(tableName);
    builder.setSchema(schema.getProto());
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

    if (isSuccess(res.getState())) {
      return CatalogUtil.newTableDesc(res.getTable());
    } else {
      throw SQLExceptionUtil.toSQLException(res.getState());
    }
  }

  @Override
  public boolean dropTable(String tableName) throws SQLException {
    return dropTable(tableName, false);
  }

  @Override
  public boolean dropTable(final String tableName, final boolean purge) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final DropTableRequest request = DropTableRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setName(tableName)
        .setPurge(purge)
        .build();

    try {
      return isSuccess(stub.dropTable(null, request));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getTableList(@Nullable final String databaseName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    StringListResponse response;
    try {
      response = stub.getTableList(null, conn.getSessionedString(databaseName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    return response.getValuesList();
  }

  @Override
  public TableDesc getTableDesc(final String tableName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    TableResponse res;
    try {
      res = stub.getTableDesc(null, conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return CatalogUtil.newTableDesc(res.getTable());
  }

  @Override
  public List<FunctionDescProto> getFunctions(final String functionName) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    String paramFunctionName = functionName == null ? "" : functionName;
    CatalogProtos.FunctionListResponse res;
    try {
      res = stub.getFunctionList(null, conn.getSessionedString(paramFunctionName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getFunctionList();
  }

  @Override
  public IndexDescProto getIndex(final String indexName) throws SQLException {
    final BlockingInterface stub = conn.getTMStub();

    IndexResponse res;
    try {
      res = stub.getIndexWithName(null, conn.getSessionedString(indexName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getIndexDesc();
  }

  @Override
  public boolean existIndex(final String indexName) throws SQLException {
    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existIndexWithName(null, conn.getSessionedString(indexName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<IndexDescProto> getIndexes(final String tableName) throws SQLException {
    final BlockingInterface stub = conn.getTMStub();

    IndexListResponse response;
    try {
      response = stub.getIndexesForTable(null,
          conn.getSessionedString(tableName));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    return response.getIndexDescList();
  }

  @Override
  public boolean hasIndexes(final String tableName) throws SQLException {
    final BlockingInterface stub = conn.getTMStub();

    try {
      return isSuccess(stub.existIndexesForTable(null, conn.getSessionedString(tableName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IndexDescProto getIndex(final String tableName, final String[] columnNames) throws SQLException {
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

    throwIfError(response.getState());
    return response.getIndexDesc();
  }

  @Override
  public boolean existIndex(final String tableName, final String[] columnName) throws SQLException {
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
  public boolean dropIndex(final String indexName) throws SQLException {
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
