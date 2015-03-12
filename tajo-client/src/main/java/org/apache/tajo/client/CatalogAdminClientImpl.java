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
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.jdbc.SQLStates;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.ServerCallable;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;

public class CatalogAdminClientImpl implements CatalogAdminClient {
  private final SessionConnection connection;

  public CatalogAdminClientImpl(SessionConnection connection) {
    this.connection = connection;
  }

  @Override
  public boolean createDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMaster = client.getStub();
        return tajoMaster.createDatabase(null, connection.convertSessionedString(databaseName)).getValue();
      }

    }.withRetries();
  }

  @Override
  public boolean existDatabase(final String databaseName) throws ServiceException {

    return new ServerCallable<Boolean>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMaster = client.getStub();
        return tajoMaster.existDatabase(null, connection.convertSessionedString(databaseName)).getValue();
      }

    }.withRetries();
  }

  @Override
  public boolean dropDatabase(final String databaseName) throws ServiceException {

    return new ServerCallable<Boolean>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.dropDatabase(null, connection.convertSessionedString(databaseName)).getValue();
      }

    }.withRetries();
  }

  @Override
  public List<String> getAllDatabaseNames() throws ServiceException {

    return new ServerCallable<List<String>>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public List<String> call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getAllDatabases(null, connection.sessionId).getValuesList();
      }

    }.withRetries();
  }

  public boolean existTable(final String tableName) throws ServiceException {

    return new ServerCallable<Boolean>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existTable(null, connection.convertSessionedString(tableName)).getValue();
      }

    }.withRetries();
  }

  @Override
  public TableDesc createExternalTable(String tableName, Schema schema, URI path, TableMeta meta)
      throws SQLException, ServiceException {
    return createExternalTable(tableName, schema, path, meta, null);
  }

  public TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws SQLException, ServiceException {

    return new ServerCallable<TableDesc>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();

        ClientProtos.CreateTableRequest.Builder builder = ClientProtos.CreateTableRequest.newBuilder();
        builder.setSessionId(connection.sessionId);
        builder.setName(tableName);
        builder.setSchema(schema.getProto());
        builder.setMeta(meta.getProto());
        builder.setPath(path.toString());
        if (partitionMethodDesc != null) {
          builder.setPartition(partitionMethodDesc.getProto());
        }
        ClientProtos.TableResponse res = tajoMasterService.createExternalTable(null, builder.build());
        if (res.getResultCode() == ClientProtos.ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }

    }.withRetries();
  }

  @Override
  public boolean dropTable(String tableName) throws ServiceException {
    return dropTable(tableName, false);
  }

  @Override
  public boolean dropTable(final String tableName, final boolean purge) throws ServiceException {

    return new ServerCallable<Boolean>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();

        ClientProtos.DropTableRequest.Builder builder = ClientProtos.DropTableRequest.newBuilder();
        builder.setSessionId(connection.sessionId);
        builder.setName(tableName);
        builder.setPurge(purge);
        return tajoMasterService.dropTable(null, builder.build()).getValue();
      }

    }.withRetries();

  }

  @Override
  public List<String> getTableList(@Nullable final String databaseName) throws ServiceException {
    return new ServerCallable<List<String>>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public List<String> call(NettyClientBase client) throws ServiceException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();

        ClientProtos.GetTableListRequest.Builder builder = ClientProtos.GetTableListRequest.newBuilder();
        builder.setSessionId(connection.sessionId);
        if (databaseName != null) {
          builder.setDatabaseName(databaseName);
        }
        ClientProtos.GetTableListResponse res = tajoMasterService.getTableList(null, builder.build());
        return res.getTablesList();
      }

    }.withRetries();
  }

  @Override
  public TableDesc getTableDesc(final String tableName) throws ServiceException {

    return new ServerCallable<TableDesc>(connection.connPool, connection.getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();

        ClientProtos.GetTableDescRequest.Builder builder = ClientProtos.GetTableDescRequest.newBuilder();
        builder.setSessionId(connection.sessionId);
        builder.setTableName(tableName);
        ClientProtos.TableResponse res = tajoMasterService.getTableDesc(null, builder.build());
        if (res.getResultCode() == ClientProtos.ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }

    }.withRetries();
  }

  @Override
  public List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName) throws ServiceException {

    return new ServerCallable<List<CatalogProtos.FunctionDescProto>>(connection.connPool,
        connection.getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public List<CatalogProtos.FunctionDescProto> call(NettyClientBase client) throws ServiceException, SQLException {

        connection.checkSessionAndGet(client);
        BlockingInterface tajoMasterService = client.getStub();

        String paramFunctionName = functionName == null ? "" : functionName;
        ClientProtos.FunctionResponse res = tajoMasterService.getFunctionList(null,
            connection.convertSessionedString(paramFunctionName));
        if (res.getResultCode() == ClientProtos.ResultCode.OK) {
          return res.getFunctionsList();
        } else {
          throw new SQLException(res.getErrorMessage());
        }
      }

    }.withRetries();
  }

  @Override
  public void close() throws IOException {
  }
}
