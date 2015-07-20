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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.*;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.util.ProtoUtil;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.exception.ReturnStateUtil.isSuccess;
import static org.apache.tajo.exception.ReturnStateUtil.returnError;
import static org.apache.tajo.exception.SQLExceptionUtil.throwIfError;
import static org.apache.tajo.ipc.ClientProtos.*;
import static org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;

public class QueryClientImpl implements QueryClient {
  private static final Log LOG = LogFactory.getLog(QueryClientImpl.class);
  private final SessionConnection conn;
  private final int defaultFetchRows;
  // maxRows number is limit value of resultSet. The value must be >= 0, and 0 means there is not limit.
  private int maxRows;

  public QueryClientImpl(SessionConnection conn) {
    this.conn = conn;
    this.defaultFetchRows = this.conn.getProperties().getInt(SessionVars.FETCH_ROWNUM.getConfVars().keyname(),
        SessionVars.FETCH_ROWNUM.getConfVars().defaultIntVal);
    this.maxRows = 0;
  }

  @Override
  public boolean isConnected() {
    return conn.isConnected();
  }

  @Override
  public String getSessionId() {
    return conn.getSessionId();
  }

  @Override
  public Map<String, String> getClientSideSessionVars() {
    return conn.getClientSideSessionVars();
  }

  @Override
  public String getBaseDatabase() {
    return conn.getBaseDatabase();
  }

  @Override
  public void close() {
  }

  @Override
  public UserRoleInfo getUserInfo() {
    return conn.getUserInfo();
  }

  @Override
  public void closeQuery(QueryId queryId) throws SQLException {
    closeNonForwardQuery(queryId);
  }

  @Override
  public void closeNonForwardQuery(QueryId queryId) throws SQLException {
    try {
      throwIfError(conn.getTMStub().closeNonForwardQuery(null, buildQueryIdRequest(queryId)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getCurrentDatabase() throws SQLException {
    return conn.getCurrentDatabase();
  }

  @Override
  public Boolean selectDatabase(String databaseName) throws SQLException {
    return conn.selectDatabase(databaseName);
  }

  @Override
  public Map<String, String> updateSessionVariables(Map<String, String> variables) throws SQLException {
    return conn.updateSessionVariables(variables);
  }

  @Override
  public Map<String, String> unsetSessionVariables(List<String> variables) throws SQLException {
    return conn.unsetSessionVariables(variables);
  }

  @Override
  public String getSessionVariable(String varname) throws SQLException {
    return conn.getSessionVariable(varname);
  }

  @Override
  public Boolean existSessionVariable(String varname) throws SQLException {
    return conn.existSessionVariable(varname);
  }

  @Override
  public Map<String, String> getAllSessionVariables() throws SQLException {
    return conn.getAllSessionVariables();
  }

  @Override
  public ClientProtos.SubmitQueryResponse executeQuery(final String sql) throws SQLException {

<<<<<<< HEAD
    final QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(sql);
    builder.setIsJson(false);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    SubmitQueryResponse response = tajoMasterService.submitQuery(null, builder.build());
    if (response.getResult().getResultCode() == ResultCode.OK) {
      connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
=======
    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(sql, false);

    SubmitQueryResponse response;
    try {
      response = stub.submitQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isSuccess(response.getState())) {
      conn.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    }

    return response;

//<<<<<<< HEAD
//        connection.checkSessionAndGet(client);
//
//        final QueryRequest.Builder builder = QueryRequest.newBuilder();
//        builder.setSessionId(connection.sessionId);
//        builder.setQuery(sql);
//        builder.setIsJson(false);
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//
//
//
//      }
//    }.withRetries();
//=======
//    SubmitQueryResponse response = tajoMasterService.submitQuery(null, builder.build());
//    if (response.getResultCode() == ResultCode.OK) {
//      connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
//    }
//    return response;
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
  }

  @Override
  public ClientProtos.SubmitQueryResponse executeQueryWithJson(final String json) throws SQLException {
    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(json, true);

    try {
      return stub.submitQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResultSet executeQueryAndGetResult(String sql) throws SQLException {

    ClientProtos.SubmitQueryResponse response = executeQuery(sql);
<<<<<<< HEAD

    if (response.getResult().getResultCode() == ClientProtos.ResultCode.ERROR) {
      if (response.getResult().hasErrorMessage()) {
        throw new ServiceException(response.getResult().getErrorMessage());
      } else if (response.getResult().hasErrorTrace()) {
        throw new ServiceException(response.getResult().getErrorTrace());
      }
    }
=======
    throwIfError(response.getState());
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867

    QueryId queryId = new QueryId(response.getQueryId());

    if (response.getIsForwarded()) {
      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
        return this.createNullResultSet(queryId);
      } else {
        return this.getQueryResultAndWait(queryId);
      }

    } else {
      // If a non-forwarded insert into query
      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID) && response.getMaxRowNum() == 0) {
        return this.createNullResultSet(queryId);
      } else {
        if (response.hasResultSet() || response.hasTableDesc()) {
          return TajoClientUtil.createResultSet(this, response, defaultFetchRows);
        } else {
          return this.createNullResultSet(queryId);
        }
      }
    }
  }

  @Override
  public ResultSet executeJsonQueryAndGetResult(final String json) throws SQLException {

    ClientProtos.SubmitQueryResponse response = executeQueryWithJson(json);
<<<<<<< HEAD

    if (response.getResult().getResultCode() == ClientProtos.ResultCode.ERROR) {
      throw new ServiceException(response.getResult().getErrorTrace());
    }
=======
    throwIfError(response.getState());
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867

    QueryId queryId = new QueryId(response.getQueryId());
    if (response.getIsForwarded()) {

      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
        return this.createNullResultSet(queryId);
      } else {
        return this.getQueryResultAndWait(queryId);
      }

    } else {

      if (response.hasResultSet() || response.hasTableDesc()) {
        return TajoClientUtil.createResultSet(this, response, defaultFetchRows);
      } else {
        return this.createNullResultSet(queryId);
      }

    }
  }

  private ResultSet getQueryResultAndWait(QueryId queryId) throws SQLException {

    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }

    QueryStatus status = TajoClientUtil.waitCompletion(this, queryId);

    if (status.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
      if (status.hasResult()) {
        return getQueryResult(queryId);
      } else {
        return createNullResultSet(queryId);
      }

    } else {
      LOG.warn("Query (" + status.getQueryId() + ") failed: " + status.getState());

      //TODO throw SQLException(?)
      return createNullResultSet(queryId);
    }
  }

  @Override
  public QueryStatus getQueryStatus(QueryId queryId) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final GetQueryStatusRequest request = GetQueryStatusRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setQueryId(queryId.getProto())
        .build();

    GetQueryStatusResponse res;
    try {
      res = stub.getQueryStatus(null, request);
    } catch (ServiceException t) {
      throw new RuntimeException(t);
    }

    throwIfError(res.getState());
    return new QueryStatus(res);
  }

  @Override
  public ResultSet getQueryResult(QueryId queryId) throws SQLException {

    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }

    GetQueryResultResponse response = getResultResponse(queryId);
    throwIfError(response.getState());
    TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
    return new FetchResultSet(this, tableDesc.getLogicalSchema(), queryId, defaultFetchRows);
  }

  @Override
  public ResultSet createNullResultSet(QueryId queryId) {
    return TajoClientUtil.createNullResultSet(queryId);
  }

  @Override
  public GetQueryResultResponse getResultResponse(QueryId queryId) throws SQLException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return null;
    }

    final BlockingInterface stub = conn.getTMStub();
    final GetQueryResultRequest request = GetQueryResultRequest.newBuilder()
        .setQueryId(queryId.getProto())
        .setSessionId(conn.sessionId)
        .build();

    GetQueryResultResponse response;
    try {
      response = stub.getQueryResult(null, request);
    } catch (ServiceException t) {
      throw new RuntimeException(t);
    }

    throwIfError(response.getState());
    return response;
  }

  @Override
  public TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum) throws SQLException {

<<<<<<< HEAD
    try {
//<<<<<<< HEAD
//      final ServerCallable<ClientProtos.SerializedResultSet> callable =
//          new ServerCallable<ClientProtos.SerializedResultSet>(connection.manager, connection.getTajoMasterAddr(),
//              TajoMasterClientProtocol.class, false) {
//
//            public ClientProtos.SerializedResultSet call(NettyClientBase client) throws ServiceException {
//
//              connection.checkSessionAndGet(client);
//              TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//
//              GetQueryResultDataRequest.Builder builder = GetQueryResultDataRequest.newBuilder();
//              builder.setSessionId(connection.sessionId);
//              builder.setQueryId(queryId.getProto());
//              builder.setFetchRowNum(fetchRowNum);
//              try {
//                GetQueryResultDataResponse response = tajoMasterService.getQueryResultData(null, builder.build());
//                if (response.getResult().getResultCode() == ClientProtos.ResultCode.ERROR) {
//                  abort();
//                  throw new ServiceException(response.getResult().getErrorMessage());
//                }
//
//                return response.getResultSet();
//              } catch (ServiceException e) {
//                abort();
//                throw e;
//              } catch (Throwable t) {
//                throw new ServiceException(t.getMessage(), t);
//              }
//            }
//          };
//
//      ClientProtos.SerializedResultSet serializedResultSet = callable.withRetries();
//=======
      NettyClientBase client = connection.getTajoMasterConnection();
      connection.checkSessionAndGet(client);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

      GetQueryResultDataRequest.Builder builder = GetQueryResultDataRequest.newBuilder();
      builder.setSessionId(connection.sessionId);
      builder.setQueryId(queryId.getProto());
      builder.setFetchRowNum(fetchRowNum);

      GetQueryResultDataResponse response = tajoMasterService.getQueryResultData(null, builder.build());
      if (response.getResult().getResultCode() == ClientProtos.ResultCode.ERROR) {
        throw new ServiceException(response.getResult().getErrorMessage());
      }

      ClientProtos.SerializedResultSet resultSet = response.getResultSet();
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
=======
    final BlockingInterface stub = conn.getTMStub();
    final GetQueryResultDataRequest request = GetQueryResultDataRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setQueryId(queryId.getProto())
        .setFetchRowNum(fetchRowNum)
        .build();

>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867

    GetQueryResultDataResponse response;
    try {
      response = stub.getQueryResultData(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());

    ClientProtos.SerializedResultSet resultSet = response.getResultSet();
    return new TajoMemoryResultSet(queryId,
        new Schema(resultSet.getSchema()),
        resultSet.getSerializedTuplesList(),
        resultSet.getSerializedTuplesCount(),
        getClientSideSessionVars());
  }

  @Override
  public boolean updateQuery(final String sql) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(sql, false);

<<<<<<< HEAD
//<<<<<<< HEAD
//        connection.checkSessionAndGet(client);
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//
//        QueryRequest.Builder builder = QueryRequest.newBuilder();
//        builder.setSessionId(connection.sessionId);
//        builder.setQuery(sql);
//        builder.setIsJson(false);
//        ClientProtos.UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
//
//        if (response.getResult().getResultCode() == ClientProtos.ResultCode.OK) {
//          connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
//          return true;
//        } else {
//          if (response.getResult().hasErrorMessage()) {
//            System.err.println("ERROR: " + response.getResult().getErrorMessage());
//          }
//          return false;
//        }
//=======
    if (response.getResult().getResultCode() == ClientProtos.ResultCode.OK) {
      connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return true;
    } else {
      if (response.getResult().hasErrorMessage()) {
        LOG.error("ERROR: " + response.getResult().getErrorMessage());
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
      }
      return false;
=======
    UpdateQueryResponse response;
    try {
      response = stub.updateQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    }

    throwIfError(response.getState());
    conn.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));

    return true;
  }

  @Override
  public boolean updateQueryWithJson(final String json) throws SQLException {

<<<<<<< HEAD
//<<<<<<< HEAD
//    return new ServerCallable<Boolean>(connection.manager, connection.getTajoMasterAddr(),
//        TajoMasterClientProtocol.class, false) {
//
//      public Boolean call(NettyClientBase client) throws ServiceException {
//
//        connection.checkSessionAndGet(client);
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//
//        QueryRequest.Builder builder = QueryRequest.newBuilder();
//        builder.setSessionId(connection.sessionId);
//        builder.setQuery(json);
//        builder.setIsJson(true);
//        ClientProtos.UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
//        if (response.getResult().getResultCode() == ClientProtos.ResultCode.OK) {
//          return true;
//        } else {
//          if (response.getResult().hasErrorMessage()) {
//            System.err.println("ERROR: " + response.getResult().getErrorMessage());
//          }
//          return false;
//        }
//=======
    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(json);
    builder.setIsJson(true);
    ClientProtos.UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
    if (response.getResult().getResultCode() == ClientProtos.ResultCode.OK) {
      return true;
    } else {
      if (response.getResult().hasErrorMessage()) {
        LOG.error("ERROR: " + response.getResult().getErrorMessage());
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
      }
      return false;
=======
    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(json, true);

    UpdateQueryResponse response;
    try {
      response = stub.updateQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    }

    throwIfError(response.getState());
    return true;
  }

  @Override
  public List<ClientProtos.BriefQueryInfo> getRunningQueryList() throws SQLException {

    final BlockingInterface stmb = conn.getTMStub();

    GetQueryListResponse res;
    try {
      res = stmb.getRunningQueryList(null, conn.sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getQueryListList();
  }

  @Override
  public List<ClientProtos.BriefQueryInfo> getFinishedQueryList() throws SQLException {

    final BlockingInterface stub = conn.getTMStub();

    GetQueryListResponse res;
    try {
      res = stub.getFinishedQueryList(null, conn.sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getQueryListList();
  }

  @Override
  public List<ClientProtos.WorkerResourceInfo> getClusterInfo() throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final GetClusterInfoRequest request = GetClusterInfoRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .build();

    GetClusterInfoResponse res;
    try {
      res = stub.getClusterInfo(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getWorkerListList();
  }

  @Override
  public QueryStatus killQuery(final QueryId queryId) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    QueryStatus status = getQueryStatus(queryId);

    /* send a kill to the TM */
    QueryIdRequest request = buildQueryIdRequest(queryId);
    try {
      stub.killQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }


    long currentTimeMillis = System.currentTimeMillis();
    long timeKillIssued = currentTimeMillis;
    while ((currentTimeMillis < timeKillIssued + 10000L)
        && ((status.getState() != TajoProtos.QueryState.QUERY_KILLED)
        || (status.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT))) {
      try {
        Thread.sleep(100L);
      } catch (InterruptedException ie) {
        break;
      }
      currentTimeMillis = System.currentTimeMillis();
      status = getQueryStatus(queryId);
    }

    return status;
  }

  @Override
  public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
  }
  
  @Override
  public int getMaxRows() {
  	return this.maxRows;
  }
  
<<<<<<< HEAD
  public QueryInfoProto getQueryInfo(final QueryId queryId) throws ServiceException {
//<<<<<<< HEAD
//    return new ServerCallable<QueryInfoProto>(connection.manager, connection.getTajoMasterAddr(),
//        TajoMasterClientProtocol.class, false) {
//      public QueryInfoProto call(NettyClientBase client) throws ServiceException {
//        connection.checkSessionAndGet(client);
//
//        QueryIdRequest.Builder builder = QueryIdRequest.newBuilder();
//        builder.setSessionId(connection.sessionId);
//        builder.setQueryId(queryId.getProto());
//
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//        GetQueryInfoResponse res = tajoMasterService.getQueryInfo(null,builder.build());
//        if (res.getResult().getResultCode() == ResultCode.OK) {
//          return res.getQueryInfo();
//        } else {
//          abort();
//          throw new ServiceException(res.getResult().getErrorMessage());
//        }
//      }
//    }.withRetries();
//=======
    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);

    QueryIdRequest.Builder builder = QueryIdRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQueryId(queryId.getProto());

    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    GetQueryInfoResponse res = tajoMasterService.getQueryInfo(null,builder.build());
    if (res.getResult().getResultCode() == ResultCode.OK) {
      return res.getQueryInfo();
    } else {
      throw new ServiceException(res.getResult().getErrorMessage());
    }
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
=======
  public QueryInfoProto getQueryInfo(final QueryId queryId) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final QueryIdRequest request = buildQueryIdRequest(queryId);

    GetQueryInfoResponse res;
    try {
      res = stub.getQueryInfo(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(res.getState());
    return res.getQueryInfo();
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
  }

  public QueryHistoryProto getQueryHistory(final QueryId queryId) throws SQLException {
    final QueryInfoProto queryInfo = getQueryInfo(queryId);

    if (queryInfo.getHostNameOfQM() == null || queryInfo.getQueryMasterClientPort() == 0) {
      return null;
    }

    InetSocketAddress qmAddress = new InetSocketAddress(
        queryInfo.getHostNameOfQM(), queryInfo.getQueryMasterClientPort());

    RpcClientManager manager = RpcClientManager.getInstance();
    NettyClientBase qmClient = null;

    try {
<<<<<<< HEAD
      connection.checkSessionAndGet(connection.getTajoMasterConnection());

//<<<<<<< HEAD
//        QueryMasterClientProtocolService.BlockingInterface queryMasterService = client.getStub();
//        GetQueryHistoryResponse res = queryMasterService.getQueryHistory(null,builder.build());
//        if (res.getResult().getResultCode() == ResultCode.OK) {
//          return res.getQueryHistory();
//        } else {
//          abort();
//          throw new ServiceException(res.getResult().getErrorMessage());
//        }
//=======
      QueryIdRequest.Builder builder = QueryIdRequest.newBuilder();
      builder.setSessionId(connection.sessionId);
      builder.setQueryId(queryId.getProto());

      QueryMasterClientProtocolService.BlockingInterface queryMasterService = queryMasterClient.getStub();
      GetQueryHistoryResponse res = queryMasterService.getQueryHistory(null, builder.build());
      if (res.getResult().getResultCode() == ResultCode.OK) {
        return res.getQueryHistory();
      } else {
        throw new ServiceException(res.getResult().getErrorMessage());
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
=======

      qmClient = manager.newClient(
          qmAddress,
          QueryMasterClientProtocol.class,
          false,
          manager.getRetries(),
          manager.getTimeoutSeconds(),
          TimeUnit.SECONDS,
          false
      );

      conn.checkSessionAndGet(conn.getTajoMasterConnection());

      QueryIdRequest request = QueryIdRequest.newBuilder()
          .setSessionId(conn.sessionId)
          .setQueryId(queryId.getProto())
          .build();

      QueryMasterClientProtocolService.BlockingInterface stub = qmClient.getStub();
      GetQueryHistoryResponse res;
      try {
        res = stub.getQueryHistory(null, request);
      } catch (ServiceException e) {
        throw new RuntimeException(e);
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
      }

      throwIfError(res.getState());
      return res.getQueryHistory();

    } catch (ConnectException e) {
      throw SQLExceptionUtil.makeUnableToEstablishConnection(e);
    } catch (ClassNotFoundException e) {
      throw SQLExceptionUtil.makeUnableToEstablishConnection(e);
    } catch (NoSuchMethodException e) {
      throw SQLExceptionUtil.makeUnableToEstablishConnection(e);
    } catch (SQLException e) {
      throw e;
    } finally {
      qmClient.close();
    }
  }

  private QueryIdRequest buildQueryIdRequest(QueryId queryId) {
    return ClientProtos.QueryIdRequest.newBuilder()
        .setSessionId(SessionIdProto.newBuilder().setId(getSessionId()))
        .setQueryId(queryId.getProto())
        .build();
  }

  private QueryRequest buildQueryRequest(String query, boolean json) {
    return QueryRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setQuery(query)
        .setIsJson(json)
        .build();
  }
}
