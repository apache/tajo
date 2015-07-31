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
    }

    return response;

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
    throwIfError(response.getState());

    QueryId queryId = new QueryId(response.getQueryId());

    switch (response.getResultType()) {
      case ENCLOSED:
        return TajoClientUtil.createResultSet(this, response, defaultFetchRows);
      case FETCH:
        return this.getQueryResultAndWait(queryId);
      default:
        return this.createNullResultSet(queryId);
    }
  }

  @Override
  public ResultSet executeJsonQueryAndGetResult(final String json) throws SQLException {

    ClientProtos.SubmitQueryResponse response = executeQueryWithJson(json);
    throwIfError(response.getState());

    QueryId queryId = new QueryId(response.getQueryId());

    switch (response.getResultType()) {
    case ENCLOSED:
      return TajoClientUtil.createResultSet(this, response, defaultFetchRows);
    case FETCH:
      return this.getQueryResultAndWait(queryId);
    default:
      return this.createNullResultSet(queryId);
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

    final BlockingInterface stub = conn.getTMStub();
    final GetQueryResultDataRequest request = GetQueryResultDataRequest.newBuilder()
        .setSessionId(conn.sessionId)
        .setQueryId(queryId.getProto())
        .setFetchRowNum(fetchRowNum)
        .build();

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

    UpdateQueryResponse response;
    try {
      response = stub.updateQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    conn.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));

    return true;
  }

  @Override
  public boolean updateQueryWithJson(final String json) throws SQLException {

    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(json, true);

    UpdateQueryResponse response;
    try {
      response = stub.updateQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
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
