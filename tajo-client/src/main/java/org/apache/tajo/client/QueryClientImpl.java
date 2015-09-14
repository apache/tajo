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

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.TajoProtos.CodecType;
import org.apache.tajo.exception.*;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.exception.ExceptionUtil.throwIfError;
import static org.apache.tajo.exception.ExceptionUtil.throwsIfThisError;
import static org.apache.tajo.exception.ReturnStateUtil.ensureOk;
import static org.apache.tajo.exception.ReturnStateUtil.isSuccess;
import static org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;

public class QueryClientImpl implements QueryClient {
  private static final Log LOG = LogFactory.getLog(QueryClientImpl.class);
  private static final CodecType DEFAULT_CODEC = CodecType.SNAPPY;
  private final ExecutorService executor;
  private final SessionConnection conn;
  private final int defaultFetchRows;
  // maxRows number is limit value of resultSet. The value must be >= 0, and 0 means there is not limit.
  private int maxRows;

  public QueryClientImpl(SessionConnection conn) {
    this.conn = conn;
    this.defaultFetchRows = this.conn.getProperties().getInt(SessionVars.FETCH_ROWNUM.getConfVars().keyname(),
        SessionVars.FETCH_ROWNUM.getConfVars().defaultIntVal);
    this.maxRows = 0;
    this.executor = Executors.newSingleThreadExecutor();
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
    executor.shutdown();
  }

  @Override
  public UserRoleInfo getUserInfo() {
    return conn.getUserInfo();
  }

  @Override
  public void closeQuery(QueryId queryId) {
    closeNonForwardQuery(queryId);
  }

  @Override
  public void closeNonForwardQuery(QueryId queryId) {
    try {
      ensureOk(conn.getTMStub().closeNonForwardQuery(null, buildQueryIdRequest(queryId)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getCurrentDatabase() {
    return conn.getCurrentDatabase();
  }

  @Override
  public void selectDatabase(String databaseName) throws UndefinedDatabaseException {
    conn.selectDatabase(databaseName);
  }

  @Override
  public Map<String, String> updateSessionVariables(Map<String, String> variables) {
    return conn.updateSessionVariables(variables);
  }

  @Override
  public Map<String, String> unsetSessionVariables(List<String> variables) {
    return conn.unsetSessionVariables(variables);
  }

  @Override
  public String getSessionVariable(String varname) throws NoSuchSessionVariableException {
    return conn.getSessionVariable(varname);
  }

  @Override
  public boolean existSessionVariable(String varname) {
    return conn.existSessionVariable(varname);
  }

  @Override
  public Map<String, String> getAllSessionVariables() {
    return conn.getAllSessionVariables();
  }

  @Override
  public SubmitQueryResponse executeQuery(final String sql) {

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
  public SubmitQueryResponse executeQueryWithJson(final String json) {
    final BlockingInterface stub = conn.getTMStub();
    final QueryRequest request = buildQueryRequest(json, true);

    try {
      return stub.submitQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResultSet executeQueryAndGetResult(String sql) throws TajoException {

    SubmitQueryResponse response = executeQuery(sql);
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
  public ResultSet executeJsonQueryAndGetResult(final String json) throws TajoException {

    SubmitQueryResponse response = executeQueryWithJson(json);
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

  public ResultSet getQueryResultAndWait(QueryId queryId)
      throws QueryNotFoundException, QueryKilledException, QueryFailedException {

    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }

    QueryStatus status = TajoClientUtil.waitCompletion(this, queryId);

    if (status.getState() == QueryState.QUERY_SUCCEEDED) {
      if (status.hasResult()) {
        return getQueryResult(queryId);
      } else {
        return createNullResultSet(queryId);
      }
    } else if (status.getState() == QueryState.QUERY_KILLED) {
      throw new QueryKilledException();
    } else if (status.getState() == QueryState.QUERY_FAILED) {
      throw new QueryFailedException(status.getErrorMessage());
    } else {
      throw new TajoInternalError("Illegal query status: " + status.getState().name() +
          ", cause: " + status.getErrorMessage());
    }
  }

  public GetQueryStatusResponse getRawQueryStatus(QueryId queryId) {

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

    return res;
  }

  @Override
  public QueryStatus getQueryStatus(QueryId queryId) throws QueryNotFoundException {

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

    throwsIfThisError(res.getState(), QueryNotFoundException.class);
    ensureOk(res.getState());
    return new QueryStatus(res);
  }

  @Override
  public ResultSet getQueryResult(QueryId queryId) throws QueryNotFoundException {

    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }

    GetQueryResultResponse response = getResultResponse(queryId);

    TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
    return new FetchResultSet(this, tableDesc.getLogicalSchema(), queryId, defaultFetchRows);
  }

  @Override
  public ResultSet createNullResultSet(QueryId queryId) {
    return TajoClientUtil.createNullResultSet(queryId);
  }

  @Override
  public GetQueryResultResponse getResultResponse(QueryId queryId) throws QueryNotFoundException {
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

    throwsIfThisError(response.getState(), QueryNotFoundException.class);
    ensureOk(response.getState());
    return response;
  }

  @Override
  public Future<TajoMemoryResultSet> fetchNextQueryResultAsync(final QueryId queryId, final int fetchRowNum) {

    final SettableFuture<TajoMemoryResultSet> future = SettableFuture.create();
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          future.set(fetchNextQueryResult(queryId, fetchRowNum));
        } catch (Throwable e) {
          future.setException(e);
        }
      }
    });
    return future;
  }

  protected TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum)
      throws TajoException {

    boolean compress = conn.getProperties().getBool(SessionVars.COMPRESSED_RESULT_TRANSFER);

    final BlockingInterface stub = conn.getTMStub();
    final GetQueryResultDataRequest.Builder request = GetQueryResultDataRequest.newBuilder();
    request.setSessionId(conn.sessionId)
        .setQueryId(queryId.getProto())
        .setFetchRowNum(fetchRowNum);
    if (compress) {
      request.setCompressCodec(DEFAULT_CODEC);
    }

    GetQueryResultDataResponse response;
    try {
      response = stub.getQueryResultData(null, request.build());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());

    if(response.hasResultSet()) {
      SerializedResultSet resultSet = response.getResultSet();
      return new TajoMemoryResultSet(queryId,
          new Schema(resultSet.getSchema()),
          resultSet, getClientSideSessionVars());
    } else {
      return TajoClientUtil.createNullResultSet(queryId);
    }
  }

  @Override
  public boolean updateQuery(final String sql) throws TajoException {

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
  public boolean updateQueryWithJson(final String json) throws TajoException {

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
  public List<BriefQueryInfo> getRunningQueryList() {

    final BlockingInterface stmb = conn.getTMStub();

    GetQueryListResponse res;
    try {
      res = stmb.getRunningQueryList(null, conn.sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(res.getState());
    return res.getQueryListList();
  }

  @Override
  public List<BriefQueryInfo> getFinishedQueryList() {

    final BlockingInterface stub = conn.getTMStub();

    GetQueryListResponse res;
    try {
      res = stub.getFinishedQueryList(null, conn.sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(res.getState());
    return res.getQueryListList();
  }

  @Override
  public List<WorkerResourceInfo> getClusterInfo() {

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

    ensureOk(res.getState());
    return res.getWorkerListList();
  }

  @Override
  public QueryStatus killQuery(final QueryId queryId) throws QueryNotFoundException {

    final BlockingInterface stub = conn.getTMStub();
    QueryStatus status = getQueryStatus(queryId);

    /* send a kill to the TM */
    final QueryIdRequest request = buildQueryIdRequest(queryId);
    try {
      stub.killQuery(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }


    long currentTimeMillis = System.currentTimeMillis();
    long timeKillIssued = currentTimeMillis;
    while ((currentTimeMillis < timeKillIssued + 10000L)
        && ((status.getState() != QueryState.QUERY_KILLED)
        || (status.getState() == QueryState.QUERY_KILL_WAIT))) {
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

  public QueryInfoProto getQueryInfo(final QueryId queryId) throws QueryNotFoundException {

    final BlockingInterface stub = conn.getTMStub();
    final QueryIdRequest request = buildQueryIdRequest(queryId);

    GetQueryInfoResponse res;
    try {
      res = stub.getQueryInfo(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwsIfThisError(res.getState(), QueryNotFoundException.class);
    ensureOk(res.getState());
    return res.getQueryInfo();
  }

  public QueryHistoryProto getQueryHistory(final QueryId queryId) throws QueryNotFoundException {
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

      ensureOk(res.getState());
      return res.getQueryHistory();

    } catch (NoSuchMethodException | ClassNotFoundException e) {
      throw new TajoInternalError(e);
    } catch (ConnectException e) {
      throw new TajoRuntimeException(
          new ClientUnableToConnectException(NetUtils.normalizeInetSocketAddress(qmAddress)));
    } finally {
      if (qmClient != null) {
        qmClient.close();
      }
    }
  }

  private QueryIdRequest buildQueryIdRequest(QueryId queryId) {
    return QueryIdRequest.newBuilder()
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
