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
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.util.ProtoUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.ipc.ClientProtos.*;
import static org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;

public class QueryClientImpl implements QueryClient {
  private static final Log LOG = LogFactory.getLog(QueryClientImpl.class);
  private final SessionConnection connection;
  private final int defaultFetchRows;
//maxRows number is limit value of resultSet. The value must be >= 0, and 0 means there is not limit.
  private int maxRows;

  public QueryClientImpl(SessionConnection connection) {
    this.connection = connection;
    this.defaultFetchRows = this.connection.getProperties().getInt(SessionVars.FETCH_ROWNUM.getConfVars().keyname(),
        SessionVars.FETCH_ROWNUM.getConfVars().defaultIntVal);
    this.maxRows = 0;
  }

  @Override
  public void setSessionId(TajoIdProtos.SessionIdProto sessionId) {
    connection.setSessionId(sessionId);
  }

  @Override
  public boolean isConnected() {
    return connection.isConnected();
  }

  @Override
  public TajoIdProtos.SessionIdProto getSessionId() {
    return connection.getSessionId();
  }

  @Override
  public Map<String, String> getClientSideSessionVars() {
    return connection.getClientSideSessionVars();
  }

  @Override
  public String getBaseDatabase() {
    return connection.getBaseDatabase();
  }

  @Override
  public void close() {
  }

  @Override
  public UserRoleInfo getUserInfo() {
    return connection.getUserInfo();
  }

  @Override
  public void closeQuery(QueryId queryId) {
    closeNonForwardQuery(queryId);
  }

  @Override
  public void closeNonForwardQuery(QueryId queryId) {
    NettyClientBase tmClient = null;
    try {
      tmClient = connection.getTajoMasterConnection();
      TajoMasterClientProtocolService.BlockingInterface tajoMaster = tmClient.getStub();
      connection.checkSessionAndGet(tmClient);

      ClientProtos.QueryIdRequest.Builder builder = ClientProtos.QueryIdRequest.newBuilder();

      builder.setSessionId(getSessionId());
      builder.setQueryId(queryId.getProto());
      tajoMaster.closeNonForwardQuery(null, builder.build());
    } catch (Exception e) {
      LOG.warn("Fail to close a TajoMaster connection (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
    }
  }

  @Override
  public String getCurrentDatabase() throws ServiceException {
    return connection.getCurrentDatabase();
  }

  @Override
  public Boolean selectDatabase(String databaseName) throws ServiceException {
    return connection.selectDatabase(databaseName);
  }

  @Override
  public Map<String, String> updateSessionVariables(Map<String, String> variables) throws ServiceException {
    return connection.updateSessionVariables(variables);
  }

  @Override
  public Map<String, String> unsetSessionVariables(List<String> variables) throws ServiceException {
    return connection.unsetSessionVariables(variables);
  }

  @Override
  public String getSessionVariable(String varname) throws ServiceException {
    return connection.getSessionVariable(varname);
  }

  @Override
  public Boolean existSessionVariable(String varname) throws ServiceException {
    return connection.existSessionVariable(varname);
  }

  @Override
  public Map<String, String> getAllSessionVariables() throws ServiceException {
    return connection.getAllSessionVariables();
  }

  @Override
  public ClientProtos.SubmitQueryResponse executeQuery(final String sql) throws ServiceException {
    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);

    final QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(sql);
    builder.setIsJson(false);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();


    SubmitQueryResponse response = tajoMasterService.submitQuery(null, builder.build());
    if (response.getResultCode() == ResultCode.OK) {
      connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
    }
    return response;
  }

  @Override
  public ClientProtos.SubmitQueryResponse executeQueryWithJson(final String json) throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);

    final QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(json);
    builder.setIsJson(true);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();


    return tajoMasterService.submitQuery(null, builder.build());
  }

  @Override
  public ResultSet executeQueryAndGetResult(String sql) throws ServiceException, IOException {

    ClientProtos.SubmitQueryResponse response = executeQuery(sql);

    if (response.getResultCode() == ClientProtos.ResultCode.ERROR) {
      if (response.hasErrorMessage()) {
        throw new ServiceException(response.getErrorMessage());
      } else if (response.hasErrorTrace()) {
        throw new ServiceException(response.getErrorTrace());
      }
    }

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
  public ResultSet executeJsonQueryAndGetResult(final String json) throws ServiceException, IOException {

    ClientProtos.SubmitQueryResponse response = executeQueryWithJson(json);

    if (response.getResultCode() == ClientProtos.ResultCode.ERROR) {
      throw new ServiceException(response.getErrorTrace());
    }

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

  private ResultSet getQueryResultAndWait(QueryId queryId) throws ServiceException, IOException {

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
  public QueryStatus getQueryStatus(QueryId queryId) throws ServiceException {

    ClientProtos.GetQueryStatusRequest.Builder builder = ClientProtos.GetQueryStatusRequest.newBuilder();
    builder.setQueryId(queryId.getProto());

    GetQueryStatusResponse res = null;

    NettyClientBase tmClient = null;
    try {
      tmClient = connection.getTajoMasterConnection();
      connection.checkSessionAndGet(tmClient);
      builder.setSessionId(connection.sessionId);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();

      res = tajoMasterService.getQueryStatus(null, builder.build());

    } catch (Exception e) {
      throw new ServiceException(e.getMessage(), e);
    }
    return new QueryStatus(res);
  }

  @Override
  public ResultSet getQueryResult(QueryId queryId) throws ServiceException, IOException {

    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }

    GetQueryResultResponse response = getResultResponse(queryId);
    TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
    return new FetchResultSet(this, tableDesc.getLogicalSchema(), queryId, defaultFetchRows);
  }

  @Override
  public ResultSet createNullResultSet(QueryId queryId) throws IOException {
    return TajoClientUtil.createNullResultSet(queryId);
  }

  @Override
  public GetQueryResultResponse getResultResponse(QueryId queryId) throws ServiceException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return null;
    }

    NettyClientBase tmClient = null;

    try {

      tmClient = connection.getTajoMasterConnection();
      connection.checkSessionAndGet(tmClient);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();

      GetQueryResultRequest.Builder builder = GetQueryResultRequest.newBuilder();
      builder.setQueryId(queryId.getProto());
      builder.setSessionId(connection.sessionId);
      GetQueryResultResponse response = tajoMasterService.getQueryResult(null,builder.build());

      return response;

    } catch (Exception e) {
      throw new ServiceException(e.getMessage(), e);
    }
  }

  @Override
  public TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum)
      throws ServiceException {

    try {
      NettyClientBase client = connection.getTajoMasterConnection();
      connection.checkSessionAndGet(client);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

      GetQueryResultDataRequest.Builder builder = GetQueryResultDataRequest.newBuilder();
      builder.setSessionId(connection.sessionId);
      builder.setQueryId(queryId.getProto());
      builder.setFetchRowNum(fetchRowNum);

      GetQueryResultDataResponse response = tajoMasterService.getQueryResultData(null, builder.build());
      if (response.getResultCode() == ClientProtos.ResultCode.ERROR) {
        throw new ServiceException(response.getErrorMessage());
      }

      ClientProtos.SerializedResultSet resultSet = response.getResultSet();

      return new TajoMemoryResultSet(queryId,
          new Schema(resultSet.getSchema()),
          resultSet.getSerializedTuplesList(),
          resultSet.getSerializedTuplesCount(),
          getClientSideSessionVars());
    } catch (ServiceException e) {
      throw e;
    } catch (Throwable e) {
      throw new ServiceException(e.getMessage(), e);
    }
  }

  @Override
  public boolean updateQuery(final String sql) throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(sql);
    builder.setIsJson(false);
    ClientProtos.UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());

    if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      connection.updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return true;
    } else {
      if (response.hasErrorMessage()) {
        LOG.error("ERROR: " + response.getErrorMessage());
      }
      return false;
    }
  }

  @Override
  public boolean updateQueryWithJson(final String json) throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    QueryRequest.Builder builder = QueryRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQuery(json);
    builder.setIsJson(true);
    ClientProtos.UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
    if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      return true;
    } else {
      if (response.hasErrorMessage()) {
        LOG.error("ERROR: " + response.getErrorMessage());
      }
      return false;
    }
  }

  @Override
  public List<ClientProtos.BriefQueryInfo> getRunningQueryList() throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    TajoIdProtos.SessionIdProto.Builder builder = TajoIdProtos.SessionIdProto.newBuilder();
    builder.setId(connection.sessionId.getId());
    ClientProtos.GetQueryListResponse res = tajoMasterService.getRunningQueryList(null, builder.build());
    return res.getQueryListList();
  }

  @Override
  public List<ClientProtos.BriefQueryInfo> getFinishedQueryList() throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    TajoIdProtos.SessionIdProto.Builder builder = TajoIdProtos.SessionIdProto.newBuilder();
    builder.setId(connection.sessionId.getId());
    ClientProtos.GetQueryListResponse res = tajoMasterService.getFinishedQueryList(null, builder.build());
    return res.getQueryListList();
  }

  @Override
  public List<ClientProtos.WorkerResourceInfo> getClusterInfo() throws ServiceException {

    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    ClientProtos.GetClusterInfoRequest.Builder builder = ClientProtos.GetClusterInfoRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    ClientProtos.GetClusterInfoResponse res = tajoMasterService.getClusterInfo(null, builder.build());
    return res.getWorkerListList();
  }

  @Override
  public QueryStatus killQuery(final QueryId queryId)
      throws ServiceException, IOException {

    QueryStatus status = getQueryStatus(queryId);

    NettyClientBase tmClient = null;
    try {
      /* send a kill to the TM */
      tmClient = connection.getTajoMasterConnection();
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();

      connection.checkSessionAndGet(tmClient);

      ClientProtos.QueryIdRequest.Builder builder = ClientProtos.QueryIdRequest.newBuilder();
      builder.setSessionId(connection.sessionId);
      builder.setQueryId(queryId.getProto());
      tajoMasterService.killQuery(null, builder.build());

      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L)
          && ((status.getState() != TajoProtos.QueryState.QUERY_KILLED)
          || (status.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT))) {
        try {
          Thread.sleep(100L);
        } catch(InterruptedException ie) {
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = getQueryStatus(queryId);
      }

    } catch(Exception e) {
      LOG.debug("Error when checking for application status", e);
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
  
  public QueryInfoProto getQueryInfo(final QueryId queryId) throws ServiceException {
    NettyClientBase client = connection.getTajoMasterConnection();
    connection.checkSessionAndGet(client);

    QueryIdRequest.Builder builder = QueryIdRequest.newBuilder();
    builder.setSessionId(connection.sessionId);
    builder.setQueryId(queryId.getProto());

    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    GetQueryInfoResponse res = tajoMasterService.getQueryInfo(null,builder.build());
    if (res.getResultCode() == ResultCode.OK) {
      return res.getQueryInfo();
    } else {
      throw new ServiceException(res.getErrorMessage());
    }
  }

  public QueryHistoryProto getQueryHistory(final QueryId queryId) throws ServiceException {
    final QueryInfoProto queryInfo = getQueryInfo(queryId);

    if (queryInfo.getHostNameOfQM() == null || queryInfo.getQueryMasterClientPort() == 0) {
      return null;
    }
    InetSocketAddress qmAddress = new InetSocketAddress(
        queryInfo.getHostNameOfQM(), queryInfo.getQueryMasterClientPort());

    RpcClientManager manager = RpcClientManager.getInstance();
    NettyClientBase queryMasterClient;
    try {
      queryMasterClient = manager.newClient(qmAddress, QueryMasterClientProtocol.class, false,
          manager.getRetries(), manager.getTimeoutSeconds(), TimeUnit.SECONDS, false);
    } catch (Exception e) {
      throw new ServiceException(e);
    }

    try {
      connection.checkSessionAndGet(connection.getTajoMasterConnection());

      QueryIdRequest.Builder builder = QueryIdRequest.newBuilder();
      builder.setSessionId(connection.sessionId);
      builder.setQueryId(queryId.getProto());

      QueryMasterClientProtocolService.BlockingInterface queryMasterService = queryMasterClient.getStub();
      GetQueryHistoryResponse res = queryMasterService.getQueryHistory(null, builder.build());
      if (res.getResultCode() == ResultCode.OK) {
        return res.getQueryHistory();
      } else {
        throw new ServiceException(res.getErrorMessage());
      }
    } finally {
      queryMasterClient.close();
    }
  }
}
