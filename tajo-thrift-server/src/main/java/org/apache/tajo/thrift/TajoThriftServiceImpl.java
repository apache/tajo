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

package org.apache.tajo.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.*;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.thrift.generated.*;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoThriftServiceImpl implements TajoThriftService.Iface {
  private static final Log LOG = LogFactory.getLog(TajoThriftServiceImpl.class);

  private Map<TajoIdProtos.SessionIdProto, TajoClientHolder> tajoClientMap =
      new ConcurrentHashMap<SessionIdProto, TajoClientHolder>();

  private Map<String, ResultSetHolder> queryResultSets = new HashMap<String, ResultSetHolder>();
  private Map<String, QuerySubmitTask> querySubmitTasks = new HashMap<String, QuerySubmitTask>();
  private ExecutorService executorService;
  private ResultSetAndTaskCleaner resultSetAndTaskCleaner;

  private int maxSession;
  private TajoConf tajoConf;

  public TajoThriftServiceImpl(TajoConf tajoConf) {
    this.tajoConf = tajoConf;
    this.maxSession = tajoConf.getInt(ThriftServerConstants.MAX_SESSION_CONF_KEY, 100);
    int maxTaskRunner = tajoConf.getInt(ThriftServerConstants.MAX_TASK_RUNNER_CONF_KEY, 200);

    this.executorService = Executors.newFixedThreadPool(maxTaskRunner);
    this.resultSetAndTaskCleaner = new ResultSetAndTaskCleaner();

    this.resultSetAndTaskCleaner.start();
  }

  public void stop() {
    if (executorService != null) {
      executorService.shutdownNow();
    }

    if (resultSetAndTaskCleaner != null) {
      resultSetAndTaskCleaner.interrupt();
    }

    synchronized (tajoClientMap) {
      for (TajoClientHolder eachClient : tajoClientMap.values()) {
        eachClient.tajoClient.close();
      }
    }
  }

  public TajoClient getTajoClient(SessionIdProto sessionId) throws TServiceException {
    if (sessionId == null || !sessionId.hasId()) {
      throw new TServiceException("No sessionId", "");
    }

    synchronized (tajoClientMap) {
      if (tajoClientMap.size() >= maxSession) {
        throw new TServiceException("exceed max session [" + maxSession + "]", "");
      }
      TajoClientHolder tajoClientHolder = tajoClientMap.get(sessionId);

      //if there is multiple proxy server, TajoProxyClient call randomly. So certain proxy server hasn't session.
      if (tajoClientHolder == null) {
        //throw new ServiceException("No session info:" + sessionId.getId());
        try {
          TajoClient tajoClient = new TajoClientImpl(tajoConf);
          tajoClient.setSessionId(sessionId);
          tajoClientHolder = new TajoClientHolder();
          tajoClientHolder.tajoClient = tajoClient;
          tajoClientHolder.lastTouchTime = System.currentTimeMillis();

          tajoClientMap.put(sessionId, tajoClientHolder);
          return tajoClient;
        } catch (Exception e) {
          throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
        }
      } else {
        tajoClientHolder.lastTouchTime = System.currentTimeMillis();
        return tajoClientHolder.tajoClient;
      }
    }
  }

  private TServerResponse makeErrorServerResponse(Throwable t) {
    LOG.error(t.getMessage(), t);
    TServerResponse response = new TServerResponse();
    response.setErrorMessage(t.getMessage());
    response.setResultCode(ResultCode.ERROR.name());
    response.setBoolResult(false);
    return response;
  }

  private TGetQueryStatusResponse makeErrorQueryStatusResponse(String errorMessage) {
    LOG.error("Query error:" + errorMessage);

    TGetQueryStatusResponse response = new TGetQueryStatusResponse();
    response.setErrorMessage(errorMessage);
    response.setState(TajoProtos.QueryState.QUERY_ERROR.name());
    response.setResultCode(ResultCode.ERROR.name());
    QueryId queryId = QueryIdFactory.newQueryId(0, 0);  //DUMMY
    response.setQueryId(queryId.toString());

    return response;
  }

  @Override
  public TGetQueryStatusResponse submitQuery(String sessionIdStr, String query, boolean isJson) throws TException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Run Query:" + query);
    }
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    TGetQueryStatusResponse queryStatus = new TGetQueryStatusResponse();

    try {
      TajoClient tajoClient = getTajoClient(sessionId);

      SubmitQueryResponse clientResponse = tajoClient.executeQuery(query);

      if (clientResponse.hasErrorMessage()) {
        return makeErrorQueryStatusResponse(clientResponse.getErrorMessage());
      }

      if (clientResponse.getIsForwarded()) {
        QuerySubmitTask querySubmitTask = new QuerySubmitTask(sessionId);
        querySubmitTask.queryProgressInfo.queryId = new QueryId(clientResponse.getQueryId());

        QueryStatus clientQueryStatus = tajoClient.getQueryStatus(querySubmitTask.queryProgressInfo.queryId);
        querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();

        queryStatus.setQueryId(clientQueryStatus.getQueryId().toString());
        queryStatus.setResultCode(ResultCode.OK.name());
        queryStatus.setState(clientQueryStatus.getState().name());
        queryStatus.setProgress(queryStatus.getProgress());
        queryStatus.setSubmitTime(queryStatus.getSubmitTime());
        queryStatus.setFinishTime(queryStatus.getFinishTime());
        queryStatus.setHasResult(clientQueryStatus.hasResult());

        if (queryStatus.getErrorMessage() != null) {
          queryStatus.setErrorMessage(clientQueryStatus.getErrorMessage());
        }

        if (queryStatus.getQueryMasterHost() != null) {
          queryStatus.setQueryMasterHost(clientQueryStatus.getQueryMasterHost());
          queryStatus.setQueryMasterPort(clientQueryStatus.getQueryMasterPort());
        }

        querySubmitTask.queryProgressInfo.queryStatus = queryStatus;
        querySubmitTask.queryProgressInfo.query = query;

        synchronized (querySubmitTasks) {
          LOG.info(querySubmitTask.getKey() + " query started");
          querySubmitTasks.put(querySubmitTask.getKey(), querySubmitTask);
        }
        executorService.submit(querySubmitTask);
        return querySubmitTask.queryProgressInfo.queryStatus;
      } else {
        //select * from table limit 100
        QueryId queryId = new QueryId(clientResponse.getQueryId());
        LOG.info(sessionId.getId() + "," + queryId + " query is started(direct query)");

        QuerySubmitTask querySubmitTask = new QuerySubmitTask(sessionId);
        querySubmitTask.queryProgressInfo.queryId = queryId;
        querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();
        querySubmitTask.queryProgressInfo.query = query;

        queryStatus.setQueryId(queryId.toString());
        queryStatus.setResultCode(ResultCode.OK.name());
        queryStatus.setState(TajoProtos.QueryState.QUERY_SUCCEEDED.name());
        queryStatus.setProgress(1.0f);
        queryStatus.setSubmitTime(System.currentTimeMillis());
        queryStatus.setFinishTime(System.currentTimeMillis());
        queryStatus.setHasResult(true);

        querySubmitTask.queryProgressInfo.queryStatus = queryStatus;

        synchronized (querySubmitTasks) {
          querySubmitTasks.put(querySubmitTask.getKey(), querySubmitTask);
        }

        ResultSet resultSet = TajoClientUtil.createResultSet(tajoConf, tajoClient, clientResponse);
        synchronized (queryResultSets) {
          ResultSetHolder rsHolder = new ResultSetHolder();
          rsHolder.sessionId = sessionId;
          rsHolder.queryId = queryId;
          rsHolder.rs = (TajoResultSetBase) resultSet;
          rsHolder.tableDesc = null;
          if (resultSet instanceof FetchResultSet) {
            rsHolder.tableDesc = new TableDesc(clientResponse.getTableDesc());
          } else if (resultSet instanceof TajoMemoryResultSet) {
            rsHolder.schema = new Schema(clientResponse.getResultSet().getSchema());
          }
          rsHolder.lastTouchTime = System.currentTimeMillis();
          queryResultSets.put(rsHolder.getKey(), rsHolder);
        }
        return querySubmitTask.queryProgressInfo.queryStatus;
      }
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      checkTajoInvalidSession(e, sessionId);
      queryStatus.setErrorMessage(StringUtils.stringifyException(e));
      queryStatus.setState(TajoProtos.QueryState.QUERY_ERROR.name());
      queryStatus.setResultCode(ResultCode.ERROR.name());
      QueryId queryId = QueryIdFactory.newQueryId(0, 0);  //DUMMY
      queryStatus.setQueryId(queryId.toString());
      return queryStatus;
    }
  }

  @Override
  public TQueryResult getQueryResult(String sessionIdStr, String queryIdStr, int fetchSize) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    QueryId queryId = TajoIdUtils.parseQueryId(queryIdStr);

    TQueryResult queryResult = new TQueryResult();
    synchronized(querySubmitTasks) {
      QuerySubmitTask querySubmitTask = querySubmitTasks.get(ResultSetHolder.getKey(sessionId, queryId));
      if (querySubmitTask == null) {
        LOG.warn("No query submit info for " + sessionId.getId() + "," + queryId);
        queryResult.setQueryStatus(createErrorResponse(queryId, "No query submit info for " + queryId));
        return queryResult;
      } else {
        queryResult.setQueryStatus(querySubmitTask.queryProgressInfo.queryStatus);
      }
    }

    ResultSetHolder rsHolder = null;
    synchronized (queryResultSets) {
      rsHolder = queryResultSets.get(ResultSetHolder.getKey(sessionId, queryId));
    }

    if (rsHolder == null) {
      LOG.warn("No QueryResult for:" + sessionId.getId() + "," + queryId);
      queryResult.setQueryStatus(createErrorResponse(queryId, "No query result for " + queryId));
      return queryResult;
    } else {
      try {
        rsHolder.lastTouchTime = System.currentTimeMillis();

        Schema schema;
        if (rsHolder.tableDesc != null)  {
          schema = rsHolder.tableDesc.getSchema();
          queryResult.setTableDesc(TajoThriftUtil.convertTableDesc(rsHolder.tableDesc));
        } else {
          schema = rsHolder.schema;
          queryResult.setSchema(TajoThriftUtil.convertSchema(schema));
        }
        RowStoreUtil.RowStoreEncoder rowEncoder = RowStoreUtil.createEncoder(schema);

        if (fetchSize <= 0) {
          LOG.warn("Fetch size(" + fetchSize + ") is less than 0, use default size:" +
              ThriftServerConstants.DEFAULT_FETCH_SIZE);
          fetchSize = ThriftServerConstants.DEFAULT_FETCH_SIZE;
        }
        int rowCount = 0;

        while (rsHolder.rs.next()) {
          Tuple tuple = rsHolder.rs.getCurrentTuple();
          queryResult.addToRows(ByteBuffer.wrap(rowEncoder.toBytes(tuple)));
          rowCount++;
          if (rowCount >= fetchSize) {
            break;
          }
        }
        LOG.info("Send result to client for " + sessionId.getId() + "," + queryId + ", " + rowCount + " rows");
        if (queryResult.getSchema() == null && queryResult.getTableDesc() == null) {
          LOG.fatal(">>>>>>>>>>>>>>>>>>>>schema & tabledesc null");
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        queryResult.setQueryStatus(
            createErrorResponse(queryId, "Error while result fetching " + queryId + " cause:\n" +
                StringUtils.stringifyException(e)));
      }
      return queryResult;
    }
  }

  @Override
  public TGetQueryStatusResponse getQueryStatus(String sessionIdStr, String queryIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    QueryId queryId = TajoIdUtils.parseQueryId(queryIdStr);

    touchTajoClient(sessionId);
    synchronized(querySubmitTasks) {
      QuerySubmitTask querySubmitTask = querySubmitTasks.get(ResultSetHolder.getKey(sessionId, queryId));
      if (querySubmitTask == null) {
        return getQueryStatusInternal(sessionId, queryId);
      } else {
        querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();
        return querySubmitTask.queryProgressInfo.queryStatus;
      }
    }
  }

  private TGetQueryStatusResponse getQueryStatusInternal(SessionIdProto sessionId, QueryId queryId) {
    TGetQueryStatusResponse queryStatus = new TGetQueryStatusResponse();
    try {
      QueryStatus clientQueryStatus = getTajoClient(sessionId).getQueryStatus(queryId);
      ResultCode resultCode = ResultCode.OK;

      if (clientQueryStatus.getErrorMessage() != null) {
        resultCode = ResultCode.ERROR;
      }

      queryStatus.setResultCode(resultCode.name());
      queryStatus.setState(clientQueryStatus.getState().name());
      if (queryStatus.getErrorMessage() != null) {
        queryStatus.setErrorMessage(clientQueryStatus.getErrorMessage());
      }
      queryStatus.setProgress(clientQueryStatus.getProgress());
      queryStatus.setFinishTime(clientQueryStatus.getFinishTime());
      queryStatus.setHasResult(clientQueryStatus.hasResult());

      if (queryStatus.getQueryMasterHost() != null) {
        queryStatus.setQueryMasterHost(clientQueryStatus.getQueryMasterHost());
      }
      queryStatus.setQueryMasterPort(clientQueryStatus.getQueryMasterPort());
      queryStatus.setSubmitTime(clientQueryStatus.getSubmitTime());
      queryStatus.setQueryId(clientQueryStatus.getQueryId().toString());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      queryStatus.setResultCode(ResultCode.ERROR.name());
      queryStatus.setErrorMessage(StringUtils.stringifyException(e));
      checkTajoInvalidSession(e, sessionId);
    }

    return queryStatus;
  }

  @Override
  public TServerResponse closeQuery(String sessionIdStr, String queryIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TServerResponse response = new TServerResponse();

      QueryId queryId = TajoIdUtils.parseQueryId(queryIdStr);
      getTajoClient(sessionId).closeQuery(queryId);
      removeQueryTask(sessionId, queryId);
      response.setResultCode(ResultCode.OK.name());
      return response;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      return makeErrorServerResponse(e);
    }
  }

  @Override
  public TServerResponse updateQuery(String sessionIdStr, String query) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TServerResponse response = new TServerResponse();

      boolean result = getTajoClient(sessionId).updateQuery(query);
      response.setBoolResult(result);
      response.setResultCode(ResultCode.OK.name());
      return response;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      return makeErrorServerResponse(e);
    }
  }

  private void removeQueryTask(TajoIdProtos.SessionIdProto sessionId, QueryId queryId) {
    synchronized (queryResultSets) {
      queryResultSets.remove(ResultSetHolder.getKey(sessionId, queryId));
    }

    synchronized (querySubmitTasks) {
      QuerySubmitTask task = querySubmitTasks.remove(ResultSetHolder.getKey(sessionId, queryId));
      if (task != null) {
        task.stopTask();
      }
    }
  }

  @Override
  public TServerResponse createSession(String userId, String defaultDatabase) throws TException {
    try {
      TServerResponse response = new TServerResponse();
      if (tajoClientMap.size() >= maxSession) {
        String errorMesasage = "exceed max session [" + maxSession + "]";
        response.setResultCode(ResultCode.ERROR.name());
        response.setErrorMessage(errorMesasage);
      } else {
        String dbName = defaultDatabase;
        if (defaultDatabase == null || defaultDatabase.isEmpty()) {
          dbName = TajoConstants.DEFAULT_DATABASE_NAME;
        }
        TajoClient tajoClient = new TajoClientImpl(tajoConf, dbName);
        tajoClient.getCurrentDatabase();
        TajoIdProtos.SessionIdProto sessionId = tajoClient.getSessionId();
        if (sessionId == null) {
          throw new TServiceException("Can't make tajo session.", "");
        }

        TajoClientHolder holder = new TajoClientHolder();
        holder.tajoClient = tajoClient;
        holder.userId = userId;
        holder.lastTouchTime = System.currentTimeMillis();
        synchronized (tajoClientMap) {
          tajoClientMap.put(sessionId, holder);
        }
        response.setBoolResult(true);
        response.setResultCode(ResultCode.OK.name());
        response.setSessionId(sessionId.getId());
      }
      return response;
    } catch (Exception e) {
      return makeErrorServerResponse(e);
    }
  }

  @Override
  public TServerResponse closeSession(String sessionId) throws TException {
    try {
      TServerResponse response = new TServerResponse();
      synchronized (tajoClientMap) {
        TajoClientHolder clientHolder = tajoClientMap.remove(TajoThriftUtil.makeSessionId(sessionId));
        if (clientHolder != null && clientHolder.tajoClient != null) {
          clientHolder.tajoClient.close();
        }
      }
      response.setResultCode(ResultCode.OK.name());
      response.setBoolResult(true);
      return response;
    } catch (Exception e) {
      checkTajoInvalidSession(e, TajoThriftUtil.makeSessionId(sessionId));
      return makeErrorServerResponse(e);
    }
  }

  @Override
  public TServerResponse refreshSession(String sessionId) throws TException {
    synchronized(tajoClientMap) {
      TajoClientHolder clientHolder = tajoClientMap.get(TajoThriftUtil.makeSessionId(sessionId));
      if (clientHolder != null) {
        clientHolder.lastTouchTime = System.currentTimeMillis();
      }
    }
    TServerResponse response = new TServerResponse();
    response.setResultCode(ResultCode.OK.name());
    response.setBoolResult(true);
    return response;
  }

  @Override
  public TServerResponse selectDatabase(String sessionIdStr, String databaseName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TServerResponse response = new TServerResponse();
      boolean result = getTajoClient(sessionId).selectDatabase(databaseName).booleanValue();
      response.setBoolResult(result);
      response.setResultCode(ResultCode.OK.name());
      return response;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      return makeErrorServerResponse(e);
    }
  }

  @Override
  public String getCurrentDatabase(String sessionIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).getCurrentDatabase();
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public TServerResponse killQuery(String sessionIdStr, String queryIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    QueryId queryId = TajoIdUtils.parseQueryId(queryIdStr);
    try {
      TServerResponse response = new TServerResponse();
      QueryStatus queryStatus = getTajoClient(sessionId).killQuery(queryId);
      boolean result = queryStatus.getState() == TajoProtos.QueryState.QUERY_KILLED ||
          queryStatus.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT;

      response.setBoolResult(result);
      response.setResultCode(ResultCode.OK.name());
      removeQueryTask(sessionId, queryId);
      return response;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      return makeErrorServerResponse(e);
    }
  }

  @Override
  public List<TBriefQueryInfo> getQueryList(String sessionIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      List<BriefQueryInfo> runningQueryList = getTajoClient(sessionId).getRunningQueryList();
      List<BriefQueryInfo> finishedQueryList = getTajoClient(sessionId).getFinishedQueryList();
      List<TBriefQueryInfo> queries = new ArrayList<TBriefQueryInfo>();

      for (BriefQueryInfo eachQuery: runningQueryList) {
        queries.add(TajoThriftUtil.convertQueryInfo(eachQuery));
      }

      for (BriefQueryInfo eachQuery: finishedQueryList) {
        queries.add(TajoThriftUtil.convertQueryInfo(eachQuery));
      }

      return queries;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean existTable(String sessionIdStr, String tableName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).existTable(tableName);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public List<String> getTableList(String sessionIdStr, String databaseName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).getTableList(databaseName);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public TTableDesc getTableDesc(String sessionIdStr, String tableName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return TajoThriftUtil.convertTableDesc(getTajoClient(sessionId).getTableDesc(tableName));
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean dropTable(String sessionIdStr, String tableName, boolean purge) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).dropTable(tableName, purge);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public List<String> getAllDatabases(String sessionIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).getAllDatabaseNames();
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean createDatabase(String sessionIdStr, String databaseName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).createDatabase(databaseName);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean dropDatabase(String sessionIdStr, String databaseName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).dropDatabase(databaseName);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean existDatabase(String sessionIdStr, String databaseName) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      return getTajoClient(sessionId).existDatabase(databaseName);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public Map<String, String> getAllSessionVariables(String sessionIdStr) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TajoClient tajoClient = getTajoClient(sessionId);

      Map<String, String> variables = new HashMap<String, String>();
      for (Map.Entry<String, String> eachValue: tajoClient.getAllSessionVariables().entrySet()) {
        variables.put(eachValue.getKey(), eachValue.getValue());
      }
      return variables;
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean updateSessionVariable(String sessionIdStr, String key, String value) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TajoClient tajoClient = getTajoClient(sessionId);
      Map<String, String> sessionVariable = new HashMap<String, String>();
      sessionVariable.put(key, value);
      return tajoClient.updateSessionVariables(sessionVariable);
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  @Override
  public boolean unsetSessionVariables(String sessionIdStr, String key) throws TException {
    SessionIdProto sessionId = TajoThriftUtil.makeSessionId(sessionIdStr);
    try {
      TajoClient tajoClient = getTajoClient(sessionId);
      return tajoClient.unsetSessionVariables(TUtil.newList(key));
    } catch (Exception e) {
      checkTajoInvalidSession(e, sessionId);
      throw new TServiceException(e.getMessage(), StringUtils.stringifyException(e));
    }
  }

  private static TGetQueryStatusResponse createErrorResponse(QueryId queryId , String errorMessagge) {
    TGetQueryStatusResponse response = new TGetQueryStatusResponse();
    response.setQueryId(queryId.toString());
    response.setResultCode(ResultCode.ERROR.name());
    response.setState(TajoProtos.QueryState.QUERY_ERROR.name());
    response.setErrorMessage(errorMessagge);

    return response;
  }

  public void touchTajoClient(TajoIdProtos.SessionIdProto sessionId)  {
    if (sessionId == null || !sessionId.hasId()) {
      return;
    }

    synchronized (tajoClientMap) {
      TajoClientHolder tajoClientHolder = tajoClientMap.get(sessionId);

      if (tajoClientHolder == null) {
        return;
      } else {
        tajoClientHolder.lastTouchTime = System.currentTimeMillis();
      }
    }
  }

  public Collection<QueryProgressInfo> getQuerySubmitTasks() {
    synchronized (querySubmitTasks) {
      List<QueryProgressInfo> infos = new ArrayList<QueryProgressInfo>();
      for (QuerySubmitTask eachTask: querySubmitTasks.values()) {
        infos.add(eachTask.queryProgressInfo);
      }
      return infos;
    }
  }

  public Collection<TajoClientHolder> getTajoClientSessions() {
    synchronized (tajoClientMap) {
      return Collections.unmodifiableCollection(tajoClientMap.values());
    }
  }

  public static class TajoClientHolder {
    TajoClient tajoClient;
    long lastTouchTime;
    String userId;

    public TajoClient getTajoClient() {
      return tajoClient;
    }

    public long getLastTouchTime() {
      return lastTouchTime;
    }

    public String getUserId() {
      return userId;
    }
  }

  /**
   * Check query status to tajo master or query master and get result data.
   */
  class QuerySubmitTask extends Thread {
    QueryProgressInfo queryProgressInfo;
    AtomicBoolean stopFlag = new AtomicBoolean(false);

    public QuerySubmitTask(SessionIdProto sessionId) {
      super("QuerySubmitTask");
      queryProgressInfo = new QueryProgressInfo(sessionId);
    }

    private String getKey() {
      return ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId);
    }

    public void stopTask() {
      stopFlag.set(true);
      this.interrupt();
    }

    @Override
    public void run() {
      LOG.info("QuerySubmitTask started for " + queryProgressInfo.sessionId.getId() + "," + queryProgressInfo.queryId);

      TGetQueryStatusResponse lastResponse = null;
      boolean queryFinished = false;
      int errorCount = 0;
      while (!stopFlag.get()) {
        lastResponse = getQueryStatusInternal(queryProgressInfo.sessionId, queryProgressInfo.queryId);
        if (lastResponse == null) {
          queryProgressInfo.queryStatus = createErrorResponse(queryProgressInfo.queryId,
              "No query submit info from TajoMaster for " + queryProgressInfo.queryId);
          break;
        }
        queryProgressInfo.queryStatus = lastResponse;

        if (!TajoThriftUtil.isQueryRunnning(lastResponse.getState())
            && !QueryState.QUERY_KILL_WAIT.name().equals(lastResponse.getState())) {
          queryFinished = true;
          break;
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
        if (ResultCode.ERROR.name().equals(lastResponse.getResultCode())) {
          errorCount++;
          // If error count > 3, query failed.
          if (errorCount > 3) {
            break;
          }
        } else {
          errorCount = 0;
        }
      }
      if (errorCount > 3) {
        LOG.error("QuerySubmitTask stopped for " + queryProgressInfo.sessionId + "," +
            queryProgressInfo.queryId + ", cause " + lastResponse.getErrorMessage());
        queryProgressInfo.queryStatus = createErrorResponse(
            queryProgressInfo.queryId, "QuerySubmitTask stopped for " + queryProgressInfo.sessionId + "," +
                queryProgressInfo.queryId + ", cause " + lastResponse.getErrorMessage());
        return;
      }

      if (stopFlag.get()) {
        LOG.info("QuerySubmitTask be forced to stop [" + queryProgressInfo.queryId + "]");
        TGetQueryStatusResponse queryStatus = new TGetQueryStatusResponse();
        queryStatus.setQueryId(queryProgressInfo.queryId.toString());
        queryStatus.setResultCode(ResultCode.OK.name());
        queryStatus.setState(TajoProtos.QueryState.QUERY_KILLED.name());
        queryStatus.setErrorMessage("QuerySubmitTask be forced to stop [" + queryProgressInfo.queryId + "]");
        queryProgressInfo.queryStatus = queryStatus;
        return;
      }
      //get result
      ResultSetHolder resultSetHolder = new ResultSetHolder();
      try {
        TajoResultSet rs = null;
        if (queryFinished && TajoProtos.QueryState.QUERY_SUCCEEDED.name().equals(lastResponse.getState())) {
          if (lastResponse.isHasResult()) {
            rs = (TajoResultSet)getTajoClient(queryProgressInfo.sessionId).getQueryResult(queryProgressInfo.queryId);
          } else {
            rs = (TajoResultSet)createNullResultSet(getTajoClient(queryProgressInfo.sessionId), queryProgressInfo.queryId);
          }
        } else {
          LOG.warn("Query (" + queryProgressInfo.sessionId.getId() + "," +
              queryProgressInfo.queryStatus.getQueryId() + ") failed: " + queryProgressInfo.queryStatus.getState());
          rs = (TajoResultSet)createNullResultSet(getTajoClient(queryProgressInfo.sessionId), queryProgressInfo.queryId);
        }
        resultSetHolder.rs = rs;
        resultSetHolder.tableDesc = rs.getTableDesc();
        resultSetHolder.lastTouchTime = System.currentTimeMillis();

        synchronized (queryResultSets) {
          LOG.info("Query completed: " + ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId));
          queryResultSets.put(ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId), resultSetHolder);
        }
        queryProgressInfo.queryStatus = lastResponse;
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        TGetQueryStatusResponse queryStatus = new TGetQueryStatusResponse();
        queryStatus.setQueryId(queryProgressInfo.queryId.toString());
        queryStatus.setResultCode(ResultCode.ERROR.name());
        queryStatus.setState(TajoProtos.QueryState.QUERY_ERROR.name());
        queryStatus.setErrorMessage("Can't get query result for " + queryProgressInfo.queryId + " cause:\n" +
            StringUtils.stringifyException(e));

        queryProgressInfo.queryStatus = queryStatus;
      }

      LOG.info("QuerySubmitTask stopped for " + queryProgressInfo.sessionId.getId() + "," +
          queryProgressInfo.queryId + "," +
          queryProgressInfo.queryStatus.getResultCode() + "," +
          queryProgressInfo.queryStatus.getState());
    }
  }

  private void checkTajoInvalidSession(Throwable e, TajoIdProtos.SessionIdProto sessionId) {
    if (e.getMessage() != null && e.getMessage().indexOf("InvalidSessionException") >= 0) {
      synchronized (tajoClientMap) {
        tajoClientMap.remove(sessionId);
      }
    }
  }

  public ResultSet createNullResultSet(TajoClient tajoClient, QueryId queryId) throws IOException {
    return new TajoResultSet(tajoClient, queryId);
  }

  /**
   * Clean the expired ResultSet and QueryTask.
   */
  class ResultSetAndTaskCleaner extends Thread {
    int resultSetExpireInterval;
    int queryTaskExpireInterval;
    int tajoClientExpireInterval;
    int sessionExpireInterval;

    public ResultSetAndTaskCleaner() {
      super("ResultSetAndTaskCleaner");
    }

    @Override
    public void run() {
      resultSetExpireInterval = tajoConf.getInt("tajo.thrift.resultset.expire.interval.sec", 120);
      queryTaskExpireInterval = tajoConf.getInt("tajo.thrift.querytask.expire.interval.sec", 120);
      tajoClientExpireInterval = tajoConf.getInt("tajo.thrift.client.expire.interval.sec", 1 * 60 * 60);   //1 hour
      sessionExpireInterval = tajoConf.getInt("tajo.thrift.session.expire.interval.sec", 1 * 60 * 60); //1 hour

      LOG.info("ResultSetAndTaskCleaner started: resultSetExpireInterval=" + resultSetExpireInterval + "," +
          "queryTaskExpireInterval=" + queryTaskExpireInterval + ",sessionExpireInterval=" + sessionExpireInterval);

      while (true) {
        try {
          cleanQueryResult();
          cleanQueryTask();
          cleanTajoClient();
          cleanSession();
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        } catch (Throwable t) {
          LOG.error(t.getMessage(), t);
        }
      }
    }

    private void cleanSession() {
      Map<SessionIdProto, TajoClientHolder> cleanCheckList = new HashMap<SessionIdProto, TajoClientHolder>();
      synchronized(tajoClientMap) {
        cleanCheckList.putAll(tajoClientMap);
      }

      for (Map.Entry<SessionIdProto, TajoClientHolder> entry: cleanCheckList.entrySet()) {
        SessionIdProto sessionId = entry.getKey();
        TajoClientHolder eachTajoClient = entry.getValue();
        long timeGap = System.currentTimeMillis() - eachTajoClient.lastTouchTime;
        if (timeGap > sessionExpireInterval * 1000) {
          synchronized (tajoClientMap) {
            tajoClientMap.remove(sessionId);
          }
        }
      }
    }

    private void cleanQueryResult() {
      List<ResultSetHolder> queryResultSetCleanList = new ArrayList<ResultSetHolder>();

      synchronized (queryResultSets) {
        queryResultSetCleanList.addAll(queryResultSets.values());
      }

      for (ResultSetHolder eachResultSetHolder: queryResultSetCleanList) {
        if (System.currentTimeMillis() - eachResultSetHolder.lastTouchTime > resultSetExpireInterval * 1000) {
          try {
            if (!eachResultSetHolder.rs.isClosed()) {
              LOG.info("ResultSet close:" + eachResultSetHolder.queryId);
              eachResultSetHolder.rs.close();

              synchronized (queryResultSets) {
                queryResultSets.remove(eachResultSetHolder.getKey());
              }
            }
          } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }

    private void cleanQueryTask() {
      List<QuerySubmitTask> querySubmitTaskCleanList = new ArrayList<QuerySubmitTask>();
      synchronized (querySubmitTasks) {
        querySubmitTaskCleanList.addAll(querySubmitTasks.values());
      }

      for (QuerySubmitTask eachTask: querySubmitTaskCleanList) {
        if (System.currentTimeMillis() - eachTask.queryProgressInfo.lastTouchTime > queryTaskExpireInterval * 1000) {
          eachTask.stopTask();
          synchronized (querySubmitTasks) {
            QuerySubmitTask task = querySubmitTasks.remove(eachTask.getKey());
            if (task != null) {
              task.stopTask();
            }
          }
        }
      }
    }

    private void cleanTajoClient() {
      List<TajoIdProtos.SessionIdProto> tajoClientCleanList = new ArrayList<TajoIdProtos.SessionIdProto>();
      synchronized (tajoClientMap) {
        tajoClientCleanList.addAll(tajoClientMap.keySet());
      }

      for (TajoIdProtos.SessionIdProto eachSessionId: tajoClientCleanList) {
        TajoClientHolder tajoClientHolder = tajoClientMap.get(eachSessionId);
        if (tajoClientHolder == null) {
          continue;
        }
        if (System.currentTimeMillis() - tajoClientHolder.lastTouchTime > tajoClientExpireInterval * 1000) {
          synchronized (tajoClientMap) {
            tajoClientMap.remove(eachSessionId);
          }
          tajoClientHolder.tajoClient.close();
        }
      }
    }
  }
}
