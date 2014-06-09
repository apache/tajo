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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.cli.InvalidClientSessionException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.QueryMasterClientProtocol;
import org.apache.tajo.ipc.QueryMasterClientProtocol.QueryMasterClientProtocolService;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;
import org.apache.tajo.jdbc.SQLStates;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse.SerializedResultSet;

@ThreadSafe
public class TajoClient implements Closeable {
  private final Log LOG = LogFactory.getLog(TajoClient.class);

  private final TajoConf conf;

  private final Map<QueryId, InetSocketAddress> queryMasterMap = new ConcurrentHashMap<QueryId, InetSocketAddress>();

  private final InetSocketAddress tajoMasterAddr;

  private final RpcConnectionPool connPool;

  private final String baseDatabase;

  private final UserGroupInformation userInfo;

  private volatile TajoIdProtos.SessionIdProto sessionId;

  public TajoClient(TajoConf conf) throws IOException {
    this(conf, NetUtils.createSocketAddr(conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)), null);
  }

  public TajoClient(TajoConf conf, @Nullable String baseDatabase) throws IOException {
    this(conf, NetUtils.createSocketAddr(conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)), baseDatabase);
  }

  /**
   * Connect to TajoMaster
   *
   * @param conf TajoConf
   * @param addr TajoMaster address
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @throws IOException
   */
  public TajoClient(TajoConf conf, InetSocketAddress addr, @Nullable String baseDatabase) throws IOException {
    this.conf = conf;
    this.conf.set("tajo.disk.scheduler.report.interval", "0");
    this.tajoMasterAddr = addr;
    int workerNum = conf.getIntVar(TajoConf.ConfVars.RPC_CLIENT_WORKER_THREAD_NUM);
    // Don't share connection pool per client
    connPool = RpcConnectionPool.newPool(conf, getClass().getSimpleName(), workerNum);
    userInfo = UserGroupInformation.getCurrentUser();
    this.baseDatabase = baseDatabase != null ? baseDatabase : null;
  }

  public void setSessionId(TajoIdProtos.SessionIdProto sessionId) {
      this.sessionId = sessionId;
  }

  public boolean isConnected() {
    try {
      return connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false).isConnected();
    } catch (Exception e) {
      return false;
    }
  }

  public TajoClient(InetSocketAddress addr) throws IOException {
    this(new TajoConf(), addr, null);
  }

  public TajoClient(String hostname, int port, String baseDatabase) throws IOException {
    this(new TajoConf(), NetUtils.createSocketAddr(hostname, port), baseDatabase);
  }

  public TajoIdProtos.SessionIdProto getSessionId() {
    return sessionId;
  }

  @Override
  public void close() {
    // remove session
    try {
      NettyClientBase client = connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false);
      TajoMasterClientProtocolService.BlockingInterface tajoMaster = client.getStub();
      tajoMaster.removeSession(null, sessionId);
    } catch (Exception e) {
      LOG.error(e);
    }

    if(connPool != null) {
      connPool.shutdown();
    }
    queryMasterMap.clear();
  }

  public TajoConf getConf() {
    return conf;
  }

  public UserGroupInformation getUserInfo() {
    return userInfo;
  }

  /**
   * Call to QueryMaster closing query resources
   * @param queryId
   */
  public void closeQuery(final QueryId queryId) {
    if(queryMasterMap.containsKey(queryId)) {
      NettyClientBase qmClient = null;
      try {
        qmClient = connPool.getConnection(queryMasterMap.get(queryId), QueryMasterClientProtocol.class, false);
        QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
        queryMasterService.closeQuery(null, queryId.getProto());
      } catch (Exception e) {
        LOG.warn("Fail to close a QueryMaster connection (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
      } finally {
        connPool.closeConnection(qmClient);
        queryMasterMap.remove(queryId);
      }
    }
  }

  private void checkSessionAndGet(NettyClientBase client) throws ServiceException {
    if (sessionId == null) {
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setUsername(userInfo.getUserName()).build();
      if (baseDatabase != null) {
        builder.setBaseDatabaseName(baseDatabase);
      }
      CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());
      if (response.getState() == CreateSessionResponse.ResultState.SUCCESS) {
        sessionId = response.getSessionId();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
        }
      } else {
        throw new InvalidClientSessionException(response.getMessage());
      }
    }
  }

  private SessionedStringProto convertSessionedString(String str) {
    SessionedStringProto.Builder builder = SessionedStringProto.newBuilder();
    builder.setSessionId(sessionId);
    builder.setValue(str);
    return builder.build();
  }

  public String getCurrentDatabase() throws ServiceException {
    return new ServerCallable<String>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getCurrentDatabase(null, sessionId).getValue();
      }
    }.withRetries();
  }

  public Boolean selectDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.selectDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  public Boolean updateSessionVariables(final Map<String, String> variables) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        KeyValueSet keyValueSet = new KeyValueSet();
        keyValueSet.putAll(variables);
        UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .setSetVariables(keyValueSet.getProto()).build();

        return tajoMasterService.updateSessionVariables(null, request).getValue();
      }
    }.withRetries();
  }

  public Boolean unsetSessionVariables(final List<String> variables)  throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllUnsetVariables(variables).build();
        return tajoMasterService.updateSessionVariables(null, request).getValue();
      }
    }.withRetries();
  }

  public String getSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<String>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Boolean existSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Map<String, String> getAllSessionVariables() throws ServiceException {
    return new ServerCallable<Map<String, String>>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class,
        false, true) {

      public Map<String, String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        KeyValueSet keyValueSet = new KeyValueSet(tajoMasterService.getAllSessionVariables(null, sessionId));
        return keyValueSet.getAllKeyValus();
      }
    }.withRetries();
  }

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}
   * or {@link #getQueryResultAndWait(org.apache.tajo.QueryId)}.
   */
  public SubmitQueryResponse executeQuery(final String sql) throws ServiceException {
    return new ServerCallable<SubmitQueryResponse>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public SubmitQueryResponse call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        final QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setQuery(sql);
        builder.setIsJson(false);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.submitQuery(null, builder.build());
      }
    }.withRetries();
  }

  public SubmitQueryResponse executeQueryWithJson(final String json) throws ServiceException {
    return new ServerCallable<SubmitQueryResponse>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public SubmitQueryResponse call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        final QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setQuery(json);
        builder.setIsJson(true);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.submitQuery(null, builder.build());
      }
    }.withRetries();
  }

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  public ResultSet executeQueryAndGetResult(final String sql)
      throws ServiceException, IOException {
    SubmitQueryResponse response = executeQuery(sql);

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
      // If a non-forwarded insert into query
      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID) && response.getMaxRowNum() < 0) {
        return this.createNullResultSet(queryId);
      } else {
        if (response.hasResultSet() || response.hasTableDesc()) {
          return createResultSet(this, response);
        } else {
          return this.createNullResultSet(queryId);
        }
      }
    }
  }

  public ResultSet executeJsonQueryAndGetResult(final String json) throws ServiceException, IOException {
    SubmitQueryResponse response = executeQueryWithJson(json);
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
        return createResultSet(this, response);
      } else {
        return this.createNullResultSet(queryId);
      }
    }
  }

  public QueryStatus getQueryStatus(QueryId queryId) throws ServiceException {
    GetQueryStatusRequest.Builder builder = GetQueryStatusRequest.newBuilder();
    builder.setQueryId(queryId.getProto());

    GetQueryStatusResponse res = null;
    if(queryMasterMap.containsKey(queryId)) {
      NettyClientBase qmClient = null;
      try {
        qmClient = connPool.getConnection(queryMasterMap.get(queryId),
            QueryMasterClientProtocol.class, false);
        QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
        res = queryMasterService.getQueryStatus(null, builder.build());
      } catch (Exception e) {
        throw new ServiceException(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(qmClient);
      }
    } else {
      NettyClientBase tmClient = null;
      try {
        tmClient = connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false);

        checkSessionAndGet(tmClient);
        builder.setSessionId(sessionId);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();
        res = tajoMasterService.getQueryStatus(null, builder.build());

        String queryMasterHost = res.getQueryMasterHost();
        if(queryMasterHost != null && !queryMasterHost.isEmpty()) {
          NettyClientBase qmClient = null;
          try {
            InetSocketAddress qmAddr = NetUtils.createSocketAddr(queryMasterHost, res.getQueryMasterPort());
            qmClient = connPool.getConnection(
                qmAddr, QueryMasterClientProtocol.class, false);
            QueryMasterClientProtocolService.BlockingInterface queryMasterService = qmClient.getStub();
            res = queryMasterService.getQueryStatus(null, builder.build());

            queryMasterMap.put(queryId, qmAddr);
          } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
          } finally {
            connPool.releaseConnection(qmClient);
          }
        }
      } catch (Exception e) {
        throw new ServiceException(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(tmClient);
      }
    }
    return new QueryStatus(res);
  }

  public static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_RUNNING ||
        state == QueryState.QUERY_MASTER_LAUNCHED ||
        state == QueryState.QUERY_MASTER_INIT ||
        state == QueryState.QUERY_NOT_ASSIGNED;
  }

  public ResultSet getQueryResult(QueryId queryId)
      throws ServiceException, IOException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }
    GetQueryResultResponse response = getResultResponse(queryId);
    TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
    conf.setVar(ConfVars.USERNAME, response.getTajoUserName());
    return new TajoResultSet(this, queryId, conf, tableDesc);
  }

  public static ResultSet createResultSet(TajoClient client, QueryId queryId, GetQueryResultResponse response)
      throws IOException {
    TableDesc desc = CatalogUtil.newTableDesc(response.getTableDesc());
    TajoConf conf = new TajoConf(client.getConf());
    conf.setVar(ConfVars.USERNAME, response.getTajoUserName());
    return new TajoResultSet(client, queryId, conf, desc);
  }

  public static ResultSet createResultSet(TajoClient client, SubmitQueryResponse response) throws IOException {
    if (response.hasTableDesc()) {
      TajoConf conf = new TajoConf(client.getConf());
      conf.setVar(ConfVars.USERNAME, response.getUserName());
      if (response.hasMaxRowNum()) {
        return new TajoResultSet(
            client,
            QueryIdFactory.NULL_QUERY_ID,
            conf,
            new TableDesc(response.getTableDesc()),
            response.getMaxRowNum());
      } else {
        return new TajoResultSet(
            client,
            QueryIdFactory.NULL_QUERY_ID,
            conf,
            new TableDesc(response.getTableDesc()));
      }
    } else {
      SerializedResultSet serializedResultSet = response.getResultSet();
      return new TajoMemoryResultSet(
          new Schema(serializedResultSet.getSchema()),
          serializedResultSet.getSerializedTuplesList(),
          response.getMaxRowNum());
    }
  }

  private ResultSet getQueryResultAndWait(QueryId queryId)
      throws ServiceException, IOException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return createNullResultSet(queryId);
    }
    QueryStatus status = getQueryStatus(queryId);

    while(status != null && isQueryRunnning(status.getState())) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = getQueryStatus(queryId);
    }

    if (status.getState() == QueryState.QUERY_SUCCEEDED) {
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

  public ResultSet createNullResultSet(QueryId queryId) throws IOException {
    return new TajoResultSet(this, queryId);
  }

  public GetQueryResultResponse getResultResponse(QueryId queryId) throws ServiceException {
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return null;
    }

    NettyClientBase client = null;
    try {
      InetSocketAddress queryMasterAddr = queryMasterMap.get(queryId);
      if(queryMasterAddr == null) {
        LOG.warn("No Connection to QueryMaster for " + queryId);
        return null;
      }
      client = connPool.getConnection(queryMasterAddr, QueryMasterClientProtocol.class, false);
      QueryMasterClientProtocolService.BlockingInterface queryMasterService = client.getStub();
      GetQueryResultRequest.Builder builder = GetQueryResultRequest.newBuilder();
      builder.setQueryId(queryId.getProto());
      GetQueryResultResponse response = queryMasterService.getQueryResult(null,
          builder.build());

      return response;
    } catch (Exception e) {
      throw new ServiceException(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(client);
    }
  }

  public boolean updateQuery(final String sql) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setQuery(sql);
        builder.setIsJson(false);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
        if (response.getResultCode() == ResultCode.OK) {
          return true;
        } else {
          if (response.hasErrorMessage()) {
            System.err.println("ERROR: " + response.getErrorMessage());
          }
          return false;
        }
      }
    }.withRetries();
  }

  public boolean updateQueryWithJson(final String json) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        QueryRequest.Builder builder = QueryRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setQuery(json);
        builder.setIsJson(true);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        UpdateQueryResponse response = tajoMasterService.updateQuery(null, builder.build());
        if (response.getResultCode() == ResultCode.OK) {
          return true;
        } else {
          if (response.hasErrorMessage()) {
            System.err.println("ERROR: " + response.getErrorMessage());
          }
          return false;
        }
      }
    }.withRetries();
  }

  /**
   * Create a database.
   *
   * @param databaseName The database name to be created. This name is case sensitive.
   * @return True if created successfully.
   * @throws ServiceException
   */
  public boolean createDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.createDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  /**
   * Does the database exist?
   *
   * @param databaseName The database name to be checked. This name is case sensitive.
   * @return True if so.
   * @throws ServiceException
   */
  public boolean existDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  /**
   * Drop the database
   *
   * @param databaseName The database name to be dropped. This name is case sensitive.
   * @return True if the database is dropped successfully.
   * @throws ServiceException
   */
  public boolean dropDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.dropDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  public List<String> getAllDatabaseNames() throws ServiceException {
    return new ServerCallable<List<String>>(connPool, tajoMasterAddr, TajoMasterClientProtocol.class, false, true) {
      public List<String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getAllDatabases(null, sessionId).getValuesList();
      }
    }.withRetries();
  }

  /**
   * Does the table exist?
   *
   * @param tableName The table name to be checked. This name is case sensitive.
   * @return True if so.
   */
  public boolean existTable(final String tableName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existTable(null, convertSessionedString(tableName)).getValue();
      }
    }.withRetries();
  }

  /**
   * Create an external table.
   *
   * @param tableName The table name to be created. This name is case sensitive. This name can be qualified or not.
   *                  If the table name is not qualified, the current database in the session will be used.
   * @param schema The schema
   * @param path The external table location
   * @param meta Table meta
   * @return the created table description.
   * @throws SQLException
   * @throws ServiceException
   */
  public TableDesc createExternalTable(final String tableName, final Schema schema, final Path path,
                                       final TableMeta meta)
      throws SQLException, ServiceException {
    return new ServerCallable<TableDesc>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setName(tableName);
        builder.setSchema(schema.getProto());
        builder.setMeta(meta.getProto());
        builder.setPath(path.toUri().toString());
        TableResponse res = tajoMasterService.createExternalTable(null, builder.build());
        if (res.getResultCode() == ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }
    }.withRetries();
  }

  /**
   * Drop a table
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @return True if the table is dropped successfully.
   */
  public boolean dropTable(final String tableName) throws ServiceException {
    return dropTable(tableName, false);
  }

  /**
   * Drop a table.
   *
   * @param tableName The table name to be dropped. This name is case sensitive.
   * @param purge If purge is true, this call will remove the entry in catalog as well as the table contents.
   * @return True if the table is dropped successfully.
   */
  public boolean dropTable(final String tableName, final boolean purge) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        DropTableRequest.Builder builder = DropTableRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setName(tableName);
        builder.setPurge(purge);
        return tajoMasterService.dropTable(null, builder.build()).getValue();
      }
    }.withRetries();

  }

  public List<BriefQueryInfo> getRunningQueryList() throws ServiceException {
    return new ServerCallable<List<BriefQueryInfo>>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<BriefQueryInfo> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetQueryListRequest.Builder builder = GetQueryListRequest.newBuilder();
        builder.setSessionId(sessionId);
        GetQueryListResponse res = tajoMasterService.getRunningQueryList(null, builder.build());
        return res.getQueryListList();
      }
    }.withRetries();
  }

  public List<BriefQueryInfo> getFinishedQueryList() throws ServiceException {
    return new ServerCallable<List<BriefQueryInfo>>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<BriefQueryInfo> call(NettyClientBase client) throws ServiceException {
        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetQueryListRequest.Builder builder = GetQueryListRequest.newBuilder();
        builder.setSessionId(sessionId);
        GetQueryListResponse res = tajoMasterService.getFinishedQueryList(null, builder.build());
        return res.getQueryListList();
      }
    }.withRetries();
  }

  public List<WorkerResourceInfo> getClusterInfo() throws ServiceException {
    return new ServerCallable<List<WorkerResourceInfo>>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<WorkerResourceInfo> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetClusterInfoRequest.Builder builder = GetClusterInfoRequest.newBuilder();
        builder.setSessionId(sessionId);
        GetClusterInfoResponse res = tajoMasterService.getClusterInfo(null, builder.build());
        return res.getWorkerListList();
      }
    }.withRetries();
  }

  /**
   * Get a list of table names.
   *
   * @param databaseName The database name to show all tables. This name is case sensitive.
   *                     If it is null, this method will show all tables
   *                     in the current database of this session.
   */
  public List<String> getTableList(@Nullable final String databaseName) throws ServiceException {
    return new ServerCallable<List<String>>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetTableListRequest.Builder builder = GetTableListRequest.newBuilder();
        builder.setSessionId(sessionId);
        if (databaseName != null) {
          builder.setDatabaseName(databaseName);
        }
        GetTableListResponse res = tajoMasterService.getTableList(null, builder.build());
        return res.getTablesList();
      }
    }.withRetries();
  }

  /**
   * Get a table description
   *
   * @param tableName The table name to get. This name is case sensitive.
   * @return Table description
   */
  public TableDesc getTableDesc(final String tableName) throws ServiceException {
    return new ServerCallable<TableDesc>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public TableDesc call(NettyClientBase client) throws ServiceException, SQLException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

        GetTableDescRequest.Builder builder = GetTableDescRequest.newBuilder();
        builder.setSessionId(sessionId);
        builder.setTableName(tableName);
        TableResponse res = tajoMasterService.getTableDesc(null, builder.build());
        if (res.getResultCode() == ResultCode.OK) {
          return CatalogUtil.newTableDesc(res.getTableDesc());
        } else {
          throw new SQLException(res.getErrorMessage(), SQLStates.ER_NO_SUCH_TABLE.getState());
        }
      }
    }.withRetries();
  }

  public QueryStatus killQuery(final QueryId queryId)
      throws ServiceException, IOException {

    QueryStatus status = getQueryStatus(queryId);

    NettyClientBase tmClient = null;
    try {
      /* send a kill to the TM */
      tmClient = connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false);
      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = tmClient.getStub();

      checkSessionAndGet(tmClient);

      KillQueryRequest.Builder builder = KillQueryRequest.newBuilder();
      builder.setSessionId(sessionId);
      builder.setQueryId(queryId.getProto());
      tajoMasterService.killQuery(null, builder.build());

      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L)
          && ((status.getState() != QueryState.QUERY_KILLED)
          || (status.getState() == QueryState.QUERY_KILL_WAIT))) {
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
    } finally {
      connPool.releaseConnection(tmClient);
    }
    return status;
  }

  public List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName) throws ServiceException {
    return new ServerCallable<List<CatalogProtos.FunctionDescProto>>(connPool, tajoMasterAddr,
        TajoMasterClientProtocol.class, false, true) {
      public List<CatalogProtos.FunctionDescProto> call(NettyClientBase client) throws ServiceException, SQLException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        String paramFunctionName = functionName == null ? "" : functionName;
        FunctionResponse res = tajoMasterService.getFunctionList(null,convertSessionedString(paramFunctionName));
        if (res.getResultCode() == ResultCode.OK) {
          return res.getFunctionsList();
        } else {
          throw new SQLException(res.getErrorMessage());
        }
      }
    }.withRetries();
  }
}
