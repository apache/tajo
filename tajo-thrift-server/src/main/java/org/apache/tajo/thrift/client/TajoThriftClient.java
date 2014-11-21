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

package org.apache.tajo.thrift.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.client.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.thrift.TajoThriftUtil;
import org.apache.tajo.thrift.ThriftServerConstants;
import org.apache.tajo.thrift.generated.*;
import org.apache.tajo.thrift.generated.TajoThriftService.Client;
import org.apache.tajo.util.TUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoThriftClient {
  private final Log LOG = LogFactory.getLog(TajoThriftClient.class);

  private int fetchSize;

  protected final TajoConf tajoConf;

  protected String currentThriftServer;

  protected Client currentClient;

  private final String baseDatabase;

  private final UserGroupInformation userInfo;

  volatile String sessionId;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Random rand = new Random(System.currentTimeMillis());

  public TajoThriftClient(TajoConf tajoConf) throws IOException {
    this(tajoConf, null);
  }

  /**
   * Connect to ThriftServer
   *
   * @param tajoConf     TajoConf
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @throws java.io.IOException
   */
  public TajoThriftClient(TajoConf tajoConf, @Nullable String baseDatabase) throws IOException {
    this.tajoConf = tajoConf;
    this.baseDatabase = baseDatabase;
    this.userInfo = UserGroupInformation.getCurrentUser();

    if (tajoConf.get(ThriftServerConstants.SERVER_LIST_CONF_KEY) == null ||
        tajoConf.get(ThriftServerConstants.SERVER_LIST_CONF_KEY).isEmpty()) {
      String serverAddress = "localhost:" +
              tajoConf.getInt(ThriftServerConstants.SERVER_PORT_CONF_KEY, ThriftServerConstants.DEFAULT_LISTEN_PORT);
      tajoConf.set(ThriftServerConstants.SERVER_LIST_CONF_KEY, serverAddress);
    }
    makeConnection(null);
  }

  public TajoConf getConf() {
    return tajoConf;
  }

  public UserGroupInformation getUserInfo() {
    return userInfo;
  }

  protected void makeConnection(String thriftServer) throws IOException {
    if (currentClient == null) {
      if (thriftServer == null) {
        List<String> thriftServers = TUtil.newList(
            tajoConf.getStrings(ThriftServerConstants.SERVER_LIST_CONF_KEY));

        if (thriftServers.isEmpty()) {
          thriftServers.add(ThriftServerConstants.DEFAULT_BIND_ADDRESS);
        }
        thriftServer = thriftServers.get(rand.nextInt(thriftServers.size()));
        currentThriftServer = thriftServer;
      }
      String[] tokens = thriftServer.split(":");
      TTransport transport = new TSocket(tokens[0], Integer.parseInt(tokens[1]));
      try {
        transport.open();
      } catch (Exception e) {
        LOG.error("Can not make protocol: " + thriftServer + ", " + e.getMessage(), e);
        throw new IOException("Can not make protocol", e);
      }
      currentClient = new TajoThriftService.Client(new TBinaryProtocol(transport));
    }
  }

  protected void reconnect() throws IOException {
    if (currentClient != null) {
      TajoThriftUtil.close(currentClient);
    }
    currentClient = null;
    makeConnection(currentThriftServer);
  }

  public boolean createDatabase(final String databaseName) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.createDatabase(sessionId, databaseName);
      }
    }.withRetries();
  }

  public boolean existDatabase(final String databaseName) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.existDatabase(sessionId, databaseName);
      }
    }.withRetries();
  }

  public boolean dropDatabase(final String databaseName) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.dropDatabase(sessionId, databaseName);
      }
    }.withRetries();
  }

  public List<String> getAllDatabaseNames() throws Exception {
    return new ReconnectThriftServerCallable<List<String>>(currentClient) {
      public List<String> call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getAllDatabases(sessionId);
      }
    }.withRetries();
  }

  public boolean existTable(final String tableName) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.existTable(sessionId, tableName);
      }
    }.withRetries();
  }

  public boolean dropTable(final String tableName) throws Exception {
    return dropTable(tableName, false);
  }

  public boolean dropTable(final String tableName, final boolean purge) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.dropTable(sessionId, tableName, purge);
      }
    }.withRetries();
  }

  public List<String> getTableList(@Nullable final String databaseName) throws Exception {
    return new ReconnectThriftServerCallable<List<String>>(currentClient) {
      public List<String> call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getTableList(sessionId, databaseName);
      }
    }.withRetries();
  }

  public TTableDesc getTableDesc(final String tableName) throws Exception {
    return new ReconnectThriftServerCallable<TTableDesc>(currentClient) {
      public TTableDesc call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getTableDesc(sessionId, tableName);
      }
    }.withRetries();
  }

  public void closeQuery(String queryId) {
    try {
      checkSessionAndGet(currentClient);

      currentClient.closeQuery(sessionId, queryId);
    } catch (Exception e) {
      LOG.warn("Fail to close query (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
    }
  }

  public TGetQueryStatusResponse executeQuery(final String sql) throws Exception {
    return new ReconnectThriftServerCallable<TGetQueryStatusResponse>(currentClient) {
      public TGetQueryStatusResponse call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.submitQuery(sessionId, sql, false);
      }
    }.withRetries();
  }

  public boolean updateQuery(final String sql) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.updateQuery(sessionId, sql).isBoolResult();
      }
    }.withRetries();
  }

  public ResultSet executeQueryAndGetResult(final String sql) throws ServiceException, IOException {
    try {
      TGetQueryStatusResponse response = new ReconnectThriftServerCallable<TGetQueryStatusResponse>(currentClient) {
        public TGetQueryStatusResponse call(Client client) throws Exception {
          checkSessionAndGet(client);
          TGetQueryStatusResponse response = null;
          try {
            response = client.submitQuery(sessionId, sql, false);
          } catch (TServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          if (!ResultCode.OK.name().equals(response.getResultCode()) || response.getErrorMessage() != null) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          return response;
        }
      }.withRetries();

      if (response != null && response.getQueryId() != null) {
        return this.getQueryResultAndWait(response.getQueryId());
      } else {
        return createNullResultSet(QueryIdFactory.NULL_QUERY_ID.toString());
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  public ResultSet createNullResultSet(String queryId) throws IOException {
    TGetQueryStatusResponse emptyQueryStatus = new TGetQueryStatusResponse();
    emptyQueryStatus.setResultCode(ResultCode.OK.name());
    emptyQueryStatus.setState(QueryState.QUERY_SUCCEEDED.name());
    emptyQueryStatus.setQueryId(queryId);

    TQueryResult emptyQueryResult = new TQueryResult();

    emptyQueryResult.setQueryStatus(emptyQueryStatus);
    emptyQueryResult.setRows(Collections.<ByteBuffer>emptyList());
    return new TajoThriftResultSet(this, queryId, emptyQueryResult);
  }

  public ResultSet getQueryResultAndWait(String queryId) throws Exception {
    TGetQueryStatusResponse status = getQueryStatus(queryId);
    while(status != null && TajoThriftUtil.isQueryRunnning(status.getState())) {
      try {
        //TODO use thread
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = getQueryStatus(queryId);
    }
    if (QueryState.QUERY_SUCCEEDED.name().equals(status.getState())) {
      if (status.isHasResult()) {
        return getQueryResult(queryId);
      } else {
        return createNullResultSet(queryId);
      }
    } else {
      LOG.warn("Query (" + status.getQueryId() + ") failed: " + status.getState());
      //TODO change SQLException
      throw new IOException("Query (" + status.getQueryId() + ") failed: " + status.getState() +
          " cause " + status.getErrorMessage());
    }
  }

  public TGetQueryStatusResponse getQueryStatus(final String queryId) throws Exception {
    return new ReconnectThriftServerCallable<TGetQueryStatusResponse>(currentClient) {
      public TGetQueryStatusResponse call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getQueryStatus(sessionId, queryId);
      }
    }.withRetries();
  }

  public ResultSet getQueryResult(String queryId) throws IOException {
    return getQueryResult(queryId, ThriftServerConstants.DEFAULT_FETCH_SIZE);
  }

  public ResultSet getQueryResult(String queryId, int fetchSize) throws IOException {
    try {
      TQueryResult queryResult = getNextQueryResult(queryId, fetchSize);
      ResultSet resultSet;
      if (queryResult.getSchema() != null) {
        resultSet = new TajoThriftMemoryResultSet(TajoThriftUtil.convertSchema(queryResult.getSchema()),
            queryResult.getRows(), queryResult.getRows() == null ? 0 : queryResult.getRows().size());
      } else {
        resultSet = new TajoThriftResultSet(this, queryId, queryResult);
        resultSet.setFetchSize(fetchSize);
      }

      return resultSet;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  public TQueryResult getNextQueryResult(final String queryId, int fetchSize) throws IOException {
    try {
      checkSessionAndGet(currentClient);

      return currentClient.getQueryResult(sessionId, queryId, fetchSize);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  public List<TBriefQueryInfo> getRunningQueryList() throws Exception {
    List<TBriefQueryInfo> queries = getQueryList();

    List<TBriefQueryInfo> runningQueries = new ArrayList<TBriefQueryInfo>();
    for (TBriefQueryInfo eachQuery: queries) {
      if (QueryState.QUERY_SUCCEEDED.name().equals(eachQuery)) {
        runningQueries.add(eachQuery);
      }
    }
    return runningQueries;
  }

  public List<TBriefQueryInfo> getFinishedQueryList() throws Exception {
    List<TBriefQueryInfo> queries = getQueryList();

    List<TBriefQueryInfo> finishedQueries = new ArrayList<TBriefQueryInfo>();
    for (TBriefQueryInfo eachQuery: queries) {
      if (!QueryState.QUERY_SUCCEEDED.name().equals(eachQuery)) {
        finishedQueries.add(eachQuery);
      }
    }

    return finishedQueries;
  }

  public List<TBriefQueryInfo> getQueryList() throws Exception {
    return new ReconnectThriftServerCallable<List<TBriefQueryInfo>>(currentClient) {
      public List<TBriefQueryInfo> call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getQueryList(sessionId);
      }
    }.withRetries();
  }

  public boolean killQuery(final String queryId) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        TServerResponse response = client.killQuery(sessionId, queryId);
        if (!ResultCode.OK.name().equals(response.getResultCode())) {
          throw new TServiceException(response.getErrorMessage(), response.getDetailErrorMessage());
        }

        return response.isBoolResult();
      }
    }.withRetries();
  }

  public String getCurrentDatabase() throws Exception {
    return new ReconnectThriftServerCallable<String>(currentClient) {
      public String call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getCurrentDatabase(sessionId.toString());
      }
    }.withRetries();
  }

  public boolean updateSessionVariable(final String key, final String value) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.updateSessionVariable(sessionId, key, value);
      }
    }.withRetries();
  }

  public boolean unsetSessionVariable(final String key)  throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.unsetSessionVariables(sessionId, key);
      }
    }.withRetries();
  }

  public String getSessionVariable(final String key) throws Exception {
    return getAllSessionVariables().get(key);
  }

  public Boolean existSessionVariable(final String key) throws Exception {
    return getAllSessionVariables().containsKey(key);
  }

  public Map<String, String> getAllSessionVariables() throws Exception {
    return new ReconnectThriftServerCallable<Map<String, String>>(currentClient) {
      public Map<String, String> call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.getAllSessionVariables(sessionId);
      }
    }.withRetries();
  }

  public Boolean selectDatabase(final String databaseName) throws Exception {
    return new ReconnectThriftServerCallable<Boolean>(currentClient) {
      public Boolean call(Client client) throws Exception {
        checkSessionAndGet(client);
        return client.selectDatabase(sessionId, databaseName).isBoolResult();
      }
    }.withRetries();
  }

  public void close() {
    if(closed.getAndSet(true)){
      return;
    }

    // remove session
    if (currentClient != null && sessionId != null) {
      try {
        currentClient.closeSession(sessionId);
      } catch (Throwable e) {
        LOG.error("Session " + sessionId + " closing error: " + e.getMessage(), e);
      }
    }

    if (currentClient != null) {
      TajoThriftUtil.close(currentClient);
    }
  }

  protected void checkSessionAndGet(Client client) throws Exception {
    if (sessionId == null) {
      TServerResponse response = client.createSession(userInfo.getUserName(), baseDatabase);

      if (ResultCode.OK.name().equals(response.getResultCode())) {
        sessionId = response.getSessionId();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId, userInfo.getUserName()));
        }
      } else {
        throw new InvalidClientSessionException(response.getErrorMessage());
      }
    }
  }

  abstract class ReconnectThriftServerCallable<T> extends ThriftServerCallable<T> {

    public ReconnectThriftServerCallable(Client client) {
      super(client);
    }

    @Override
    protected void failedCall() throws Exception {
      if (client != null) {
        reconnect();
      }
      client = TajoThriftClient.this.currentClient;
    }
  }
}
