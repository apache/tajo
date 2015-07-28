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

package org.apache.tajo.client.v2;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.catalog.exception.UndefinedDatabaseException;
import org.apache.tajo.client.DummyServiceTracker;
import org.apache.tajo.client.QueryClientImpl;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.GetQueryStatusResponse;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerException;
import org.apache.tajo.service.TajoMasterInfo;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.exception.ReturnStateUtil.ensureOk;

@ThreadSafe
public class LegacyClientDelegate extends SessionConnection implements ClientDelegate {

  private QueryClientImpl queryClient;
  private ExecutorService executor = Executors.newFixedThreadPool(8);

  public LegacyClientDelegate(String host, int port, Map<String, String> props) {
    super(new DummyServiceTracker(NetUtils.createSocketAddr(host, port)), null, new KeyValueSet(props));
    queryClient = new QueryClientImpl(this);
  }

  public LegacyClientDelegate(ServiceDiscovery discovery, Map<String, String> props) {
    super(new DelegateServiceTracker(discovery), null, new KeyValueSet(props));
    queryClient = new QueryClientImpl(this);
  }

  @Override
  public int executeUpdate(String sql) throws TajoException {
    queryClient.updateQuery(sql);
    return 0;
  }

  @Override
  public ResultSet executeSQL(String sql) throws TajoException {
    try {
      return executeSQLAsync(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public QueryFuture executeSQLAsync(String sql) throws TajoException {
    ClientProtos.SubmitQueryResponse response = queryClient.executeQuery(sql);
    ensureOk(response.getState());

    QueryId queryId = new QueryId(response.getQueryId());

    switch (response.getResultType()) {
    case ENCLOSED:
      return new QueryFutureForEnclosed(queryId, TajoClientUtil.createResultSet(this.queryClient, response, 200));
    case FETCH:
      AsyncQueryFuture future = new AsyncQueryFuture(queryId);
      executor.execute(future);
      return future;
    default:
      return new QueryFutureForNoFetch(queryId);
    }
  }

  @Override
  public String currentDB() {
    return getCurrentDatabase();
  }

  @Override
  public void selectDB(String db) throws UndefinedDatabaseException {
    selectDatabase(db);
  }

  private class QueryFutureForNoFetch implements QueryFuture {
    private final QueryId id;
    private final long now = System.currentTimeMillis();

    QueryFutureForNoFetch(QueryId id) {
      this.id = id;
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public String queue() {
      return "default";
    }

    @Override
    public QueryState state() {
      return QueryState.COMPLETED;
    }

    @Override
    public float progress() {
      return 1.0f;
    }

    @Override
    public boolean isOk() {
      return true;
    }

    @Override
    public boolean isCompleted() {
      return true;
    }

    @Override
    public boolean isFailed() {
      return false;
    }

    @Override
    public boolean isKilled() {
      return false;
    }

    @Override
    public UserRoleInfo user() {
      return UserRoleInfo.getCurrentUser();
    }

    @Override
    public void kill() {
    }

    @Override
    public long submitTime() {
      return 0;
    }

    @Override
    public long startTime() {
      return now;
    }

    @Override
    public long finishTime() {
      return now;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public ResultSet get() {
      return TajoClientUtil.NULL_RESULT_SET;
    }

    @Override
    public ResultSet get(long timeout, TimeUnit unit) {
      return TajoClientUtil.NULL_RESULT_SET;
    }
  }

  private class QueryFutureForEnclosed extends QueryFutureForNoFetch {
    private final ResultSet resultSet;
    QueryFutureForEnclosed(QueryId id, ResultSet resultSet) {
      super(id);
      this.resultSet = resultSet;
    }

    @Override
    public ResultSet get() {
      return resultSet;
    }

    @Override
    public ResultSet get(long timeout, TimeUnit unit) {
      return resultSet;
    }
  }

  private class AsyncQueryFuture extends AbstractFuture<ResultSet> implements QueryFuture, Runnable {
    private final QueryId queryId;
    private volatile QueryState lastState;
    private volatile float progress;
    private final long submitTime = System.currentTimeMillis();
    private volatile long startTime = 0;
    private volatile long finishTime = 0;

    public AsyncQueryFuture(QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public String id() {
      return queryId.toString();
    }

    @Override
    public boolean isOk() {
      return ClientUtil.isOk(lastState);
    }

    @Override
    public boolean isCompleted() {
      return lastState == QueryState.COMPLETED;
    }

    @Override
    public boolean isFailed() {
      return ClientUtil.isFailed(lastState);
    }

    @Override
    public boolean isKilled() {
      return queryClient.getQueryStatus(queryId).getState() == TajoProtos.QueryState.QUERY_KILLED;
    }

    @Override
    public QueryState state() {
      return lastState;
    }

    @Override
    public String queue() {
      return "default";
    }

    @Override
    public UserRoleInfo user() {
      return UserRoleInfo.getCurrentUser();
    }

    @Override
    public float progress() {
      return progress;
    }

    @Override
    public void kill() {
      queryClient.killQuery(queryId).getState();
    }

    @Override
    public long submitTime() {
      return submitTime;
    }

    @Override
    public long startTime() {
      return startTime;
    }

    @Override
    public long finishTime() {
      return finishTime;
    }

    private void updateState(GetQueryStatusResponse lastState) {
      this.startTime = lastState.getSubmitTime();
      this.finishTime = lastState.getFinishTime();
      this.progress = lastState.getProgress();
      this.lastState = convert(lastState.getQueryState());
    }

    GetQueryStatusResponse waitCompletion() {
      GetQueryStatusResponse response = queryClient.getRawQueryStatus(queryId);
      ensureOk(response.getState());
      updateState(response);

      while(!TajoClientUtil.isQueryComplete(response.getQueryState())) {
        try {
          Thread.sleep(500);
          updateState(response);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        response = queryClient.getRawQueryStatus(queryId);
        ensureOk(response.getState());
      }
      return response;
    }

    @Override
    public void run() {
      GetQueryStatusResponse finalResponse;
      try {
        finalResponse = waitCompletion();
      } catch (Throwable t) {
        setException(t);
        return;
      }

      if (finalResponse.getQueryState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        if (finalResponse.hasHasResult()) {
          set(queryClient.getQueryResult(queryId));
        } else { // when update
          set(TajoClientUtil.NULL_RESULT_SET);
        }
      } else {
        cancel(false); // failed
        set(TajoClientUtil.NULL_RESULT_SET);
      }
    }
  }

  private static class DelegateServiceTracker implements ServiceTracker {

    private final ServiceDiscovery discovery;
    DelegateServiceTracker(ServiceDiscovery discovery) {
      this.discovery = discovery;
    }

    @Override
    public boolean isHighAvailable() {
      return false;
    }

    @Override
    public InetSocketAddress getUmbilicalAddress() throws ServiceTrackerException {
      return null;
    }

    @Override
    public InetSocketAddress getClientServiceAddress() throws ServiceTrackerException {
      return discovery.clientAddress();
    }

    @Override
    public InetSocketAddress getResourceTrackerAddress() throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public InetSocketAddress getCatalogAddress() throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public InetSocketAddress getMasterHttpInfo() throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public int getState(String masterName, TajoConf conf) throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public int formatHA(TajoConf conf) throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public List<String> getMasters(TajoConf conf) throws ServiceTrackerException {
      throw new UnimplementedException();
    }

    @Override
    public void register() throws IOException {
      throw new UnimplementedException();
    }

    @Override
    public void delete() throws IOException {
      throw new UnimplementedException();
    }

    @Override
    public boolean isActiveMaster() {
      throw new UnimplementedException();
    }

    @Override
    public List<TajoMasterInfo> getMasters() throws IOException {
      throw new UnimplementedException();
    }
  }

  public static QueryState convert(TajoProtos.QueryState state) {
    switch (state) {
    case QUERY_NEW:
    case QUERY_INIT:
    case QUERY_NOT_ASSIGNED:
      return QueryState.SCHEDULED;

    case QUERY_MASTER_INIT:
    case QUERY_MASTER_LAUNCHED:
    case QUERY_RUNNING:
      return QueryState.RUNNING;

    case QUERY_KILL_WAIT:
      return QueryState.KILLING;

    case QUERY_KILLED:
      return QueryState.KILLED;

    case QUERY_FAILED:
      return QueryState.FAILED;

    case QUERY_ERROR:
      return QueryState.ERROR;

    case QUERY_SUCCEEDED:
      return QueryState.COMPLETED;

    default:
      throw new RuntimeException("Unknown state:" + state.name());
    }
  }
}
