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

package org.apache.tajo.querymaster;

import com.google.common.collect.Maps;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.QueryId;
import org.apache.tajo.ResourceProtos.TajoHeartbeatRequest;
import org.apache.tajo.ResourceProtos.TajoHeartbeatResponse;
import org.apache.tajo.ResourceProtos.WorkerConnectionsResponse;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.ReturnStateUtil;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.QueryCoordinatorProtocolService;
import org.apache.tajo.master.event.QueryStartEvent;
import org.apache.tajo.master.event.QueryStopEvent;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.HistoryWriter.WriterFuture;
import org.apache.tajo.util.history.HistoryWriter.WriterHolder;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.worker.TajoWorker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());

  private int querySessionTimeout;

  private Clock clock;

  private AsyncDispatcher dispatcher;

  private GlobalPlanner globalPlanner;

  private TajoConf systemConf;

  private Map<QueryId, QueryMasterTask> queryMasterTasks = Maps.newConcurrentMap();

  private LRUMap finishedQueryMasterTasksCache;

  private ClientSessionTimeoutCheckThread clientSessionTimeoutCheckThread;

  private volatile boolean isStopped;

  private QueryMasterContext queryMasterContext;

  private QueryContext queryContext;

  private QueryHeartbeatThread queryHeartbeatThread;

  private TajoWorker.WorkerContext workerContext;

  private RpcClientManager manager;

  private ExecutorService eventExecutor;

  private ExecutorService singleEventExecutor;

  public QueryMaster(TajoWorker.WorkerContext workerContext) {
    super(QueryMaster.class.getName());
    this.workerContext = workerContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    this.manager = RpcClientManager.getInstance();

    querySessionTimeout = systemConf.getIntVar(TajoConf.ConfVars.QUERY_SESSION_TIMEOUT);
    queryMasterContext = new QueryMasterContext(systemConf);

    clock = new SystemClock();
    finishedQueryMasterTasksCache = new LRUMap(systemConf.getIntVar(TajoConf.ConfVars.HISTORY_QUERY_CACHE_SIZE));

    this.dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    globalPlanner = new GlobalPlanner(systemConf, workerContext);

    dispatcher.register(QueryStartEvent.EventType.class, new QueryStartEventHandler());
    dispatcher.register(QueryStopEvent.EventType.class, new QueryStopEventHandler());
    super.serviceInit(conf);
    LOG.info("QueryMaster inited");
  }

  @Override
  public void serviceStart() throws Exception {
    queryHeartbeatThread = new QueryHeartbeatThread();
    queryHeartbeatThread.start();

    clientSessionTimeoutCheckThread = new ClientSessionTimeoutCheckThread();
    clientSessionTimeoutCheckThread.start();

    eventExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    singleEventExecutor = Executors.newSingleThreadExecutor();
    super.serviceStart();
    LOG.info("QueryMaster started");
  }

  @Override
  public void serviceStop() throws Exception {
    isStopped = true;

    if(queryHeartbeatThread != null) {
      queryHeartbeatThread.interrupt();
    }

    if(clientSessionTimeoutCheckThread != null) {
      clientSessionTimeoutCheckThread.interrupt();
    }

    if(eventExecutor != null){
      eventExecutor.shutdown();
    }

    if(singleEventExecutor != null){
      singleEventExecutor.shutdown();
    }

    super.serviceStop();
    LOG.info("QueryMaster stopped");
  }

  public List<TajoProtos.WorkerConnectionInfoProto> getAllWorker() {

    NettyClientBase rpc = null;
    try {
      // In TajoMaster HA mode, if backup master be active status,
      // worker may fail to connect existing active master. Thus,
      // if worker can't connect the master, worker should try to connect another master and
      // update master address in worker context.

      ServiceTracker serviceTracker = workerContext.getServiceTracker();
      rpc = manager.getClient(serviceTracker.getUmbilicalAddress(), QueryCoordinatorProtocol.class, true);
      QueryCoordinatorProtocolService masterService = rpc.getStub();

      CallFuture<WorkerConnectionsResponse> callBack = new CallFuture<WorkerConnectionsResponse>();
      masterService.getAllWorkers(callBack.getController(),
          PrimitiveProtos.NullProto.getDefaultInstance(), callBack);

      WorkerConnectionsResponse connectionsProto =
          callBack.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return connectionsProto.getWorkerList();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return new ArrayList<TajoProtos.WorkerConnectionInfoProto>();
  }

  @Override
  public void handle(Event event) {
    dispatcher.getEventHandler().handle(event);
  }

  public Query getQuery(QueryId queryId) {
    return queryMasterTasks.get(queryId).getQuery();
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId) {
    return queryMasterTasks.get(queryId);
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId, boolean includeFinished) {
    QueryMasterTask queryMasterTask = queryMasterTasks.get(queryId);
    if (queryMasterTask != null) {
      return queryMasterTask;
    } else {
      if (includeFinished) {
        synchronized (finishedQueryMasterTasksCache) {
          return (QueryMasterTask) finishedQueryMasterTasksCache.get(queryId);
        }
      } else {
        return null;
      }
    }
  }

  public QueryMasterContext getContext() {
    return this.queryMasterContext;
  }

  public Collection<QueryMasterTask> getQueryMasterTasks() {
    return queryMasterTasks.values();
  }

  public QueryHistory getQueryHistory(QueryId queryId) throws IOException {
    QueryMasterTask queryMasterTask = getQueryMasterTask(queryId, true);
    if(queryMasterTask != null) {
      return queryMasterTask.getQuery().getQueryHistory();
    } else {
      return workerContext.getHistoryReader().getQueryHistory(queryId.toString());
    }
  }

  public class QueryMasterContext {
    private TajoConf conf;

    public QueryMasterContext(TajoConf conf) {
      this.conf = conf;
    }

    public TajoConf getConf() {
      return conf;
    }

    public ExecutorService getEventExecutor(){
      return eventExecutor;
    }

    public ExecutorService getSingleEventExecutor(){
      return singleEventExecutor;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Clock getClock() {
      return clock;
    }

    public QueryMaster getQueryMaster() {
      return QueryMaster.this;
    }

    public GlobalPlanner getGlobalPlanner() {
      return globalPlanner;
    }

    public TajoWorker.WorkerContext getWorkerContext() {
      return workerContext;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public void stopQuery(final QueryId queryId) {
      QueryMasterTask queryMasterTask = queryMasterTasks.get(queryId);
      if(queryMasterTask == null) {
        LOG.warn("No query info:" + queryId);
        return;
      }

      synchronized (finishedQueryMasterTasksCache) {
        finishedQueryMasterTasksCache.put(queryId, queryMasterTask);
      }

      queryMasterTasks.remove(queryId);

      TajoHeartbeatRequest queryHeartbeat = buildTajoHeartBeat(queryMasterTask);
      CallFuture<TajoHeartbeatResponse> future = new CallFuture<TajoHeartbeatResponse>();

      NettyClientBase tmClient;
      try {
        tmClient = manager.getClient(workerContext.getServiceTracker().getUmbilicalAddress(),
            QueryCoordinatorProtocol.class, true);

        QueryCoordinatorProtocolService masterClientService = tmClient.getStub();
        masterClientService.heartbeat(future.getController(), queryHeartbeat, future);
        future.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }  catch (Exception e) {
        //this function will be closed in new thread.
        //When tajo do stop cluster, tajo master maybe throw closed connection exception

        LOG.error(e.getMessage(), e);
      }

      try {
        queryMasterTask.stop();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
      Query query = queryMasterTask.getQuery();
      if (query != null) {
        QueryHistory queryHisory = query.getQueryHistory();
        if (queryHisory != null) {
          try {
            WriterFuture<WriterHolder> writerFuture = new WriterFuture<WriterHolder>(queryHisory) {
              @Override
              public void done(WriterHolder writerHolder) {
                super.done(writerHolder);

                //remove memory cache, if history file writer is done
                synchronized (finishedQueryMasterTasksCache) {
                  finishedQueryMasterTasksCache.remove(queryId);
                }
              }
            };
            query.context.getQueryMasterContext().getWorkerContext().
                getTaskHistoryWriter().appendHistory(writerFuture);
          } catch (Throwable e) {
            LOG.warn(e, e);
          }
        }
      }
    }
  }

  private TajoHeartbeatRequest buildTajoHeartBeat(QueryMasterTask queryMasterTask) {
    TajoHeartbeatRequest.Builder builder = TajoHeartbeatRequest.newBuilder();

    builder.setConnectionInfo(workerContext.getConnectionInfo().getProto());
    builder.setQueryId(queryMasterTask.getQueryId().getProto());
    builder.setState(queryMasterTask.getState());
    if (queryMasterTask.getQuery() != null) {
      if (queryMasterTask.getQuery().getResultDesc() != null) {
        builder.setResultDesc(queryMasterTask.getQuery().getResultDesc().getProto());
      }
      builder.setQueryProgress(queryMasterTask.getQuery().getProgress());
    }
    if (queryMasterTask.isInitError()) {
      builder.setStatusMessage(ReturnStateUtil.returnError(queryMasterTask.getInitError()).getMessage());
    }
    return builder.build();
  }

  private class QueryStartEventHandler implements EventHandler<QueryStartEvent> {
    @Override
    public void handle(QueryStartEvent event) {
      LOG.info("Start QueryStartEventHandler:" + event.getQueryId());
      QueryMasterTask queryMasterTask = new QueryMasterTask(queryMasterContext,
          event.getQueryId(), event.getSession(), event.getQueryContext(), event.getJsonExpr(), event.getAllocation());

      synchronized(queryMasterTasks) {
        queryMasterTasks.put(event.getQueryId(), queryMasterTask);
      }

      queryMasterTask.init(systemConf);
      if (!queryMasterTask.isInitError()) {
        queryMasterTask.start();
      }

      queryContext = event.getQueryContext();

      if (queryMasterTask.isInitError()) {
        queryMasterContext.stopQuery(queryMasterTask.getQueryId());
      }
    }
  }

  private class QueryStopEventHandler implements EventHandler<QueryStopEvent> {
    @Override
    public void handle(QueryStopEvent event) {
      queryMasterContext.stopQuery(event.getQueryId());
    }
  }

  class QueryHeartbeatThread extends Thread {
    public QueryHeartbeatThread() {
      super("QueryHeartbeatThread");
    }

    @Override
    public void run() {
      LOG.info("Start QueryMaster heartbeat thread");
      while(!isStopped) {
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        tempTasks.addAll(queryMasterTasks.values());

        for(QueryMasterTask eachTask: tempTasks) {
          NettyClientBase tmClient;
          try {

            ServiceTracker serviceTracker = queryMasterContext.getWorkerContext().getServiceTracker();
            tmClient = manager.getClient(serviceTracker.getUmbilicalAddress(),
                QueryCoordinatorProtocol.class, true);
            QueryCoordinatorProtocolService masterClientService = tmClient.getStub();

            TajoHeartbeatRequest queryHeartbeat = buildTajoHeartBeat(eachTask);
            masterClientService.heartbeat(null, queryHeartbeat, NullCallback.get());
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }

        synchronized(this) {
          try {
            this.wait(2000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      LOG.info("QueryMaster heartbeat thread stopped");
    }
  }

  class ClientSessionTimeoutCheckThread extends Thread {
    public void run() {
      LOG.info("ClientSessionTimeoutCheckThread started");
      while(!isStopped) {
        try {
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException e) {
          break;
        }
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        tempTasks.addAll(queryMasterTasks.values());

        for(QueryMasterTask eachTask: tempTasks) {
          if(!eachTask.isStopped()) {
            try {
              long lastHeartbeat = eachTask.getLastClientHeartbeat();
              long time = System.currentTimeMillis() - lastHeartbeat;
              if(lastHeartbeat > 0 && time > querySessionTimeout * 1000) {
                LOG.warn("Query " + eachTask.getQueryId() + " stopped cause query session timeout: " + time + " ms");
                eachTask.expireQuerySession();
              }
            } catch (Exception e) {
              LOG.error(eachTask.getQueryId() + ":" + e.getMessage(), e);
            }
          }
        }
      }
    }
  }
}
