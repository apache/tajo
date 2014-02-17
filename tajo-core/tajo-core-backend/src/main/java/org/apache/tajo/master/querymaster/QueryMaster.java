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

package org.apache.tajo.master.querymaster;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TajoAsyncDispatcher;
import org.apache.tajo.master.event.QueryStartEvent;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.TajoWorker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.TajoMasterProtocol.TajoHeartbeat;
import static org.apache.tajo.ipc.TajoMasterProtocol.TajoHeartbeatResponse;

// TODO - when exception, send error status to QueryJobManager
public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());

  private int querySessionTimeout;

  private Clock clock;

  private TajoAsyncDispatcher dispatcher;

  private GlobalPlanner globalPlanner;

  private AbstractStorageManager storageManager;

  private TajoConf systemConf;

  private Map<QueryId, QueryMasterTask> queryMasterTasks = Maps.newConcurrentMap();

  private Map<QueryId, QueryMasterTask> finishedQueryMasterTasks = Maps.newConcurrentMap();

  private ClientSessionTimeoutCheckThread clientSessionTimeoutCheckThread;

  private AtomicBoolean queryMasterStop = new AtomicBoolean(false);

  private QueryMasterContext queryMasterContext;

  private QueryHeartbeatThread queryHeartbeatThread;

  private FinishedQueryMasterTaskCleanThread finishedQueryMasterTaskCleanThread;

  private TajoWorker.WorkerContext workerContext;

  private RpcConnectionPool connPool;

  public QueryMaster(TajoWorker.WorkerContext workerContext) {
    super(QueryMaster.class.getName());
    this.workerContext = workerContext;
  }

  public void init(Configuration conf) {
    LOG.info("QueryMaster init");
    try {
      this.systemConf = (TajoConf)conf;
      this.connPool = RpcConnectionPool.getPool(systemConf);

      querySessionTimeout = systemConf.getIntVar(TajoConf.ConfVars.QUERY_SESSION_TIMEOUT);
      queryMasterContext = new QueryMasterContext(systemConf);

      clock = new SystemClock();

      this.dispatcher = new TajoAsyncDispatcher("querymaster_" + System.currentTimeMillis());
      addIfService(dispatcher);

      this.storageManager = StorageManagerFactory.getStorageManager(systemConf);

      globalPlanner = new GlobalPlanner(systemConf, storageManager);

      dispatcher.register(QueryStartEvent.EventType.class, new QueryStartEventHandler());

    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new RuntimeException(t);
    }
    super.init(conf);
  }

  @Override
  public void start() {
    LOG.info("QueryMaster start");

    queryHeartbeatThread = new QueryHeartbeatThread();
    queryHeartbeatThread.start();

    clientSessionTimeoutCheckThread = new ClientSessionTimeoutCheckThread();
    clientSessionTimeoutCheckThread.start();

    finishedQueryMasterTaskCleanThread = new FinishedQueryMasterTaskCleanThread();
    finishedQueryMasterTaskCleanThread.start();

    super.start();
  }

  @Override
  public void stop() {
    if(queryMasterStop.getAndSet(true)){
      return;
    }

    if(queryHeartbeatThread != null) {
      queryHeartbeatThread.interrupt();
    }

    if(clientSessionTimeoutCheckThread != null) {
      clientSessionTimeoutCheckThread.interrupt();
    }

    if(finishedQueryMasterTaskCleanThread != null) {
      finishedQueryMasterTaskCleanThread.interrupt();
    }
    super.stop();

    LOG.info("QueryMaster stop");
    if(queryMasterContext.getWorkerContext().isYarnContainerMode()) {
      queryMasterContext.getWorkerContext().stopWorker(true);
    }
  }

  private void cleanup(QueryId queryId) {
    LOG.info("cleanup query resources : " + queryId);
    NettyClientBase rpc = null;
    List<TajoMasterProtocol.WorkerResourceProto> workers = getAllWorker();

    for (TajoMasterProtocol.WorkerResourceProto worker : workers) {
      try {
        if (worker.getPeerRpcPort() == 0) continue;

        rpc = connPool.getConnection(NetUtils.createSocketAddr(worker.getHost(), worker.getPeerRpcPort()),
            TajoWorkerProtocol.class, true);
        TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerProtocolService = rpc.getStub();

        tajoWorkerProtocolService.cleanup(null, queryId.getProto(), NullCallback.get());
      } catch (Exception e) {
        LOG.error(e.getMessage());
      } finally {
        connPool.releaseConnection(rpc);
      }
    }
  }

  public List<TajoMasterProtocol.WorkerResourceProto> getAllWorker() {

    NettyClientBase rpc = null;
    try {
      rpc = connPool.getConnection(queryMasterContext.getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterService = rpc.getStub();

      CallFuture<TajoMasterProtocol.WorkerResourcesRequest> callBack =
          new CallFuture<TajoMasterProtocol.WorkerResourcesRequest>();
      masterService.getAllWorkerResource(callBack.getController(),
          PrimitiveProtos.NullProto.getDefaultInstance(), callBack);

      TajoMasterProtocol.WorkerResourcesRequest workerResourcesRequest = callBack.get(2, TimeUnit.SECONDS);
      return workerResourcesRequest.getWorkerResourcesList();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(rpc);
    }
    return new ArrayList<TajoMasterProtocol.WorkerResourceProto>();
  }

  public void reportQueryStatusToQueryMaster(QueryId queryId, TajoProtos.QueryState state) {
    LOG.info("Send QueryMaster Ready to QueryJobManager:" + queryId);
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(queryMasterContext.getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();

      TajoHeartbeat.Builder queryHeartbeatBuilder = TajoHeartbeat.newBuilder()
          .setTajoWorkerHost(workerContext.getQueryMasterManagerService().getBindAddr().getHostName())
          .setPeerRpcPort(workerContext.getPeerRpcPort())
          .setTajoQueryMasterPort(workerContext.getQueryMasterManagerService().getBindAddr().getPort())
          .setTajoWorkerClientPort(workerContext.getTajoWorkerClientService().getBindAddr().getPort())
          .setState(state)
          .setQueryId(queryId.getProto());

      CallFuture<TajoHeartbeatResponse> callBack =
          new CallFuture<TajoHeartbeatResponse>();

      masterClientService.heartbeat(callBack.getController(), queryHeartbeatBuilder.build(), callBack);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }
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
    QueryMasterTask queryMasterTask =  queryMasterTasks.get(queryId);
    if(queryMasterTask != null) {
      return queryMasterTask;
    } else {
      if(includeFinished) {
        return finishedQueryMasterTasks.get(queryId);
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

  public Collection<QueryMasterTask> getFinishedQueryMasterTasks() {
    return finishedQueryMasterTasks.values();
  }

  public class QueryMasterContext {
    private TajoConf conf;

    public QueryMasterContext(TajoConf conf) {
      this.conf = conf;
    }

    public TajoConf getConf() {
      return conf;
    }

    public TajoAsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Clock getClock() {
      return clock;
    }

    public AbstractStorageManager getStorageManager() {
      return storageManager;
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

    public void stopQuery(QueryId queryId) {
      QueryMasterTask queryMasterTask;
      queryMasterTask = queryMasterTasks.remove(queryId);
      finishedQueryMasterTasks.put(queryId, queryMasterTask);

      if(queryMasterTask != null) {
        TajoHeartbeat queryHeartbeat = buildTajoHeartBeat(queryMasterTask);
        CallFuture<TajoHeartbeatResponse> future = new CallFuture<TajoHeartbeatResponse>();

        NettyClientBase tmClient = null;
        try {
          tmClient = connPool.getConnection(queryMasterContext.getWorkerContext().getTajoMasterAddress(),
              TajoMasterProtocol.class, true);
          TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
          masterClientService.heartbeat(future.getController(), queryHeartbeat, future);
        }  catch (Exception e) {
          //this function will be closed in new thread.
          //When tajo do stop cluster, tajo master maybe throw closed connection exception

          LOG.error(e.getMessage(), e);
        } finally {
          connPool.releaseConnection(tmClient);
        }

        try {
          queryMasterTask.stop();
          if (!systemConf.get(CommonTestingUtil.TAJO_TEST, "FALSE").equalsIgnoreCase("TRUE")
              && !workerContext.isYarnContainerMode()) {
            cleanup(queryId);       // TODO We will support yarn mode
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      } else {
        LOG.warn("No query info:" + queryId);
      }
      if(workerContext.isYarnContainerMode()) {
        stop();
      }
    }
  }

  private TajoHeartbeat buildTajoHeartBeat(QueryMasterTask queryMasterTask) {
    TajoHeartbeat queryHeartbeat = TajoHeartbeat.newBuilder()
        .setTajoWorkerHost(workerContext.getQueryMasterManagerService().getBindAddr().getHostName())
        .setTajoQueryMasterPort(workerContext.getQueryMasterManagerService().getBindAddr().getPort())
        .setPeerRpcPort(workerContext.getPeerRpcPort())
        .setTajoWorkerClientPort(workerContext.getTajoWorkerClientService().getBindAddr().getPort())
        .setState(queryMasterTask.getState())
        .setQueryId(queryMasterTask.getQueryId().getProto())
        .setQueryProgress(queryMasterTask.getQuery().getProgress())
        .setQueryFinishTime(queryMasterTask.getQuery().getFinishTime())
        .build();
    return queryHeartbeat;
  }

  private class QueryStartEventHandler implements EventHandler<QueryStartEvent> {
    @Override
    public void handle(QueryStartEvent event) {
      LOG.info("Start QueryStartEventHandler:" + event.getQueryId());
      QueryMasterTask queryMasterTask = new QueryMasterTask(queryMasterContext,
          event.getQueryId(), event.getQueryContext(), event.getSql(), event.getLogicalPlanJson());

      queryMasterTask.init(systemConf);
      queryMasterTask.start();
      synchronized(queryMasterTasks) {
        queryMasterTasks.put(event.getQueryId(), queryMasterTask);
      }
    }
  }

  class QueryHeartbeatThread extends Thread {
    public QueryHeartbeatThread() {
      super("QueryHeartbeatThread");
    }

    @Override
    public void run() {
      LOG.info("Start QueryMaster heartbeat thread");
      while(!queryMasterStop.get()) {
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        synchronized(queryMasterTasks) {
          tempTasks.addAll(queryMasterTasks.values());
        }
        synchronized(queryMasterTasks) {
          for(QueryMasterTask eachTask: tempTasks) {
            NettyClientBase tmClient;
            try {
              tmClient = connPool.getConnection(queryMasterContext.getWorkerContext().getTajoMasterAddress(),
                  TajoMasterProtocol.class, true);
              TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();

              CallFuture<TajoHeartbeatResponse> callBack =
                  new CallFuture<TajoHeartbeatResponse>();

              TajoHeartbeat queryHeartbeat = buildTajoHeartBeat(eachTask);
              masterClientService.heartbeat(callBack.getController(), queryHeartbeat, callBack);
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
        }
        synchronized(queryMasterStop) {
          try {
            queryMasterStop.wait(2000);
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
      while(!queryMasterStop.get()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        synchronized(queryMasterTasks) {
          tempTasks.addAll(queryMasterTasks.values());
        }

        for(QueryMasterTask eachTask: tempTasks) {
          if(!eachTask.isStopped()) {
            try {
              long lastHeartbeat = eachTask.getLastClientHeartbeat();
              long time = System.currentTimeMillis() - lastHeartbeat;
              if(lastHeartbeat > 0 && time > querySessionTimeout * 1000) {
                LOG.warn("Query " + eachTask.getQueryId() + " stopped cause query sesstion timeout: " + time + " ms");
                eachTask.expiredSessionTimeout();
              }
            } catch (Exception e) {
              LOG.error(eachTask.getQueryId() + ":" + e.getMessage(), e);
            }
          }
        }
      }
    }
  }

  class FinishedQueryMasterTaskCleanThread extends Thread {
    public void run() {
      int expireIntervalTime = systemConf.getIntVar(TajoConf.ConfVars.WORKER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!queryMasterStop.get()) {
        try {
          Thread.sleep(60 * 1000 * 60);   // hourly
        } catch (InterruptedException e) {
          break;
        }
        try {
          long expireTime = System.currentTimeMillis() - expireIntervalTime * 60 * 1000;
          cleanExpiredFinishedQueryMasterTask(expireTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    private void cleanExpiredFinishedQueryMasterTask(long expireTime) {
      synchronized(finishedQueryMasterTasks) {
        List<QueryId> expiredQueryIds = new ArrayList<QueryId>();
        for(Map.Entry<QueryId, QueryMasterTask> entry: finishedQueryMasterTasks.entrySet()) {
          if(entry.getValue().getStartTime() < expireTime) {
            expiredQueryIds.add(entry.getKey());
          }
        }

        for(QueryId eachId: expiredQueryIds) {
          finishedQueryMasterTasks.remove(eachId);
        }
      }
    }
  }
}
