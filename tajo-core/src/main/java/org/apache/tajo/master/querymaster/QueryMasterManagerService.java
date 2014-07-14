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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tajo.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.LazyTaskScheduler;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.TajoWorker;

import java.net.InetSocketAddress;

public class QueryMasterManagerService extends CompositeService
    implements QueryMasterProtocol.QueryMasterProtocolService.Interface {
  private static final Log LOG = LogFactory.getLog(QueryMasterManagerService.class.getName());

  private AsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private int port;

  private QueryMaster queryMaster;

  private TajoWorker.WorkerContext workerContext;

  public QueryMasterManagerService(TajoWorker.WorkerContext workerContext, int port) {
    super(QueryMasterManagerService.class.getName());
    this.workerContext = workerContext;
    this.port = port;
  }

  public QueryMaster getQueryMaster() {
    return queryMaster;
  }

  @Override
  public void init(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf);
    TajoConf tajoConf = (TajoConf) conf;
    try {
      // Setup RPC server
      InetSocketAddress initIsa =
          new InetSocketAddress("0.0.0.0", port);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      int workerNum = tajoConf.getIntVar(TajoConf.ConfVars.QUERY_MASTER_RPC_SERVER_WORKER_THREAD_NUM);
      this.rpcServer = new AsyncRpcServer(QueryMasterProtocol.class, this, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
      this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

      this.port = bindAddr.getPort();

      queryMaster = new QueryMaster(workerContext);
      addService(queryMaster);

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info("QueryMasterManagerService is bind to " + addr);
    ((TajoConf)conf).setVar(TajoConf.ConfVars.WORKER_QM_RPC_ADDRESS, addr);

    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    if(rpcServer != null) {
      rpcServer.shutdown();
    }
    LOG.info("QueryMasterManagerService stopped");
    super.stop();
  }

  public InetSocketAddress getBindAddr() {
    return bindAddr;
  }

  public String getHostAndPort() {
    return bindAddr.getHostName() + ":" + bindAddr.getPort();
  }

  @Override
  public void getTask(RpcController controller, TajoWorkerProtocol.GetTaskRequestProto request,
                      RpcCallback<TajoWorkerProtocol.QueryUnitRequestProto> done) {
    try {
      ExecutionBlockId ebId = new ExecutionBlockId(request.getExecutionBlockId());
      QueryMasterTask queryMasterTask = workerContext.getQueryMaster().getQueryMasterTask(ebId.getQueryId());

      if(queryMasterTask == null || queryMasterTask.isStopped()) {
        done.run(LazyTaskScheduler.stopTaskRunnerReq);
      } else {
        ContainerId cid =
            queryMasterTask.getQueryTaskContext().getResourceAllocator().makeContainerId(request.getContainerId());
        LOG.debug("getTask:" + cid + ", ebId:" + ebId);
        queryMasterTask.handleTaskRequestEvent(new TaskRequestEvent(cid, ebId, done));
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void statusUpdate(RpcController controller, TajoWorkerProtocol.TaskStatusProto request,
                           RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      QueryId queryId = new QueryId(request.getId().getQueryUnitId().getExecutionBlockId().getQueryId());
      QueryUnitAttemptId attemptId = new QueryUnitAttemptId(request.getId());
      QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(queryId);
      if (queryMasterTask == null) {
        queryMasterTask = queryMaster.getQueryMasterTask(queryId, true);
      }
      SubQuery sq = queryMasterTask.getQuery().getSubQuery(attemptId.getQueryUnitId().getExecutionBlockId());
      QueryUnit task = sq.getQueryUnit(attemptId.getQueryUnitId());
      QueryUnitAttempt attempt = task.getAttempt(attemptId.getId());

      if(LOG.isDebugEnabled()){
        LOG.debug(String.format("Task State: %s, Attempt State: %s", task.getState().name(), attempt.getState().name()));
      }

      if (request.getState() == TajoProtos.TaskAttemptState.TA_KILLED) {
        LOG.warn(attemptId + " Killed");
        attempt.handle(
            new TaskAttemptEvent(new QueryUnitAttemptId(request.getId()), TaskAttemptEventType.TA_LOCAL_KILLED));
      } else {
        queryMasterTask.getEventHandler().handle(
            new TaskAttemptStatusUpdateEvent(new QueryUnitAttemptId(request.getId()), request));
      }
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void ping(RpcController controller,
                   TajoIdProtos.QueryUnitAttemptIdProto attemptId,
                   RpcCallback<PrimitiveProtos.BoolProto> done) {
    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void fatalError(RpcController controller, TajoWorkerProtocol.TaskFatalErrorReport report,
                         RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(
          new QueryId(report.getId().getQueryUnitId().getExecutionBlockId().getQueryId()));
      if (queryMasterTask != null) {
        queryMasterTask.handleTaskFailed(report);
      } else {
        LOG.warn("No QueryMasterTask: " + new QueryUnitAttemptId(report.getId()));
      }
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void done(RpcController controller, TajoWorkerProtocol.TaskCompletionReport report,
                   RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(
          new QueryId(report.getId().getQueryUnitId().getExecutionBlockId().getQueryId()));
      queryMasterTask.getEventHandler().handle(new TaskCompletionEvent(report));
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void killQuery(RpcController controller, TajoIdProtos.QueryIdProto request,
                        RpcCallback<PrimitiveProtos.BoolProto> done) {
    QueryId queryId = new QueryId(request);
    QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(queryId);
    if (queryMasterTask != null) {
      Query query = queryMasterTask.getQuery();
      if (query != null) {
        query.handle(new QueryEvent(queryId, QueryEventType.KILL));
      }
    }
  }

  @Override
  public void executeQuery(RpcController controller,
                           TajoWorkerProtocol.QueryExecutionRequestProto request,
                           RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      workerContext.getWorkerSystemMetrics().counter("querymaster", "numQuery").inc();

      QueryId queryId = new QueryId(request.getQueryId());
      LOG.info("Receive executeQuery request:" + queryId);
      queryMaster.handle(new QueryStartEvent(queryId,
          new Session(request.getSession()),
          new QueryContext(request.getQueryContext()), request.getExprInJson().getValue(),
          request.getLogicalPlanJson().getValue()));
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      workerContext.getWorkerSystemMetrics().counter("querymaster", "errorQuery").inc();
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }
}
