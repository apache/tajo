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

package org.apache.tajo.worker;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.QueryContext;
import org.apache.tajo.master.TaskSchedulerImpl;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.querymaster.QueryMaster;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.rpc.ProtoAsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;

public class TajoWorkerManagerService extends CompositeService
    implements TajoWorkerProtocol.TajoWorkerProtocolService.Interface {
  private static final Log LOG = LogFactory.getLog(TajoWorkerManagerService.class.getName());

  private ProtoAsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private int port;

  private QueryMaster queryMaster;

  private TajoWorker.WorkerContext workerContext;

  public TajoWorkerManagerService(TajoWorker.WorkerContext workerContext, int port) {
    super(TajoWorkerManagerService.class.getName());
    this.workerContext = workerContext;
    this.port = port;
  }

  public QueryMaster getQueryMaster() {
    return queryMaster;
  }

  @Override
  public void init(Configuration conf) {
    try {
      // Setup RPC server
      InetSocketAddress initIsa =
          new InetSocketAddress("0.0.0.0", port);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      this.rpcServer = new ProtoAsyncRpcServer(TajoWorkerProtocol.class, this, initIsa);
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
    LOG.info("TajoWorkerManagerService is bind to " + addr);
    ((TajoConf)conf).setVar(TajoConf.ConfVars.TASKRUNNER_LISTENER_ADDRESS, addr);

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
    LOG.info("TajoWorkerManagerService stopped");
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
      ContainerId cid =
          queryMasterTask.getQueryTaskContext().getResourceAllocator().makeContainerId(request.getContainerId());

      if(queryMasterTask == null || queryMasterTask.isStopped()) {
        LOG.debug("getTask:" + cid + ", ebId:" + ebId + ", but query is finished.");
        done.run(TaskSchedulerImpl.stopTaskRunnerReq);
      } else {
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
      QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(
          new QueryId(request.getId().getQueryUnitId().getExecutionBlockId().getQueryId()));
      queryMasterTask.getEventHandler().handle(
          new TaskAttemptStatusUpdateEvent(new QueryUnitAttemptId(request.getId()), request));
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
    // TODO - to be completed
//      QueryUnitAttemptId attemptId = new QueryUnitAttemptId(attemptIdProto);
//    context.getQuery(attemptId.getQueryId()).getSubQuery(attemptId.getExecutionBlockId()).
//        getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId).
//        resetExpireTime();
    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void fatalError(RpcController controller, TajoWorkerProtocol.TaskFatalErrorReport report,
                         RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      QueryMasterTask queryMasterTask = queryMaster.getQueryMasterTask(
          new QueryId(report.getId().getQueryUnitId().getExecutionBlockId().getQueryId()));
      queryMasterTask.getEventHandler().handle(new TaskFatalErrorEvent(report));
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
  public void executeQuery(RpcController controller,
                           TajoWorkerProtocol.QueryExecutionRequestProto request,
                           RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      QueryId queryId = new QueryId(request.getQueryId());
      LOG.info("Receive executeQuery request:" + queryId);
      queryMaster.handle(new QueryStartEvent(queryId,
          new QueryContext(request.getQueryContext()), request.getSql().getValue(),
          request.getLogicalPlanJson().getValue()));
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void executeExecutionBlock(RpcController controller,
                                    TajoWorkerProtocol.RunExecutionBlockRequestProto request,
                                    RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      String[] params = new String[7];
      params[0] = "standby";  //mode(never used)
      params[1] = request.getExecutionBlockId();
      // NodeId has a form of hostname:port.
      params[2] = request.getNodeId();
      params[3] = request.getContainerId();

      // QueryMaster's address
      params[4] = request.getQueryMasterHost();
      params[5] = String.valueOf(request.getQueryMasterPort());
      params[6] = request.getQueryOutputPath();
      workerContext.getTaskRunnerManager().startTask(params);
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      done.run(TajoWorker.FALSE_PROTO);
    }
  }
}
