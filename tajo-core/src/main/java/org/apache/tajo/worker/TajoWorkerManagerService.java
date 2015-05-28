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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.event.TaskRunnerStartEvent;
import org.apache.tajo.worker.event.TaskRunnerStopEvent;

import java.net.InetSocketAddress;

public class TajoWorkerManagerService extends CompositeService
    implements TajoWorkerProtocol.TajoWorkerProtocolService.Interface {
  private static final Log LOG = LogFactory.getLog(TajoWorkerManagerService.class.getName());

  private AsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private int port;

  private TajoWorker.WorkerContext workerContext;

  public TajoWorkerManagerService(TajoWorker.WorkerContext workerContext, int port) {
    super(TajoWorkerManagerService.class.getName());
    this.workerContext = workerContext;
    this.port = port;
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

      int workerNum = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_RPC_SERVER_WORKER_THREAD_NUM);
      this.rpcServer = new AsyncRpcServer(TajoWorkerProtocol.class, this, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
      this.port = bindAddr.getPort();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info("TajoWorkerManagerService is bind to " + bindAddr);
    tajoConf.setVar(TajoConf.ConfVars.WORKER_PEER_RPC_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddr));
    super.init(tajoConf);
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

  @Override
  public void ping(RpcController controller,
                   TajoIdProtos.TaskAttemptIdProto attemptId,
                   RpcCallback<PrimitiveProtos.BoolProto> done) {
    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void startExecutionBlock(RpcController controller,
                                    TajoWorkerProtocol.RunExecutionBlockRequestProto request,
                                    RpcCallback<PrimitiveProtos.BoolProto> done) {
    workerContext.getWorkerSystemMetrics().counter("query", "executedExecutionBlocksNum").inc();

    try {
      workerContext.getTaskRunnerManager().getEventHandler().handle(new TaskRunnerStartEvent(request));
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      controller.setFailed(t.getMessage());
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void stopExecutionBlock(RpcController controller,
                                 TajoIdProtos.ExecutionBlockIdProto requestProto,
                                 RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {
      workerContext.getTaskRunnerManager().getEventHandler().handle(new TaskRunnerStopEvent(
          new ExecutionBlockId(requestProto)
      ));
      done.run(TajoWorker.TRUE_PROTO);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      controller.setFailed(e.getMessage());
      done.run(TajoWorker.FALSE_PROTO);
    }
  }

  @Override
  public void killTaskAttempt(RpcController controller, TajoIdProtos.TaskAttemptIdProto request,
                              RpcCallback<PrimitiveProtos.BoolProto> done) {
    Task task = workerContext.getTaskRunnerManager().getTaskByTaskAttemptId(new TaskAttemptId(request));
    if(task != null) task.kill();

    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void cleanup(RpcController controller, TajoIdProtos.QueryIdProto request,
                      RpcCallback<PrimitiveProtos.BoolProto> done) {
    workerContext.cleanup(new QueryId(request).toString());
    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void cleanupExecutionBlocks(RpcController controller,
                                     TajoWorkerProtocol.ExecutionBlockListProto ebIds,
                                     RpcCallback<PrimitiveProtos.BoolProto> done) {
    for (TajoIdProtos.ExecutionBlockIdProto executionBlockIdProto : ebIds.getExecutionBlockIdList()) {
      String inputDir = ExecutionBlockContext.getBaseInputDir(new ExecutionBlockId(executionBlockIdProto)).toString();
      workerContext.cleanup(inputDir);
      String outputDir = ExecutionBlockContext.getBaseOutputDir(new ExecutionBlockId(executionBlockIdProto)).toString();
      workerContext.cleanup(outputDir);
    }
    done.run(TajoWorker.TRUE_PROTO);
  }
}
