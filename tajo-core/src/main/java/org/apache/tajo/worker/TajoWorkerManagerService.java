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
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;

public class TajoWorkerManagerService extends CompositeService
    implements TajoWorkerProtocol.TajoWorkerProtocolService.Interface {
  private static final Log LOG = LogFactory.getLog(TajoWorkerManagerService.class.getName());

  private AsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
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
      this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

      this.port = bindAddr.getPort();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info("TajoWorkerManagerService is bind to " + addr);
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

  public String getHostAndPort() {
    return bindAddr.getHostName() + ":" + bindAddr.getPort();
  }

  @Override
  public void ping(RpcController controller,
                   TajoIdProtos.QueryUnitAttemptIdProto attemptId,
                   RpcCallback<PrimitiveProtos.BoolProto> done) {
    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void executeExecutionBlock(RpcController controller,
                                    TajoWorkerProtocol.RunExecutionBlockRequestProto request,
                                    RpcCallback<PrimitiveProtos.BoolProto> done) {
    workerContext.getWorkerSystemMetrics().counter("query", "executedExecutionBlocksNum").inc();
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

  @Override
  public void killTaskAttempt(RpcController controller, TajoIdProtos.QueryUnitAttemptIdProto request,
                              RpcCallback<PrimitiveProtos.BoolProto> done) {
    Task task = workerContext.getTaskRunnerManager().getTaskByQueryUnitAttemptId(new QueryUnitAttemptId(request));
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
  public void cleanupExecutionBlocks(RpcController controller, TajoWorkerProtocol.ExecutionBlockListProto request,
                                     RpcCallback<PrimitiveProtos.BoolProto> done) {
    for (TajoIdProtos.ExecutionBlockIdProto executionBlockIdProto : request.getExecutionBlockIdList()) {
      String inputDir = TaskRunner.getBaseInputDir(new ExecutionBlockId(executionBlockIdProto)).toString();
      workerContext.cleanup(inputDir);
      String outputDir = TaskRunner.getBaseOutputDir(new ExecutionBlockId(executionBlockIdProto)).toString();
      workerContext.cleanup(outputDir);
    }
    done.run(TajoWorker.TRUE_PROTO);
  }
}
