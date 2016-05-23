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
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.QueryId;
import org.apache.tajo.ResourceProtos.BatchAllocationRequest;
import org.apache.tajo.ResourceProtos.BatchAllocationResponse;
import org.apache.tajo.ResourceProtos.StopExecutionBlockRequest;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.ExecutionBlockStopEvent;
import org.apache.tajo.worker.event.NodeResourceAllocateEvent;
import org.apache.tajo.worker.event.QueryStopEvent;

import java.net.InetSocketAddress;

public class TajoWorkerManagerService extends CompositeService
    implements TajoWorkerProtocol.TajoWorkerProtocolService.Interface {
  private static final Log LOG = LogFactory.getLog(TajoWorkerManagerService.class.getName());

  private AsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;

  private TajoWorker.WorkerContext workerContext;

  public TajoWorkerManagerService(TajoWorker.WorkerContext workerContext) {
    super(TajoWorkerManagerService.class.getName());
    this.workerContext = workerContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    TajoConf tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    try {
      // Setup RPC server
      InetSocketAddress initIsa = tajoConf.getSocketAddrVar(TajoConf.ConfVars.WORKER_PEER_RPC_ADDRESS);

      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      int workerNum = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_RPC_SERVER_WORKER_THREAD_NUM);
      this.rpcServer = new AsyncRpcServer(TajoWorkerProtocol.class, this, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Get the master address
    LOG.info("TajoWorkerManagerService is bind to " + bindAddr);
    tajoConf.setVar(TajoConf.ConfVars.WORKER_PEER_RPC_ADDRESS, NetUtils.getHostPortString(bindAddr));
    super.serviceInit(tajoConf);
  }

  @Override
  public void serviceStop() throws Exception {
    if(rpcServer != null) {
      rpcServer.shutdown();
    }
    LOG.info("TajoWorkerManagerService stopped");
    super.serviceStop();
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
  public void allocateTasks(RpcController controller,
                            BatchAllocationRequest request,
                            RpcCallback<BatchAllocationResponse> done) {
    workerContext.getNodeResourceManager().getDispatcher().
        getEventHandler().handle(new NodeResourceAllocateEvent(request, done));
  }

  @Override
  public void stopExecutionBlock(RpcController controller,
                                 StopExecutionBlockRequest requestProto,
                                 RpcCallback<PrimitiveProtos.BoolProto> done) {
    try {

      workerContext.getTaskManager().getDispatcher().getEventHandler().handle(
          new ExecutionBlockStopEvent(requestProto.getExecutionBlockId(), requestProto.getCleanupList()));

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
    //TODO change to async ?
    Task task = workerContext.getTaskManager().getTaskByTaskAttemptId(new TaskAttemptId(request));
    if(task != null) task.kill();

    done.run(TajoWorker.TRUE_PROTO);
  }

  @Override
  public void stopQuery(RpcController controller, TajoIdProtos.QueryIdProto request,
                      RpcCallback<PrimitiveProtos.BoolProto> done) {

    workerContext.getTaskManager().getDispatcher().getEventHandler().handle(new QueryStopEvent(new QueryId(request)));
    done.run(TajoWorker.TRUE_PROTO);
  }
}
