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

package org.apache.tajo.master;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.QueryCoordinatorProtocolService;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.rm.NodeStatus;
import org.apache.tajo.master.scheduler.event.ResourceReserveSchedulerEvent;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.Collection;

import static org.apache.tajo.ResourceProtos.*;
import static org.apache.tajo.util.ProtoUtil.TRUE;

public class QueryCoordinatorService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(QueryCoordinatorService.class);

  private final TajoMaster.MasterContext context;
  private final TajoConf conf;
  private final ProtocolServiceHandler masterHandler;
  private AsyncRpcServer server;
  private InetSocketAddress bindAddress;

  public QueryCoordinatorService(TajoMaster.MasterContext context) {
    super(QueryCoordinatorService.class.getName());
    this.context = context;
    this.conf = context.getConf();
    this.masterHandler = new ProtocolServiceHandler();
  }

  @Override
  public void serviceStart() throws Exception {
    InetSocketAddress initIsa = conf.getSocketAddrVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    int workerNum = conf.getIntVar(TajoConf.ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM);

    server = new AsyncRpcServer(QueryCoordinatorProtocol.class, masterHandler, initIsa, workerNum);
    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS,
        NetUtils.normalizeInetSocketAddress(bindAddress));
    LOG.info("Instantiated TajoMasterService at " + this.bindAddress);
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    if(server != null) {
      server.shutdown();
      server = null;
    }
    super.serviceStop();
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  /**
   * Actual protocol service handler
   */
  private class ProtocolServiceHandler implements QueryCoordinatorProtocolService.Interface {

    @Override
    public void heartbeat(
        RpcController controller,
        TajoHeartbeatRequest request, RpcCallback<TajoHeartbeatResponse> done) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Received QueryHeartbeat:" + new WorkerConnectionInfo(request.getConnectionInfo()));
      }

      TajoHeartbeatResponse.ResponseCommand command;

      QueryManager queryManager = context.getQueryJobManager();
      command = queryManager.queryHeartbeat(request);

      TajoHeartbeatResponse.Builder builder = TajoHeartbeatResponse.newBuilder();
      builder.setHeartbeatResult(TRUE);
      if(command != null) {
        builder.setResponseCommand(command);
      }

      done.run(builder.build());
    }

    /**
     * Reserve a node resources to TajoMaster
     */
    @Override
    public void reserveNodeResources(RpcController controller, NodeResourceRequest request,
                                     RpcCallback<NodeResourceResponse> done) {
      Dispatcher dispatcher = context.getResourceManager().getRMContext().getDispatcher();
      dispatcher.getEventHandler().handle(new ResourceReserveSchedulerEvent(request, done));
    }

    /**
     * Get all worker connection information
     */
    @Override
    public void getAllWorkers(RpcController controller, PrimitiveProtos.NullProto request,
                              RpcCallback<WorkerConnectionsResponse> done) {

      WorkerConnectionsResponse.Builder builder = WorkerConnectionsResponse.newBuilder();
      Collection<NodeStatus> nodeStatuses = context.getResourceManager().getRMContext().getNodes().values();

      for(NodeStatus nodeStatus : nodeStatuses) {
        builder.addWorker(nodeStatus.getConnectionInfo().getProto());
      }
      done.run(builder.build());
    }
  }
}
