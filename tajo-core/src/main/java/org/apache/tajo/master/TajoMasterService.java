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
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

public class TajoMasterService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(TajoMasterService.class);

  private final TajoMaster.MasterContext context;
  private final TajoConf conf;
  private final TajoMasterServiceHandler masterHandler;
  private AsyncRpcServer server;
  private InetSocketAddress bindAddress;

  private final BoolProto BOOL_TRUE = BoolProto.newBuilder().setValue(true).build();
  private final BoolProto BOOL_FALSE = BoolProto.newBuilder().setValue(false).build();

  public TajoMasterService(TajoMaster.MasterContext context) {
    super(TajoMasterService.class.getName());
    this.context = context;
    this.conf = context.getConf();
    this.masterHandler = new TajoMasterServiceHandler();
  }

  @Override
  public void start() {
    String confMasterServiceAddr = conf.getVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);
    int workerNum = conf.getIntVar(TajoConf.ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM);
    try {
      server = new AsyncRpcServer(TajoMasterProtocol.class, masterHandler, initIsa, workerNum);
    } catch (Exception e) {
      LOG.error(e);
    }
    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    this.conf.setVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS,
        NetUtils.normalizeInetSocketAddress(bindAddress));
    LOG.info("Instantiated TajoMasterService at " + this.bindAddress);
    super.start();
  }

  @Override
  public void stop() {
    if(server != null) {
      server.shutdown();
      server = null;
    }
    super.stop();
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  public class TajoMasterServiceHandler
      implements TajoMasterProtocol.TajoMasterProtocolService.Interface {
    @Override
    public void heartbeat(
        RpcController controller,
        TajoMasterProtocol.TajoHeartbeat request, RpcCallback<TajoMasterProtocol.TajoHeartbeatResponse> done) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Received QueryHeartbeat:" + new WorkerConnectionInfo(request.getConnectionInfo()));
      }

      TajoMasterProtocol.TajoHeartbeatResponse.ResponseCommand command = null;

      QueryJobManager queryJobManager = context.getQueryJobManager();
      command = queryJobManager.queryHeartbeat(request);

      TajoMasterProtocol.TajoHeartbeatResponse.Builder builder = TajoMasterProtocol.TajoHeartbeatResponse.newBuilder();
      builder.setHeartbeatResult(BOOL_TRUE);
      if(command != null) {
        builder.setResponseCommand(command);
      }

      builder.setClusterResourceSummary(context.getResourceManager().getClusterResourceSummary());
      done.run(builder.build());
    }

    @Override
    public void allocateWorkerResources(
        RpcController controller,
        TajoMasterProtocol.WorkerResourceAllocationRequest request,
        RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> done) {
      context.getResourceManager().allocateWorkerResources(request, done);
    }

    @Override
    public void releaseWorkerResource(RpcController controller,
                                           TajoMasterProtocol.WorkerResourceReleaseRequest request,
                                           RpcCallback<PrimitiveProtos.BoolProto> done) {
      List<YarnProtos.ContainerIdProto> containerIds = request.getContainerIdsList();

      for(YarnProtos.ContainerIdProto eachContainer: containerIds) {
        context.getResourceManager().releaseWorkerResource(eachContainer);
      }
      done.run(BOOL_TRUE);
    }

    @Override
    public void stopQueryMaster(RpcController controller, TajoIdProtos.QueryIdProto request,
                                RpcCallback<BoolProto> done) {
      context.getQueryJobManager().stopQuery(new QueryId(request));
      done.run(BOOL_TRUE);
    }

    @Override
    public void getAllWorkerResource(RpcController controller, PrimitiveProtos.NullProto request,
                                     RpcCallback<TajoMasterProtocol.WorkerResourcesRequest> done) {

      TajoMasterProtocol.WorkerResourcesRequest.Builder builder =
          TajoMasterProtocol.WorkerResourcesRequest.newBuilder();
      Collection<Worker> workers = context.getResourceManager().getWorkers().values();

      for(Worker worker: workers) {
        WorkerResource resource = worker.getResource();

        TajoMasterProtocol.WorkerResourceProto.Builder workerResource =
            TajoMasterProtocol.WorkerResourceProto.newBuilder();

        workerResource.setConnectionInfo(worker.getConnectionInfo().getProto());
        workerResource.setMemoryMB(resource.getMemoryMB());
        workerResource.setDiskSlots(resource.getDiskSlots());

        builder.addWorkerResources(workerResource);
      }
      done.run(builder.build());
    }
  }
}
