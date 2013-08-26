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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.ProtoAsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;

import java.net.InetSocketAddress;
import java.util.List;

public class TajoMasterService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(TajoMasterService.class);

  private final TajoMaster.MasterContext context;
  private final TajoConf conf;
  private final TajoMasterServiceHandler masterHandler;
  private ProtoAsyncRpcServer server;
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
    // TODO resolve hostname
    String confMasterServiceAddr = conf.getVar(TajoConf.ConfVars.TAJO_MASTER_SERVICE_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);
    try {
      server = new ProtoAsyncRpcServer(TajoMasterProtocol.class, masterHandler, initIsa);
    } catch (Exception e) {
      LOG.error(e);
    }
    server.start();
    bindAddress = server.getListenAddress();
    this.conf.setVar(TajoConf.ConfVars.TAJO_MASTER_SERVICE_ADDRESS,
        org.apache.tajo.util.NetUtils.getIpPortString(bindAddress));
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
        LOG.debug("Received QueryHeartbeat:" + request.getTajoWorkerHost() + ":" + request.getTajoWorkerPort());
      }

      TajoMasterProtocol.TajoHeartbeatResponse.ResponseCommand command = null;
      if(request.hasQueryId()) {
        QueryId queryId = new QueryId(request.getQueryId());

        //heartbeat from querymaster
        //LOG.info("Received QueryHeartbeat:" + queryId + "," + request);
        QueryJobManager queryJobManager = context.getQueryJobManager();
        command = queryJobManager.queryHeartbeat(request);
      } else {
        //heartbeat from TajoWorker
        context.getResourceManager().workerHeartbeat(request);
      }

      //ApplicationAttemptId attemptId = queryJobManager.getAppAttemptId();
      //String attemptIdStr = attemptId == null ? null : attemptId.toString();
      TajoMasterProtocol.TajoHeartbeatResponse.Builder builder = TajoMasterProtocol.TajoHeartbeatResponse.newBuilder();
      builder.setHeartbeatResult(BOOL_TRUE);
      if(command != null) {
        builder.setResponseCommand(command);
      }
      done.run(builder.build());
    }

    @Override
    public void allocateWorkerResources(
        RpcController controller,
        TajoMasterProtocol.WorkerResourceAllocationRequest request,
        RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> done) {
      context.getResourceManager().allocateWorkerResources(request, done);

//      List<String> workerHosts = new ArrayList<String>();
//      for(WorkerResource eachWorker: workerResources) {
//        workerHosts.add(eachWorker.getAllocatedHost() + ":" + eachWorker.getPorts()[0]);
//      }
//
//      done.run(TajoMasterProtocol.WorkerResourceAllocationResponse.newBuilder()
//          .setExecutionBlockId(request.getExecutionBlockId())
//          .addAllAllocatedWorks(workerHosts)
//          .build()
//      );
    }

    @Override
    public void releaseWorkerResource(RpcController controller,
                                           TajoMasterProtocol.WorkerResourceReleaseRequest request,
                                           RpcCallback<PrimitiveProtos.BoolProto> done) {
      List<TajoMasterProtocol.WorkerResourceProto> workerResources = request.getWorkerResourcesList();
      for(TajoMasterProtocol.WorkerResourceProto eachWorkerResource: workerResources) {
        WorkerResource workerResource = new WorkerResource();
        String[] tokens = eachWorkerResource.getWorkerHostAndPort().split(":");
        workerResource.setAllocatedHost(tokens[0]);
        workerResource.setPorts(new int[]{Integer.parseInt(tokens[1])});
        workerResource.setMemoryMBSlots(eachWorkerResource.getMemoryMBSlots());
        workerResource.setDiskSlots(eachWorkerResource.getDiskSlots());

        LOG.info("====> releaseWorkerResource:" + workerResource);
        context.getResourceManager().releaseWorkerResource(
            new QueryId(eachWorkerResource.getExecutionBlockId().getQueryId()),
            workerResource);
      }
      done.run(BOOL_TRUE);
    }

    @Override
    public void stopQueryMaster(RpcController controller, TajoIdProtos.QueryIdProto request,
                                RpcCallback<BoolProto> done) {
      context.getQueryJobManager().stopQuery(new QueryId(request));
      done.run(BOOL_TRUE);
    }
  }
}
