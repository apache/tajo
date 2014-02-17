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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TajoContainerProxy extends ContainerProxy {
  public TajoContainerProxy(QueryMasterTask.QueryMasterTaskContext context,
                            Configuration conf, Container container,
                            ExecutionBlockId executionBlockId) {
    super(context, conf, executionBlockId, container);
  }

  @Override
  public void launch(ContainerLaunchContext containerLaunchContext) {
    context.getResourceAllocator().addContainer(containerID, this);

    this.hostName = container.getNodeId().getHost();
    //this.port = context.getQueryMasterContext().getWorkerContext().getPullService().getPort();
    this.port = ((TajoWorkerContainer)container).getWorkerResource().getPullServerPort();
    this.state = ContainerState.RUNNING;

    LOG.info("Launch Container:" + executionBlockId + "," + containerID.getId() + "," +
        container.getId() + "," + container.getNodeId() + ", pullServer=" + port);

    assignExecutionBlock(executionBlockId, container);
  }

  private void assignExecutionBlock(ExecutionBlockId executionBlockId, Container container) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress myAddr= context.getQueryMasterContext().getWorkerContext()
          .getQueryMasterManagerService().getBindAddr();

      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(context.getConf()).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      TajoWorkerProtocol.RunExecutionBlockRequestProto request =
          TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder()
              .setExecutionBlockId(executionBlockId.toString())
              .setQueryMasterHost(myAddr.getHostName())
              .setQueryMasterPort(myAddr.getPort())
              .setNodeId(container.getNodeId().toString())
              .setContainerId(container.getId().toString())
              .setQueryOutputPath(context.getStagingDir().toString())
              .build();

      tajoWorkerRpcClient.executeExecutionBlock(null, request, NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(context.getConf()).releaseConnection(tajoWorkerRpc);
    }
  }

  @Override
  public synchronized void stopContainer() {
    LOG.info("Release TajoWorker Resource: " + executionBlockId + "," + containerID + ", state:" + this.state);
    if(isCompletelyDone()) {
      LOG.info("Container already stopped:" + containerID);
      return;
    }
    if(this.state == ContainerState.PREP) {
      this.state = ContainerState.KILLED_BEFORE_LAUNCH;
    } else {
      try {
        TajoWorkerContainer tajoWorkerContainer = ((TajoWorkerContainer)container);
        releaseWorkerResource(context, executionBlockId, tajoWorkerContainer.getId());
        context.getResourceAllocator().removeContainer(containerID);
        this.state = ContainerState.DONE;
      } catch (Throwable t) {
        // ignore the cleanup failure
        String message = "cleanup failed for container "
            + this.containerID + " : "
            + StringUtils.stringifyException(t);
        LOG.warn(message);
        this.state = ContainerState.DONE;
        return;
      }
    }
  }

  public static void releaseWorkerResource(QueryMasterTask.QueryMasterTaskContext context,
                                           ExecutionBlockId executionBlockId,
                                           ContainerId containerId) throws Exception {
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);

    releaseWorkerResource(context, executionBlockId, containerIds);
  }

  public static void releaseWorkerResource(QueryMasterTask.QueryMasterTaskContext context,
                                           ExecutionBlockId executionBlockId,
                                           List<ContainerId> containerIds) throws Exception {
    List<YarnProtos.ContainerIdProto> containerIdProtos =
        new ArrayList<YarnProtos.ContainerIdProto>();

    for(ContainerId eachContainerId: containerIds) {
      containerIdProtos.add(TajoWorkerContainerId.getContainerIdProto(eachContainerId));
    }

    RpcConnectionPool connPool = RpcConnectionPool.getPool(context.getConf());
    NettyClientBase tmClient = null;
    try {
        tmClient = connPool.getConnection(context.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
            TajoMasterProtocol.class, true);
        TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
        masterClientService.releaseWorkerResource(null,
          TajoMasterProtocol.WorkerResourceReleaseRequest.newBuilder()
              .setExecutionBlockId(executionBlockId.getProto())
              .addAllContainerIds(containerIdProtos)
              .build(),
          NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }
  }
}
