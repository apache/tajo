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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.NullCallback;

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

    context.getEventHandler().handle(new QueryEvent(context.getQueryId(), QueryEventType.INIT_COMPLETED));
  }

  private void assignExecutionBlock(ExecutionBlockId executionBlockId, Container container) {
    AsyncRpcClient tajoWorkerRpc = null;
    try {
      InetSocketAddress myAddr= context.getQueryMasterContext().getWorkerContext()
          .getQueryMasterManagerService().getBindAddr();

      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = new AsyncRpcClient(TajoWorkerProtocol.class, addr);
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
      //TODO retry
      LOG.error(e.getMessage(), e);
    } finally {
      if(tajoWorkerRpc != null) {
        (new AyncRpcClose(tajoWorkerRpc)).start();
      }
    }
  }

  class AyncRpcClose extends Thread {
    AsyncRpcClient client;
    public AyncRpcClose(AsyncRpcClient client) {
      this.client = client;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      client.close();
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
        releaseWorkerResource(context, executionBlockId, ((TajoWorkerContainer)container).getWorkerResource());
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
                                           WorkerResource workerResource) throws Exception {
    List<WorkerResource> workerResources = new ArrayList<WorkerResource>();
    workerResources.add(workerResource);

    releaseWorkerResource(context, executionBlockId, workerResources);
  }

  public static void releaseWorkerResource(QueryMasterTask.QueryMasterTaskContext context,
                                           ExecutionBlockId executionBlockId,
                                           List<WorkerResource> workerResources) throws Exception {
    List<TajoMasterProtocol.WorkerResourceProto> workerResourceProtos =
        new ArrayList<TajoMasterProtocol.WorkerResourceProto>();

    for(WorkerResource eahWorkerResource: workerResources) {
      workerResourceProtos.add(TajoMasterProtocol.WorkerResourceProto.newBuilder()
          .setHost(eahWorkerResource.getAllocatedHost())
          .setQueryMasterPort(eahWorkerResource.getQueryMasterPort())
          .setPeerRpcPort(eahWorkerResource.getPeerRpcPort())
          .setExecutionBlockId(executionBlockId.getProto())
          .setMemoryMBSlots(eahWorkerResource.getMemoryMBSlots())
          .setDiskSlots(eahWorkerResource.getDiskSlots())
          .build()
      );
    }
    context.getQueryMasterContext().getWorkerContext().getTajoMasterRpcClient()
        .releaseWorkerResource(null,
            TajoMasterProtocol.WorkerResourceReleaseRequest.newBuilder()
                .addAllWorkerResources(workerResourceProtos)
                .build(),
            NullCallback.get());
  }
}
