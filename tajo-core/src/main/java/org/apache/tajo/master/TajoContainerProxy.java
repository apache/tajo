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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ContainerProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.container.TajoContainer;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.event.TaskFatalErrorEvent;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.worker.TajoWorker;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TajoContainerProxy extends ContainerProxy {
  private final QueryContext queryContext;
  private final TajoWorker.WorkerContext workerContext;
  private final String planJson;

  public TajoContainerProxy(QueryMasterTask.QueryMasterTaskContext context,
                            Configuration conf, TajoContainer container,
                            QueryContext queryContext, ExecutionBlockId executionBlockId, String planJson) {
    super(context, conf, executionBlockId, container);
    this.queryContext = queryContext;
    this.workerContext = context.getQueryMasterContext().getWorkerContext();
    this.planJson = planJson;
  }

  @Override
  public synchronized void launch(ContainerLaunchContext containerLaunchContext) {
    context.getResourceAllocator().addContainer(containerId, this);

    this.hostName = container.getNodeId().getHost();
    this.port = ((TajoWorkerContainer)container).getWorkerResource().getConnectionInfo().getPullServerPort();
    this.state = ContainerState.RUNNING;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Launch Container:" + executionBlockId + "," + containerId.getId() + "," +
          container.getId() + "," + container.getNodeId() + ", pullServer=" + port);
    }

    assignExecutionBlock(executionBlockId, container);
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  public void killTaskAttempt(TaskAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = RpcClientManager.getInstance().getClient(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), NullCallback.get());
    } catch (Throwable e) {
      /* Worker RPC failure */
      context.getEventHandler().handle(new TaskFatalErrorEvent(taskAttemptId, e.getMessage()));
    }
  }

  private void assignExecutionBlock(ExecutionBlockId executionBlockId, TajoContainer container) {
    NettyClientBase tajoWorkerRpc;
    try {

      InetSocketAddress addr = new InetSocketAddress(container.getNodeId().getHost(), container.getNodeId().getPort());
      tajoWorkerRpc = RpcClientManager.getInstance().getClient(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      PlanProto.ShuffleType shuffleType =
          context.getQuery().getStage(executionBlockId).getDataChannel().getShuffleType();

      TajoWorkerProtocol.RunExecutionBlockRequestProto request =
          TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder()
              .setExecutionBlockId(executionBlockId.getProto())
              .setQueryMaster(context.getQueryMasterContext().getWorkerContext().getConnectionInfo().getProto())
              .setNodeId(container.getNodeId().toString())
              .setContainerId(container.getId().toString())
              .setQueryOutputPath(context.getStagingDir().toString())
              .setQueryContext(queryContext.getProto())
              .setPlanJson(planJson)
              .setShuffleType(shuffleType)
              .build();

      tajoWorkerRpcClient.startExecutionBlock(null, request, NullCallback.get());
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void stopContainer() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Release TajoWorker Resource: " + executionBlockId + "," + containerId + ", state:" + this.state);
    }
    if(isCompletelyDone()) {
      LOG.info("Container already stopped:" + containerId);
      return;
    }
    if(this.state == ContainerState.PREP) {
      this.state = ContainerState.KILLED_BEFORE_LAUNCH;
    } else {
      try {
        releaseWorkerResource(context, executionBlockId, Arrays.asList(containerId));
        context.getResourceAllocator().removeContainer(containerId);
      } catch (Throwable t) {
        // ignore the cleanup failure
        String message = "cleanup failed for container "
            + this.containerId + " : "
            + StringUtils.stringifyException(t);
        LOG.warn(message);
      } finally {
        this.state = ContainerState.DONE;
      }
    }
  }

  public static void releaseWorkerResource(QueryMasterTask.QueryMasterTaskContext context,
                                           ExecutionBlockId executionBlockId,
                                           List<TajoContainerId> containerIds) throws Exception {
    List<ContainerProtocol.TajoContainerIdProto> containerIdProtos =
        new ArrayList<ContainerProtocol.TajoContainerIdProto>();

    for(TajoContainerId eachContainerId: containerIds) {
      containerIdProtos.add(TajoWorkerContainerId.getContainerIdProto(eachContainerId));
    }

    RpcClientManager manager = RpcClientManager.getInstance();
    NettyClientBase tmClient = null;

    ServiceTracker serviceTracker = context.getQueryMasterContext().getWorkerContext().getServiceTracker();
    tmClient = manager.getClient(serviceTracker.getUmbilicalAddress(), QueryCoordinatorProtocol.class, true);

    QueryCoordinatorProtocol.QueryCoordinatorProtocolService masterClientService = tmClient.getStub();
    masterClientService.releaseWorkerResource(null,
        QueryCoordinatorProtocol.WorkerResourceReleaseRequest.newBuilder()
            .setExecutionBlockId(executionBlockId.getProto())
            .addAllContainerIds(containerIdProtos)
            .build(),
        NullCallback.get());

  }
}
