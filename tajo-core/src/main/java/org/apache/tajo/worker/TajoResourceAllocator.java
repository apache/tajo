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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.utils.ThreadUtil;
import org.apache.tajo.ipc.ContainerProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.*;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.*;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.container.TajoContainer;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.StageContainerAllocationEvent;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.querymaster.Stage;
import org.apache.tajo.querymaster.StageState;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.ApplicationIdUtils;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService allocationExecutor;
  private final Deallocator deallocator;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    allocationExecutor = Executors.newFixedThreadPool(
      queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
    deallocator = new Deallocator();
  }

  @Override
  public TajoContainerId makeContainerId(ContainerProtocol.TajoContainerIdProto containerIdProto) {
    TajoWorkerContainerId containerId = new TajoWorkerContainerId();
    ApplicationAttemptId appAttemptId = new ApplicationAttemptIdPBImpl(containerIdProto.getAppAttemptId());
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(containerIdProto.getId());
    return containerId;
  }

  @Override
  public void allocateTaskWorker() {
  }

  @Override
  public int calculateNumRequestContainers(TajoWorker.WorkerContext workerContext,
                                           int numTasks,
                                           int memoryMBPerTask) {
    //TODO consider disk slot

    ClusterResourceSummary clusterResource = workerContext.getClusterResource();
    int clusterSlots = clusterResource == null ? 0 : clusterResource.getTotalMemoryMB() / memoryMBPerTask;
    clusterSlots =  Math.max(1, clusterSlots - 1); // reserve query master slot
    LOG.info("CalculateNumberRequestContainer - Number of Tasks=" + numTasks +
        ", Number of Cluster Slots=" + clusterSlots);
    return  Math.min(numTasks, clusterSlots);
  }

  @Override
  public void init(Configuration conf) {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("conf should be a TajoConf type.");
    }
    tajoConf = (TajoConf)conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());

    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());

    deallocator.start();

    super.init(conf);
  }

  @Override
  public synchronized void stop() {
    if (stopped.compareAndSet(false, true)) {
      return;
    }

    allocationExecutor.shutdownNow();
    deallocator.shutdown();

    Map<TajoContainerId, ContainerProxy> containers = queryTaskContext.getResourceAllocator()
      .getContainers();
    List<ContainerProxy> list = new ArrayList<ContainerProxy>(containers.values());
    for(ContainerProxy eachProxy: list) {
      try {
        eachProxy.stopContainer();
      } catch (Throwable e) {
        LOG.warn(e.getMessage(), e);
      }
    }

    super.stop();
  }

  @Override
  public void start() {
    super.start();
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        if (!(event instanceof LaunchTaskRunnersEvent)) {
          throw new IllegalArgumentException("event should be a LaunchTaskRunnersEvent type.");
        }
        LaunchTaskRunnersEvent launchEvent = (LaunchTaskRunnersEvent) event;
        launchTaskRunners(launchEvent);
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        stopContainers(event.getContainers());
        stopExecutionBlock(event.getExecutionBlockId(), event.getContainers());
      }
    }
  }

  private void launchTaskRunners(LaunchTaskRunnersEvent event) {
    // Query in standby mode doesn't need launch Worker.
    // But, Assign ExecutionBlock to assigned tajo worker
    for(TajoContainer eachContainer: event.getContainers()) {
      TajoContainerProxy containerProxy = new TajoContainerProxy(queryTaskContext, tajoConf,
        eachContainer, event.getQueryContext(), event.getExecutionBlockId(), event.getPlanJson());
      allocationExecutor.submit(new LaunchRunner(eachContainer.getId(), containerProxy));
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    releaseResources(removeFreeResources());
    // todo do something
  }

  public void stopExecutionBlock(final ExecutionBlockId executionBlockId,
                                 Collection<TajoWorkerContainer> containers) {
    List<AllocatedResource> resources = Lists.newArrayList();
    for (TajoWorkerContainer container : containers) {
      resources.add(container.getResource());
    }
    releaseResources(resources);

    for (final NodeId worker : getUniqueNodes(containers)) {
      allocationExecutor.submit(new Runnable() {
        @Override
        public void run() {
          stopExecutionBlock(executionBlockId, worker);
        }
      });
    }
  }

  private Set<NodeId> getUniqueNodes(Collection<TajoWorkerContainer> containers) {
    Set<NodeId> nodes = Sets.newHashSet();
    for (TajoWorkerContainer container : containers) {
      nodes.add(container.getNodeId());
    }
    return nodes;
  }

  private void stopExecutionBlock(ExecutionBlockId executionBlockId, NodeId worker) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress addr = new InetSocketAddress(worker.getHost(), worker.getPort());
      tajoWorkerRpc = RpcClientManager.getInstance().getClient(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      tajoWorkerRpcClient.stopExecutionBlock(null, executionBlockId.getProto(),
          NullCallback.get(PrimitiveProtos.BoolProto.class));
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  protected static class LaunchRunner implements Runnable {
    private final ContainerProxy proxy;
    private final TajoContainerId id;
    public LaunchRunner(TajoContainerId id, ContainerProxy proxy) {
      this.proxy = proxy;
      this.id = id;
    }
    @Override
    public void run() {
      proxy.launch(null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerProxy started:" + id);
      }
    }
  }

  private void stopContainers(Collection<TajoWorkerContainer> containers) {
    deallocator.submit(Iterables.transform(containers, new Function<TajoContainer, TajoContainerId>() {
      public TajoContainerId apply(TajoContainer input) { return input.getId(); }
    }));
  }

  private static final TajoContainerId FIN = new TajoWorkerContainerId();

  private class Deallocator extends Thread {

    private final BlockingDeque<TajoContainerId> queue = new LinkedBlockingDeque<TajoContainerId>();

    public Deallocator() {
      setName("Deallocator");
      setDaemon(true);
    }

    private void submit(Iterable<TajoContainerId> container) {
      queue.addAll(Lists.newArrayList(container));
    }

    private void shutdown() {
      queue.add(FIN);
    }

    @Override
    public void run() {
      final AbstractResourceAllocator allocator = queryTaskContext.getResourceAllocator();
      while (!stopped.get() || !queue.isEmpty()) {
        TajoContainerId containerId;
        try {
          containerId = queue.take();
        } catch (InterruptedException e) {
          continue;
        }
        if (containerId == FIN) {
          break;
        }
        ContainerProxy proxy = allocator.getContainer(containerId);
        if (proxy == null) {
          continue;
        }
        try {
          LOG.info("Stopping ContainerProxy: " + proxy.getContainerId() + "," + proxy.getBlockId());
          proxy.stopContainer();
        } catch (Exception e) {
          LOG.warn("Failed to stop container " + proxy.getContainerId() + "," + proxy.getBlockId(), e);
        }
      }
      LOG.info("Deallocator exiting");
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {
    @Override
    public void handle(ContainerAllocationEvent event) {
      List<AllocatedResource> allocated = allocatedResources(event.getCapability(), event.getRequiredNum());
      List<TajoWorkerContainer> containers = publish(event.getExecutionBlockId(), allocated);
      if (allocated.size() < event.getRequiredNum()) {
        ContainerAllocationEvent shortage = event.shortOf(event.getRequiredNum() - allocated.size());
        allocationExecutor.submit(new TajoWorkerAllocationThread(shortage));
      }
      // todo consider parallelism
      releaseResources(removeFreeResources());
    }
  }

  private void releaseResources(List<AllocatedResource> resources) {
    Iterable<Integer> resourceIds= Iterables.transform(resources,
        new Function<AllocatedResource, Integer>() {
      public Integer apply(AllocatedResource input) { return input.getResourceId(); }
    });

    RpcClientManager manager = RpcClientManager.getInstance();
    try {
      ServiceTracker serviceTracker = queryTaskContext.getQueryMasterContext().getWorkerContext().getServiceTracker();
      NettyClientBase tmClient = manager.getClient(serviceTracker.getUmbilicalAddress(), QueryCoordinatorProtocol.class, true);

      QueryCoordinatorProtocolService masterClientService = tmClient.getStub();
      masterClientService.releaseWorkerResource(null,
          WorkerResourceReleaseRequest.newBuilder().addAllResourceIds(resourceIds).build(),
          NullCallback.get());
      removeResources(resources);
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
      // todo: do something (delegate to master)
    }
  }

  private final AtomicInteger containerIdSeq = new AtomicInteger();

  List<TajoWorkerContainer> publish(ExecutionBlockId executionBlockId, List<AllocatedResource> resources) {
    StageState state = queryTaskContext.getStage(executionBlockId).getSynchronizedState();
    if (!Stage.isRunningState(state)) {
      releaseResources(resources);
      return null;
    }

    List<TajoWorkerContainer> containers = new ArrayList<TajoWorkerContainer>(resources.size());
    for (AllocatedResource resource : resources) {
      QueryId queryId = executionBlockId.getQueryId();
      TajoWorkerContainer container = new TajoWorkerContainer();
      TajoWorkerContainerId containerId = new TajoWorkerContainerId();
      containerId.setApplicationAttemptId(ApplicationIdUtils.createApplicationAttemptId(queryId));
      containerId.setId(containerIdSeq.incrementAndGet());
      container.setId(containerId);

      WorkerConnectionInfo connectionInfo = resource.getConnectionInfo();
      NodeId nodeId = NodeId.newInstance(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      container.setNodeId(nodeId);

      addAllocatedResource(resource);

      container.setResource(resource);
      containers.add(container);
    }

    if (!containers.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("StageContainerAllocationEvent fire:" + executionBlockId);
      }
      queryTaskContext.getEventHandler().handle(new StageContainerAllocationEvent(executionBlockId, containers));
    }

    return containers;
  }

  class TajoWorkerAllocationThread extends Thread {
    ContainerAllocationEvent event;
    TajoWorkerAllocationThread(ContainerAllocationEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Start TajoWorkerAllocationThread");
      CallFuture<WorkerResourceAllocationResponse> callBack =
        new CallFuture<WorkerResourceAllocationResponse>();

      Resources requirement = event.getCapability();
      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setMinMemoryMBPerContainer(requirement.getMemory())
          .setMaxMemoryMBPerContainer(requirement.getMemory())
          .setNumContainers(event.getRequiredNum())
          .setResourceRequestPriority(!event.isLeafQuery() ?
              ResourceRequestPriority.MEMORY : ResourceRequestPriority.DISK)
          .setMinDiskSlotPerContainer(requirement.getDiskSlots())
          .setMaxDiskSlotPerContainer(requirement.getDiskSlots())
          .setQueryId(event.getExecutionBlockId().getQueryId().getProto())
          .build();

      LOG.info("Requesting resource for " + event.getRequiredNum() + " containers, " + requirement);

      ExecutionBlockId executionBlockId = event.getExecutionBlockId();

      for (int i = 0; i < 3; i++) {
        WorkerResourceAllocationResponse response = allocate(request);
        if (response == null) {
          ThreadUtil.sleepWithoutInterrupt(1000 * (1 << i));
          continue;
        }

        List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
        for (WorkerAllocatedResource allocated : response.getWorkerAllocatedResourceList()) {

          AllocatedResource resource = new AllocatedResource(allocated.getResourceId(),
              1, allocated.getAllocatedMemoryMB(), allocated.getAllocatedDiskSlots(),
              new WorkerConnectionInfo(allocated.getConnectionInfo()));
          resource.acquire();

          resources.add(resource);
        }

        List<TajoWorkerContainer> containers = publish(event.getExecutionBlockId(), resources);
        if (containers != null && (event.getRequiredNum() > containers.size())) {
          ContainerAllocationEvent shortage = event.shortOf(event.getRequiredNum() - containers.size());
          queryTaskContext.getEventHandler().handle(shortage);
        }
      }
      LOG.info("Stop TajoWorkerAllocationThread");
    }

    private WorkerResourceAllocationResponse allocate(WorkerResourceAllocationRequest request) {

      RpcClientManager manager = RpcClientManager.getInstance();
      ServiceTracker serviceTracker = queryTaskContext.getQueryMasterContext().getWorkerContext().getServiceTracker();

      WorkerResourceAllocationResponse response = null;
      try {
        NettyClientBase tmClient = manager.getClient(serviceTracker.getUmbilicalAddress(),
            QueryCoordinatorProtocol.class, true);
        QueryCoordinatorProtocolService masterClientService = tmClient.getStub();

        CallFuture<WorkerResourceAllocationResponse> callBack =
            new CallFuture<WorkerResourceAllocationResponse>();

        masterClientService.allocateWorkerResources(callBack.getController(), request, callBack);

        while (!stopped.get() && response == null) {
          try {
            response = callBack.get(3, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            // loop again
          } catch (TimeoutException e) {
            LOG.info("No available worker resource for " + event.getExecutionBlockId());
          }
        }
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
      }
      return response;
    }
  }
}
