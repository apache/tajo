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

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ha.HAServiceUtil;
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
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.querymaster.Stage;
import org.apache.tajo.querymaster.StageState;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.util.ApplicationIdUtils;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService executorService;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    executorService = Executors.newFixedThreadPool(
      queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
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
    tajoConf = (TajoConf)conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());

    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());

    super.init(conf);
  }

  @Override
  public synchronized void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }

    executorService.shutdownNow();

    Map<TajoContainerId, ContainerProxy> containers = queryTaskContext.getResourceAllocator()
      .getContainers();
    List<ContainerProxy> list = new ArrayList<ContainerProxy>(containers.values());
    for(ContainerProxy eachProxy: list) {
      try {
        eachProxy.stopContainer();
      } catch (Exception e) {
        LOG.warn(e.getMessage());
      }
    }

    workerInfoMap.clear();
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
      executorService.submit(new LaunchRunner(eachContainer.getId(), containerProxy));
    }
  }

  public void stopExecutionBlock(final ExecutionBlockId executionBlockId,
                                 Collection<TajoContainer> containers) {
    Set<NodeId> workers = Sets.newHashSet();
    for (TajoContainer container : containers){
      workers.add(container.getNodeId());
    }

    for (final NodeId worker : workers) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          stopExecutionBlock(executionBlockId, worker);
        }
      });
    }
  }

  private void stopExecutionBlock(ExecutionBlockId executionBlockId, NodeId worker) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      InetSocketAddress addr = new InetSocketAddress(worker.getHost(), worker.getPort());
      tajoWorkerRpc = RpcConnectionPool.getPool().getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      tajoWorkerRpcClient.stopExecutionBlock(null, executionBlockId.getProto(), NullCallback.get());
    } catch (Throwable e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool().releaseConnection(tajoWorkerRpc);
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

  private void stopContainers(Collection<TajoContainer> containers) {
    for (TajoContainer container : containers) {
      final ContainerProxy proxy = queryTaskContext.getResourceAllocator().getContainer(container.getId());
      executorService.submit(new StopContainerRunner(container.getId(), proxy));
    }
  }

  private static class StopContainerRunner implements Runnable {
    private final ContainerProxy proxy;
    private final TajoContainerId id;
    public StopContainerRunner(TajoContainerId id, ContainerProxy proxy) {
      this.id = id;
      this.proxy = proxy;
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerProxy stopped:" + id + "," + proxy.getId());
      }
      proxy.stopContainer();
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {
    @Override
    public void handle(ContainerAllocationEvent event) {
      executorService.submit(new TajoWorkerAllocationThread(event));
    }
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

      //TODO consider task's resource usage pattern
      int requiredMemoryMB = tajoConf.getIntVar(TajoConf.ConfVars.TASK_DEFAULT_MEMORY);
      float requiredDiskSlots = tajoConf.getFloatVar(TajoConf.ConfVars.TASK_DEFAULT_DISK);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setMinMemoryMBPerContainer(requiredMemoryMB)
          .setMaxMemoryMBPerContainer(requiredMemoryMB)
          .setNumContainers(event.getRequiredNum())
          .setResourceRequestPriority(!event.isLeafQuery() ?
              ResourceRequestPriority.MEMORY : ResourceRequestPriority.DISK)
          .setMinDiskSlotPerContainer(requiredDiskSlots)
          .setMaxDiskSlotPerContainer(requiredDiskSlots)
          .setQueryId(event.getExecutionBlockId().getQueryId().getProto())
          .build();

      RpcConnectionPool connPool = RpcConnectionPool.getPool();
      NettyClientBase tmClient = null;
      try {

        // In TajoMaster HA mode, if backup master be active status,
        // worker may fail to connect existing active master. Thus,
        // if worker can't connect the master, worker should try to connect another master and
        // update master address in worker context.
        if (tajoConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
          try {
            tmClient = connPool.getConnection(
              queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
              QueryCoordinatorProtocol.class, true);
          } catch (Exception e) {
            queryTaskContext.getQueryMasterContext().getWorkerContext().
              setWorkerResourceTrackerAddr(HAServiceUtil.getResourceTrackerAddress(tajoConf));
            queryTaskContext.getQueryMasterContext().getWorkerContext().
              setTajoMasterAddress(HAServiceUtil.getMasterUmbilicalAddress(tajoConf));
            tmClient = connPool.getConnection(
              queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
              QueryCoordinatorProtocol.class, true);
          }
        } else {
          tmClient = connPool.getConnection(
            queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
            QueryCoordinatorProtocol.class, true);
        }

        QueryCoordinatorProtocolService masterClientService = tmClient.getStub();
        masterClientService.allocateWorkerResources(null, request, callBack);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      } finally {
        connPool.releaseConnection(tmClient);
      }

      WorkerResourceAllocationResponse response = null;
      while(!stopped.get()) {
        try {
          response = callBack.get(3, TimeUnit.SECONDS);
          break;
        } catch (InterruptedException e) {
          if(stopped.get()) {
            return;
          }
        } catch (TimeoutException e) {
          LOG.info("No available worker resource for " + event.getExecutionBlockId());
          continue;
        }
      }
      int numAllocatedContainers = 0;

      if(response != null) {
        List<WorkerAllocatedResource> allocatedResources = response.getWorkerAllocatedResourceList();
        ExecutionBlockId executionBlockId = event.getExecutionBlockId();

        List<TajoContainer> containers = new ArrayList<TajoContainer>();
        for(WorkerAllocatedResource eachAllocatedResource: allocatedResources) {
          TajoWorkerContainer container = new TajoWorkerContainer();
          NodeId nodeId = NodeId.newInstance(eachAllocatedResource.getConnectionInfo().getHost(),
            eachAllocatedResource.getConnectionInfo().getPeerRpcPort());

          TajoWorkerContainerId containerId = new TajoWorkerContainerId();

          containerId.setApplicationAttemptId(
            ApplicationIdUtils.createApplicationAttemptId(executionBlockId.getQueryId(),
              eachAllocatedResource.getContainerId().getAppAttemptId().getAttemptId()));
          containerId.setId(eachAllocatedResource.getContainerId().getId());

          container.setId(containerId);
          container.setNodeId(nodeId);


          WorkerResource workerResource = new WorkerResource();
          workerResource.setMemoryMB(eachAllocatedResource.getAllocatedMemoryMB());
          workerResource.setDiskSlots(eachAllocatedResource.getAllocatedDiskSlots());

          Worker worker = new Worker(null, workerResource,
            new WorkerConnectionInfo(eachAllocatedResource.getConnectionInfo()));
          container.setWorkerResource(worker);
          addWorkerConnectionInfo(worker.getConnectionInfo());
          containers.add(container);
        }

        StageState state = queryTaskContext.getStage(executionBlockId).getSynchronizedState();
        if (!Stage.isRunningState(state)) {
          try {
            List<TajoContainerId> containerIds = new ArrayList<TajoContainerId>();
            for(TajoContainer eachContainer: containers) {
              containerIds.add(eachContainer.getId());
            }
            TajoContainerProxy.releaseWorkerResource(queryTaskContext, executionBlockId, containerIds);
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
          return;
        }

        if (allocatedResources.size() > 0) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("StageContainerAllocationEvent fire:" + executionBlockId);
          }
          queryTaskContext.getEventHandler().handle(new StageContainerAllocationEvent(executionBlockId, containers));
        }
        numAllocatedContainers += allocatedResources.size();

      }
      if(event.getRequiredNum() > numAllocatedContainers) {
        ContainerAllocationEvent shortRequestEvent = new ContainerAllocationEvent(
          event.getType(), event.getExecutionBlockId(), event.getPriority(),
          event.getResource(),
          event.getRequiredNum() - numAllocatedContainers,
          event.isLeafQuery(), event.getProgress()
        );
        queryTaskContext.getEventHandler().handle(shortRequestEvent);

      }
      LOG.info("Stop TajoWorkerAllocationThread");
    }
  }
}
