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

package org.apache.tajo.master.rm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ContainerProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.*;
import org.apache.tajo.master.QueryInProgress;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.rpc.CancelableRpcCallback;
import org.apache.tajo.rpc.RpcUtils;
import org.apache.tajo.util.ApplicationIdUtils;
import org.apache.tajo.util.BasicFuture;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * It manages all resources of tajo workers.
 */
public class TajoWorkerResourceManager extends CompositeService implements WorkerResourceManager {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TajoWorkerResourceManager.class);

  static AtomicInteger containerIdSeq = new AtomicInteger(0);

  private TajoMaster.MasterContext masterContext;

  private TajoRMContext rmContext;

  private String queryIdSeed;

  private WorkerResourceAllocationThread workerResourceAllocator;

  /**
   * Worker Liveliness monitor
   */
  private WorkerLivelinessMonitor workerLivelinessMonitor;

  private final BlockingQueue<WorkerResourceRequest> requestQueue =
      new LinkedBlockingDeque<WorkerResourceRequest>();
  private final RpcUtils.Scrutineer<BasicFuture<ClusterResourceSummary>> summaryRequest =
      new RpcUtils.Scrutineer<BasicFuture<ClusterResourceSummary>>();

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private TajoConf systemConf;

  private ConcurrentMap<ContainerProtocol.TajoContainerIdProto, AllocatedWorkerResource> allocatedResourceMap = Maps
    .newConcurrentMap();

  /** It receives status messages from workers and their resources. */
  private TajoResourceTracker resourceTracker;

  public TajoWorkerResourceManager(TajoMaster.MasterContext masterContext) {
    super(TajoWorkerResourceManager.class.getSimpleName());
    this.masterContext = masterContext;
  }

  public TajoWorkerResourceManager(TajoConf systemConf) {
    super(TajoWorkerResourceManager.class.getSimpleName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Preconditions.checkArgument(conf instanceof TajoConf);
    this.systemConf = (TajoConf) conf;

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    rmContext = new TajoRMContext(dispatcher);

    this.queryIdSeed = String.valueOf(System.currentTimeMillis());

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    this.workerLivelinessMonitor = new WorkerLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.workerLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(WorkerEventType.class, new WorkerEventDispatcher(rmContext));

    resourceTracker = new TajoResourceTracker(this, workerLivelinessMonitor);
    addIfService(resourceTracker);

    super.serviceInit(systemConf);
  }

  @InterfaceAudience.Private
  public static final class WorkerEventDispatcher implements EventHandler<WorkerEvent> {

    private final TajoRMContext rmContext;

    public WorkerEventDispatcher(TajoRMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(WorkerEvent event) {
      int workerId = event.getWorkerId();
      Worker node = this.rmContext.getWorkers().get(workerId);
      if (node != null) {
        try {
          node.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType() + " for node " + workerId, t);
        }
      }
    }
  }

  @Override
  public Map<Integer, Worker> getWorkers() {
    return ImmutableMap.copyOf(rmContext.getWorkers());
  }

  @Override
  public Map<Integer, Worker> getInactiveWorkers() {
    return ImmutableMap.copyOf(rmContext.getInactiveWorkers());
  }

  public Collection<Integer> getQueryMasters() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public void serviceStop() throws Exception {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    if(workerResourceAllocator != null) {
      workerResourceAllocator.interrupt();
    }

    super.serviceStop();
  }

  /**
   *
   * @return The prefix of queryId. It is generated when a TajoMaster starts up.
   */
  @Override
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @VisibleForTesting
  TajoResourceTracker getResourceTracker() {
    return resourceTracker;
  }

  private WorkerResourceAllocationRequest createQMResourceRequest(QueryId queryId) {
    float queryMasterDefaultDiskSlot = masterContext.getConf().getFloatVar(
      TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT);
    int queryMasterDefaultMemoryMB = masterContext.getConf().getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB);

    WorkerResourceAllocationRequest.Builder builder = WorkerResourceAllocationRequest.newBuilder();
    builder.setQueryId(queryId.getProto());
    builder.setMaxMemoryMBPerContainer(queryMasterDefaultMemoryMB);
    builder.setMinMemoryMBPerContainer(queryMasterDefaultMemoryMB);
    builder.setMaxDiskSlotPerContainer(queryMasterDefaultDiskSlot);
    builder.setMinDiskSlotPerContainer(queryMasterDefaultDiskSlot);
    builder.setResourceRequestPriority(ResourceRequestPriority.MEMORY);
    builder.setNumContainers(1);
    return builder.build();
  }

  @Override
  public WorkerAllocatedResource allocateQueryMaster(QueryInProgress queryInProgress) {

    // 3 seconds, by default
    long timeout = masterContext.getConf().getTimeVar(
        TajoConf.ConfVars.TAJO_QUERYMASTER_ALLOCATION_TIMEOUT, TimeUnit.MILLISECONDS);

    // Create a resource request for a query master
    WorkerResourceAllocationRequest qmResourceRequest = createQMResourceRequest(queryInProgress.getQueryId());

    // call future for async call
    final CancelableRpcCallback<WorkerResourceAllocationResponse> callFuture =
        new CancelableRpcCallback<WorkerResourceAllocationResponse>() {
          @Override
          protected void cancel(WorkerResourceAllocationResponse canceled) {
            if (canceled != null && !canceled.getWorkerAllocatedResourceList().isEmpty()) {
              LOG.info("Canceling resources allocated");
              WorkerAllocatedResource resource = canceled.getWorkerAllocatedResource(0);
              releaseWorkerResource(resource.getContainerId());
            }
          }
        };
    allocateWorkerResources(qmResourceRequest, callFuture);

    WorkerResourceAllocationResponse response = null;
    try {
      response = callFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      response = callFuture.cancel(); // try cancel
      if (response == null) {
        // canceled successfuly
        LOG.warn("Got exception waiting resources for query master " + queryInProgress.getQueryId(), t);
        return null;
      }
    }

    if (response == null || response.getWorkerAllocatedResourceList().size() == 0) {
      return null;
    }

    WorkerAllocatedResource resource = response.getWorkerAllocatedResource(0);
    registerQueryMaster(queryInProgress.getQueryId(), resource.getContainerId());
    return resource;
  }

  private void registerQueryMaster(QueryId queryId, ContainerProtocol.TajoContainerIdProto containerId) {
    rmContext.getQueryMasterContainer().putIfAbsent(queryId, containerId);
  }

  @Override
  public void allocateWorkerResources(WorkerResourceAllocationRequest request,
                                      RpcCallback<WorkerResourceAllocationResponse> callBack) {
    try {
      //TODO checking queue size
      requestQueue.put(new WorkerResourceRequest(new QueryId(request.getQueryId()), false, request, callBack));
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static class WorkerResourceRequest {
    boolean queryMasterRequest;
    QueryId queryId;
    WorkerResourceAllocationRequest request;
    RpcCallback<WorkerResourceAllocationResponse> callBack;
    WorkerResourceRequest(
      QueryId queryId,
      boolean queryMasterRequest, WorkerResourceAllocationRequest request,
      RpcCallback<WorkerResourceAllocationResponse> callBack) {
      this.queryId = queryId;
      this.queryMasterRequest = queryMasterRequest;
      this.request = request;
      this.callBack = callBack;
    }
  }

  static class AllocatedWorkerResource {
    Worker worker;
    int allocatedMemoryMB;
    float allocatedDiskSlots;
  }

  private static final long QUEUE_POLLING_TIME = 100;

  class WorkerResourceAllocationThread extends Thread {
    @Override
    public void run() {
      LOG.info("WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        BasicFuture<ClusterResourceSummary> future = summaryRequest.expire();
        if (future != null) {
          future.done(makeClusterResourceSummary());
        }
        try {
          WorkerResourceRequest resourceRequest = requestQueue.poll(
              QUEUE_POLLING_TIME, TimeUnit.MILLISECONDS);
          if (resourceRequest == null) {
            continue;
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("allocateWorkerResources:" +
              (new QueryId(resourceRequest.request.getQueryId())) +
              ", requiredMemory:" + resourceRequest.request.getMinMemoryMBPerContainer() +
              "~" + resourceRequest.request.getMaxMemoryMBPerContainer() +
              ", requiredContainers:" + resourceRequest.request.getNumContainers() +
              ", requiredDiskSlots:" + resourceRequest.request.getMinDiskSlotPerContainer() +
              "~" + resourceRequest.request.getMaxDiskSlotPerContainer() +
              ", queryMasterRequest=" + resourceRequest.queryMasterRequest +
              ", liveWorkers=" + rmContext.getWorkers().size());
          }

          // TajoWorkerResourceManager can't return allocated disk slots occasionally.
          // Because the rest resource request can remains after QueryMaster stops.
          // Thus we need to find whether QueryId stopped or not.
          if (!rmContext.getStoppedQueryIds().contains(resourceRequest.queryId)) {
            List<AllocatedWorkerResource> allocatedWorkerResources = chooseWorkers(resourceRequest);

            if(allocatedWorkerResources.size() > 0) {
              List<WorkerAllocatedResource> allocatedResources =
                new ArrayList<WorkerAllocatedResource>();

              for(AllocatedWorkerResource allocatedResource: allocatedWorkerResources) {
                NodeId nodeId = NodeId.newInstance(allocatedResource.worker.getConnectionInfo().getHost(),
                  allocatedResource.worker.getConnectionInfo().getPeerRpcPort());

                TajoWorkerContainerId containerId = new TajoWorkerContainerId();

                containerId.setApplicationAttemptId(
                  ApplicationIdUtils.createApplicationAttemptId(resourceRequest.queryId));
                containerId.setId(containerIdSeq.incrementAndGet());

                ContainerProtocol.TajoContainerIdProto containerIdProto = containerId.getProto();
                allocatedResources.add(WorkerAllocatedResource.newBuilder()
                  .setContainerId(containerIdProto)
                  .setConnectionInfo(allocatedResource.worker.getConnectionInfo().getProto())
                  .setAllocatedMemoryMB(allocatedResource.allocatedMemoryMB)
                  .setAllocatedDiskSlots(allocatedResource.allocatedDiskSlots)
                  .build());


                allocatedResourceMap.putIfAbsent(containerIdProto, allocatedResource);
              }

              resourceRequest.callBack.run(WorkerResourceAllocationResponse.newBuilder()
                  .setQueryId(resourceRequest.request.getQueryId())
                  .addAllWorkerAllocatedResource(allocatedResources)
                  .build()
              );

            } else {
              if(LOG.isDebugEnabled()) {
                LOG.debug("=========================================");
                LOG.debug("Available Workers");
                for(Worker worker: rmContext.getWorkers().values()) {
                  LOG.debug(worker.toString());
                }
                LOG.debug("=========================================");
              }
              requestQueue.put(resourceRequest);
              Thread.sleep(QUEUE_POLLING_TIME);
            }
          }
        } catch(InterruptedException ie) {
          LOG.error(ie);
        } catch (Throwable t) {
          LOG.error(t, t);
        }
      }
    }
  }

  private static final long MAX_WAIT_TIME = 10000;

  public ClusterResourceSummary getClusterResourceSummary() {
    BasicFuture<ClusterResourceSummary> future =
        summaryRequest.check(new BasicFuture<ClusterResourceSummary>());
    try {
      return future.get(MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to get cluster summary by exception", e);
    }
    return null;
  }

  private ClusterResourceSummary makeClusterResourceSummary() {

    int totalDiskSlots = 0;
    int totalCpuCoreSlots = 0;
    int totalMemoryMB = 0;

    int totalAvailableDiskSlots = 0;
    int totalAvailableCpuCoreSlots = 0;
    int totalAvailableMemoryMB = 0;

    for(Worker worker: rmContext.getWorkers().values()) {

      WorkerResource resource = worker.getResource();

      totalMemoryMB += resource.getMemoryMB();
      totalAvailableMemoryMB += resource.getAvailableMemoryMB();

      totalDiskSlots += resource.getDiskSlots();
      totalAvailableDiskSlots += resource.getAvailableDiskSlots();

      totalCpuCoreSlots += resource.getCpuCoreSlots();
      totalAvailableCpuCoreSlots += resource.getAvailableCpuCoreSlots();
    }

    return ClusterResourceSummary.newBuilder()
        .setNumWorkers(rmContext.getWorkers().size())
        .setTotalCpuCoreSlots(totalCpuCoreSlots)
        .setTotalDiskSlots(totalDiskSlots)
        .setTotalMemoryMB(totalMemoryMB)
        .setTotalAvailableCpuCoreSlots(totalAvailableCpuCoreSlots)
        .setTotalAvailableDiskSlots(totalAvailableDiskSlots)
        .setTotalAvailableMemoryMB(totalAvailableMemoryMB)
        .build();
  }

  private List<AllocatedWorkerResource> chooseWorkers(WorkerResourceRequest resourceRequest) {
    List<AllocatedWorkerResource> selectedWorkers = new ArrayList<AllocatedWorkerResource>();

    int allocatedResources = 0;

    ResourceRequestPriority resourceRequestPriority
      = resourceRequest.request.getResourceRequestPriority();

    List<Worker> randomWorkers = new ArrayList<Worker>(rmContext.getWorkers().values());
    Collections.shuffle(randomWorkers);

    if(resourceRequestPriority == ResourceRequestPriority.MEMORY) {

      int numContainers = resourceRequest.request.getNumContainers();
      int minMemoryMB = resourceRequest.request.getMinMemoryMBPerContainer();
      int maxMemoryMB = resourceRequest.request.getMaxMemoryMBPerContainer();
      float diskSlot = Math.max(resourceRequest.request.getMaxDiskSlotPerContainer(),
          resourceRequest.request.getMinDiskSlotPerContainer());

      int liveWorkerSize = randomWorkers.size();
      Set<Integer> insufficientWorkers = new HashSet<Integer>();
      boolean stop = false;
      boolean checkMax = true;
      while(!stop) {
        if(allocatedResources >= numContainers) {
          break;
        }

        if(insufficientWorkers.size() >= liveWorkerSize) {
          if(!checkMax) {
            break;
          }
          insufficientWorkers.clear();
          checkMax = false;
        }
        int compareAvailableMemory = checkMax ? maxMemoryMB : minMemoryMB;

        for(Worker worker: randomWorkers) {
          if(allocatedResources >= numContainers) {
            stop = true;
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            break;
          }

          WorkerResource workerResource = worker.getResource();
          if(workerResource.getAvailableMemoryMB() >= compareAvailableMemory) {
            int workerMemory;
            if(workerResource.getAvailableMemoryMB() >= maxMemoryMB) {
              workerMemory = maxMemoryMB;
            } else {
              workerMemory = workerResource.getAvailableMemoryMB();
            }
            AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
            allocatedWorkerResource.worker = worker;
            allocatedWorkerResource.allocatedMemoryMB = workerMemory;
            if(workerResource.getAvailableDiskSlots() >= diskSlot) {
              allocatedWorkerResource.allocatedDiskSlots = diskSlot;
            } else {
              allocatedWorkerResource.allocatedDiskSlots = workerResource.getAvailableDiskSlots();
            }

            workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                allocatedWorkerResource.allocatedMemoryMB);

            selectedWorkers.add(allocatedWorkerResource);

            allocatedResources++;
          } else {
            insufficientWorkers.add(worker.getWorkerId());
          }
        }
      }
    } else {
      int numContainers = resourceRequest.request.getNumContainers();
      float minDiskSlots = resourceRequest.request.getMinDiskSlotPerContainer();
      float maxDiskSlots = resourceRequest.request.getMaxDiskSlotPerContainer();
      int memoryMB = Math.max(resourceRequest.request.getMaxMemoryMBPerContainer(),
          resourceRequest.request.getMinMemoryMBPerContainer());

      int liveWorkerSize = randomWorkers.size();
      Set<Integer> insufficientWorkers = new HashSet<Integer>();
      boolean stop = false;
      boolean checkMax = true;
      while(!stop) {
        if(allocatedResources >= numContainers) {
          break;
        }

        if(insufficientWorkers.size() >= liveWorkerSize) {
          if(!checkMax) {
            break;
          }
          insufficientWorkers.clear();
          checkMax = false;
        }
        float compareAvailableDisk = checkMax ? maxDiskSlots : minDiskSlots;

        for(Worker worker: randomWorkers) {
          if(allocatedResources >= numContainers) {
            stop = true;
            break;
          }

          if(insufficientWorkers.size() >= liveWorkerSize) {
            break;
          }

          WorkerResource workerResource = worker.getResource();
          if(workerResource.getAvailableDiskSlots() >= compareAvailableDisk) {
            float workerDiskSlots;
            if(workerResource.getAvailableDiskSlots() >= maxDiskSlots) {
              workerDiskSlots = maxDiskSlots;
            } else {
              workerDiskSlots = workerResource.getAvailableDiskSlots();
            }
            AllocatedWorkerResource allocatedWorkerResource = new AllocatedWorkerResource();
            allocatedWorkerResource.worker = worker;
            allocatedWorkerResource.allocatedDiskSlots = workerDiskSlots;

            if(workerResource.getAvailableMemoryMB() >= memoryMB) {
              allocatedWorkerResource.allocatedMemoryMB = memoryMB;
            } else {
              allocatedWorkerResource.allocatedMemoryMB = workerResource.getAvailableMemoryMB();
            }
            workerResource.allocateResource(allocatedWorkerResource.allocatedDiskSlots,
                allocatedWorkerResource.allocatedMemoryMB);

            selectedWorkers.add(allocatedWorkerResource);

            allocatedResources++;
          } else {
            insufficientWorkers.add(worker.getWorkerId());
          }
        }
      }
    }
    return selectedWorkers;
  }

  /**
   * Release allocated resource.
   *
   * @param containerId ContainerIdProto to be released
   */
  @Override
  public void releaseWorkerResource(ContainerProtocol.TajoContainerIdProto containerId) {
    AllocatedWorkerResource allocated = allocatedResourceMap.remove(containerId);
    if(allocated != null) {
      LOG.info("Release Resource: " + allocated.allocatedDiskSlots + "," + allocated.allocatedMemoryMB);
      allocated.worker.getResource().releaseResource( allocated.allocatedDiskSlots, allocated.allocatedMemoryMB);
    } else {
      LOG.warn("No AllocatedWorkerResource data for [" + containerId + "]");
      return;
    }
  }

  @Override
  public boolean isQueryMasterStopped(QueryId queryId) {
    return !rmContext.getQueryMasterContainer().containsKey(queryId);
  }

  @Override
  public void releaseQueryMaster(QueryId queryId) {
    if(!rmContext.getQueryMasterContainer().containsKey(queryId)) {
      LOG.warn("No QueryMaster resource info for " + queryId);
      return;
    } else {
      ContainerProtocol.TajoContainerIdProto containerId = rmContext.getQueryMasterContainer().remove(queryId);
      releaseWorkerResource(containerId);
      rmContext.getStoppedQueryIds().add(queryId);
      LOG.info(String.format("Released QueryMaster (%s) resource." , queryId.toString()));
    }
  }

  public TajoRMContext getRMContext() {
    return rmContext;
  }
}
