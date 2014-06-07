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
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.util.ApplicationIdUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import static org.apache.tajo.ipc.TajoMasterProtocol.*;


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

  private BlockingQueue<WorkerResourceRequest> requestQueue;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private TajoConf systemConf;

  private ConcurrentMap<ContainerIdProto, AllocatedWorkerResource> allocatedResourceMap = Maps.newConcurrentMap();

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

    requestQueue = new LinkedBlockingDeque<WorkerResourceRequest>();

    workerResourceAllocator = new WorkerResourceAllocationThread();
    workerResourceAllocator.start();

    this.workerLivelinessMonitor = new WorkerLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.workerLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(WorkerEventType.class, new WorkerEventDispatcher(rmContext));

    resourceTracker = new TajoResourceTracker(rmContext, workerLivelinessMonitor);
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
      String workerId = event.getWorkerId();
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
  public Map<String, Worker> getWorkers() {
    return ImmutableMap.copyOf(rmContext.getWorkers());
  }

  @Override
  public Map<String, Worker> getInactiveWorkers() {
    return ImmutableMap.copyOf(rmContext.getInactiveWorkers());
  }

  public Collection<String> getQueryMasters() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    return resourceTracker.getClusterResourceSummary();
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
    builder.setResourceRequestPriority(TajoMasterProtocol.ResourceRequestPriority.MEMORY);
    builder.setNumContainers(1);
    return builder.build();
  }

  @Override
  public WorkerAllocatedResource allocateQueryMaster(QueryInProgress queryInProgress) {
    // Create a resource request for a query master
    WorkerResourceAllocationRequest qmResourceRequest = createQMResourceRequest(queryInProgress.getQueryId());

    // call future for async call
    CallFuture<WorkerResourceAllocationResponse> callFuture = new CallFuture<WorkerResourceAllocationResponse>();
    allocateWorkerResources(qmResourceRequest, callFuture);

    // Wait for 3 seconds
    WorkerResourceAllocationResponse response = null;
    try {
      response = callFuture.get(3, TimeUnit.SECONDS);
    } catch (Throwable t) {
      LOG.error(t);
      return null;
    }

    if (response.getWorkerAllocatedResourceList().size() == 0) {
      return null;
    }

    WorkerAllocatedResource resource = response.getWorkerAllocatedResource(0);
    registerQueryMaster(queryInProgress.getQueryId(), resource.getContainerId());
    return resource;
  }

  private void registerQueryMaster(QueryId queryId, ContainerIdProto containerId) {
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

  class WorkerResourceAllocationThread extends Thread {
    @Override
    public void run() {
      LOG.info("WorkerResourceAllocationThread start");
      while(!stopped.get()) {
        try {
          WorkerResourceRequest resourceRequest = requestQueue.take();

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
                NodeId nodeId = NodeId.newInstance(allocatedResource.worker.getHostName(),
                    allocatedResource.worker.getPeerRpcPort());

                TajoWorkerContainerId containerId = new TajoWorkerContainerId();

                containerId.setApplicationAttemptId(
                    ApplicationIdUtils.createApplicationAttemptId(resourceRequest.queryId));
                containerId.setId(containerIdSeq.incrementAndGet());

                ContainerIdProto containerIdProto = containerId.getProto();
                allocatedResources.add(WorkerAllocatedResource.newBuilder()
                    .setContainerId(containerIdProto)
                    .setNodeId(nodeId.toString())
                    .setWorkerHost(allocatedResource.worker.getHostName())
                    .setQueryMasterPort(allocatedResource.worker.getQueryMasterPort())
                    .setClientPort(allocatedResource.worker.getClientPort())
                    .setPeerRpcPort(allocatedResource.worker.getPeerRpcPort())
                    .setWorkerPullServerPort(allocatedResource.worker.getPullServerPort())
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
                for(String liveWorker: rmContext.getWorkers().keySet()) {
                  LOG.debug(rmContext.getWorkers().get(liveWorker).toString());
                }
                LOG.debug("=========================================");
              }
              requestQueue.put(resourceRequest);
              Thread.sleep(100);
            }
          }

        } catch(InterruptedException ie) {
          LOG.error(ie);
        }
      }
    }
  }

  private List<AllocatedWorkerResource> chooseWorkers(WorkerResourceRequest resourceRequest) {
    List<AllocatedWorkerResource> selectedWorkers = new ArrayList<AllocatedWorkerResource>();

    int allocatedResources = 0;

    TajoMasterProtocol.ResourceRequestPriority resourceRequestPriority
        = resourceRequest.request.getResourceRequestPriority();

    if(resourceRequestPriority == TajoMasterProtocol.ResourceRequestPriority.MEMORY) {
      synchronized(rmContext) {
        List<String> randomWorkers = new ArrayList<String>(rmContext.getWorkers().keySet());
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        int minMemoryMB = resourceRequest.request.getMinMemoryMBPerContainer();
        int maxMemoryMB = resourceRequest.request.getMaxMemoryMBPerContainer();
        float diskSlot = Math.max(resourceRequest.request.getMaxDiskSlotPerContainer(),
            resourceRequest.request.getMinDiskSlotPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
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

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
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
              insufficientWorkers.add(eachWorker);
            }
          }
        }
      }
    } else {
      synchronized(rmContext) {
        List<String> randomWorkers = new ArrayList<String>(rmContext.getWorkers().keySet());
        Collections.shuffle(randomWorkers);

        int numContainers = resourceRequest.request.getNumContainers();
        float minDiskSlots = resourceRequest.request.getMinDiskSlotPerContainer();
        float maxDiskSlots = resourceRequest.request.getMaxDiskSlotPerContainer();
        int memoryMB = Math.max(resourceRequest.request.getMaxMemoryMBPerContainer(),
            resourceRequest.request.getMinMemoryMBPerContainer());

        int liveWorkerSize = randomWorkers.size();
        Set<String> insufficientWorkers = new HashSet<String>();
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

          for(String eachWorker: randomWorkers) {
            if(allocatedResources >= numContainers) {
              stop = true;
              break;
            }

            if(insufficientWorkers.size() >= liveWorkerSize) {
              break;
            }

            Worker worker = rmContext.getWorkers().get(eachWorker);
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
              insufficientWorkers.add(eachWorker);
            }
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
  public void releaseWorkerResource(ContainerIdProto containerId) {
    AllocatedWorkerResource allocated = allocatedResourceMap.get(containerId);
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
  public void stopQueryMaster(QueryId queryId) {
    if(!rmContext.getQueryMasterContainer().containsKey(queryId)) {
      LOG.warn("No QueryMaster resource info for " + queryId);
      return;
    } else {
      ContainerIdProto containerId = rmContext.getQueryMasterContainer().remove(queryId);
      releaseWorkerResource(containerId);
      rmContext.getStoppedQueryIds().add(queryId);
      LOG.info(String.format("Released QueryMaster (%s) resource." , queryId.toString()));
    }
  }

  public TajoRMContext getRMContext() {
    return rmContext;
  }
}
