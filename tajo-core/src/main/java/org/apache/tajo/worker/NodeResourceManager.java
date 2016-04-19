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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.*;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ResourceProtos.*;

public class NodeResourceManager extends AbstractService implements EventHandler<NodeResourceEvent> {
  private static final Log LOG = LogFactory.getLog(NodeResourceManager.class);

  private final Dispatcher dispatcher;
  private final TajoWorker.WorkerContext workerContext;
  private final AtomicInteger runningQueryMasters = new AtomicInteger(0);
  private final HashMap<Integer, AtomicInteger> volumeMap = Maps.newHashMap();
  private NodeResource totalResource;
  private NodeResource availableResource;
  private TajoConf tajoConf;
  private boolean enableTest;
  private int diskParallels;

  public NodeResourceManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext) {
    super(NodeResourceManager.class.getName());
    this.dispatcher = dispatcher;
    this.workerContext = workerContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    this.totalResource = createWorkerResource(tajoConf);
    this.availableResource = NodeResources.clone(totalResource);
    this.dispatcher.register(NodeResourceEvent.EventType.class, this);
    validateConf(tajoConf);
    this.enableTest = conf.get(TajoConstants.TEST_KEY, Boolean.FALSE.toString())
        .equalsIgnoreCase(Boolean.TRUE.toString());
    this.diskParallels = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM);
    super.serviceInit(conf);
    LOG.info("Initialized NodeResourceManager for " + totalResource);
  }

  @Override
  public void handle(NodeResourceEvent event) {

    switch (event.getType()) {
      case ALLOCATE: {
        if (event.getResourceType() == NodeResourceEvent.ResourceType.TASK) {
          // allocate task resource
          NodeResourceAllocateEvent allocateEvent = TUtil.checkTypeAndGet(event, NodeResourceAllocateEvent.class);
          BatchAllocationResponse.Builder response = BatchAllocationResponse.newBuilder();
          for (TaskAllocationProto request : allocateEvent.getRequest().getTaskRequestList()) {
            Allocation allocation = new Allocation(request);

            if (allocate(allocation)) {
              //send task start event to TaskExecutor
              startTask(request.getTaskRequest(), allocation);
            } else {
              // reject the exceeded requests
              response.addCancellationTask(request);
            }
          }
          allocateEvent.getCallback().run(response.build());

        } else if (event.getResourceType() == NodeResourceEvent.ResourceType.QUERY_MASTER) {
          QMResourceAllocateEvent allocateEvent = TUtil.checkTypeAndGet(event, QMResourceAllocateEvent.class);
          // allocate query master resource

          Allocation allocation = new Allocation(new NodeResource(allocateEvent.getRequest().getResource()));
          if (allocate(allocation)) {
            allocateEvent.getCallback().run(TajoWorker.TRUE_PROTO);
            runningQueryMasters.incrementAndGet();
          } else {
            allocateEvent.getCallback().run(TajoWorker.FALSE_PROTO);
          }
        }
        break;
      }
      case DEALLOCATE: {
        NodeResourceDeallocateEvent deallocateEvent = TUtil.checkTypeAndGet(event, NodeResourceDeallocateEvent.class);
        release(deallocateEvent.getAllocation());

        if (deallocateEvent.getResourceType() == NodeResourceEvent.ResourceType.QUERY_MASTER) {
          runningQueryMasters.decrementAndGet();
        }
        // send current resource to ResourceTracker
        getDispatcher().getEventHandler().handle(
            new NodeStatusEvent(NodeStatusEvent.EventType.REPORT_RESOURCE));
        break;
      }
    }
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public NodeResource getTotalResource() {
    return totalResource;
  }

  public NodeResource getAvailableResource() {
    return availableResource;
  }

  public int getRunningQueryMasters() {
    return runningQueryMasters.get();
  }

  private boolean allocate(Allocation allocation) {

    if (allocation.hasVolumeId() && allocation.getVolumeId() > DataLocation.UNKNOWN_VOLUME_ID) {
      int volumeId = allocation.getVolumeId();

      if (!volumeMap.containsKey(volumeId)) {
        AtomicInteger load = new AtomicInteger();
        volumeMap.put(volumeId, load);
      }

      //This load is measured by counting how many number of tasks are running.
      if (volumeMap.get(volumeId).get() < diskParallels && allocateResource(allocation.getResource())) {
        volumeMap.get(volumeId).incrementAndGet();
        return true;
      } else {
        return false;
      }
    } else {
      return allocateResource(allocation.getResource());
    }
  }

  private boolean allocateResource(NodeResource resource) {
    if (NodeResources.fitsIn(resource, availableResource) && checkFreeHeapMemory(resource)) {
      NodeResources.subtractFrom(availableResource, resource);
      return true;
    }
    return false;
  }

  private boolean checkFreeHeapMemory(NodeResource resource) {
    //TODO consider the jvm free memory
    return true;
  }

  @SuppressWarnings("unchecked")
  protected void startTask(TaskRequestProto request, Allocation allocation) {
    workerContext.getTaskManager().getDispatcher().getEventHandler().handle(new TaskStartEvent(request, allocation));
  }

  private void release(Allocation allocation) {
    NodeResources.addTo(availableResource, allocation.getResource());

    if (allocation.hasVolumeId() && allocation.getVolumeId() > DataLocation.UNKNOWN_VOLUME_ID) {
      volumeMap.get(allocation.getVolumeId()).decrementAndGet();
    }
  }

  private NodeResource createWorkerResource(TajoConf conf) {

    int memoryMb = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB);
    if (!enableTest) {
      // Set memory resource to max heap
      int maxHeap = (int) (Runtime.getRuntime().maxMemory() / StorageUnit.MB);
      if(maxHeap > memoryMb) {
        memoryMb = maxHeap;
        conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB, memoryMb);
      }
    }

    int vCores = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    return NodeResource.createResource(memoryMb, vCores);
  }

  private void validateConf(TajoConf conf) {
    // validate node memory allocation setting
    int minMem = conf.getIntVar(TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY);
    int minQMMem = conf.getIntVar(TajoConf.ConfVars.QUERYMASTER_MINIMUM_MEMORY);
    int maxMem = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB);

    if (minMem <= 0 || minQMMem <= 0 || minMem + minQMMem > maxMem) {
      throw new RuntimeException("Invalid resource worker memory"
          + " allocation configuration"
          + ", " + TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY.varname
          + "=" + minMem
          + ", " + TajoConf.ConfVars.QUERYMASTER_MINIMUM_MEMORY.varname
          + "=" + minQMMem
          + ", " + TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB.varname
          + "=" + maxMem + ", min and max should be greater than 0"
          + ", max should be no smaller than min.");
    }
  }

  public static class Allocation {
    private NodeResource resource;
    private int volumeId;

    public Allocation(NodeResource resource) {
      this(resource, DataLocation.UNSET_VOLUME_ID);
    }

    public Allocation(TaskAllocationProto taskAllocation) {
      this(new NodeResource(taskAllocation.getResource()),
          taskAllocation.hasVolumeId() ? taskAllocation.getVolumeId() : DataLocation.UNSET_VOLUME_ID);
    }

    public Allocation(NodeResource resource, int volumeId) {
      this.volumeId = volumeId;
      this.resource = resource;
    }

    public NodeResource getResource() {
      return resource;
    }

    public int getVolumeId() {
      return volumeId;
    }

    public boolean hasVolumeId() {
      return volumeId != DataLocation.UNSET_VOLUME_ID;
    }

    @Override
    public String toString() {
      return "Resource: " + resource + (hasVolumeId() ? "VolumeId: " + volumeId : "");
    }
  }
}
