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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.storage.DiskUtil;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.event.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

public class NodeResourceManager extends AbstractService implements EventHandler<NodeResourceEvent> {
  private static final Log LOG = LogFactory.getLog(NodeResourceManager.class);

  private final Dispatcher dispatcher;
  private final EventHandler taskEventHandler;
  private NodeResource totalResource;
  private NodeResource availableResource;
  private TajoConf tajoConf;

  public NodeResourceManager(Dispatcher dispatcher, EventHandler taskEventHandler) {
    super(NodeResourceManager.class.getName());
    this.dispatcher = dispatcher;
    this.taskEventHandler = taskEventHandler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.tajoConf = (TajoConf)conf;
    this.totalResource = createWorkerResource(tajoConf);
    this.availableResource = NodeResources.clone(totalResource);
    this.dispatcher.register(NodeResourceEvent.EventType.class, this);

    super.serviceInit(conf);
    LOG.info("Initialized NodeResourceManager for " + totalResource);
  }

  @Override
  public void handle(NodeResourceEvent event) {

    if (event instanceof NodeResourceAllocateEvent) {
      NodeResourceAllocateEvent allocateEvent = (NodeResourceAllocateEvent) event;
      BatchAllocationResponseProto.Builder response = BatchAllocationResponseProto.newBuilder();
      for (TaskAllocationRequestProto request : allocateEvent.getRequest().getTaskRequestList()) {
        NodeResource resource = new NodeResource(request.getResource());
        if (allocate(resource)) {
          if(allocateEvent.getRequest().hasExecutionBlockRequest()){
            //send ExecutionBlock start event to TaskManager
            startExecutionBlock(allocateEvent.getRequest().getExecutionBlockRequest());
          }

          //send task start event to TaskExecutor
          startTask(request.getTaskRequest(), resource);
        } else {
          // reject the exceeded requests
          response.addCancellationTask(request);
        }
      }
      allocateEvent.getCallback().run(response.build());

    } else if (event instanceof NodeResourceDeallocateEvent) {
      NodeResourceDeallocateEvent deallocateEvent = (NodeResourceDeallocateEvent) event;
      release(deallocateEvent.getResource());

      // send current resource to ResourceTracker
      getDispatcher().getEventHandler().handle(
          new NodeStatusEvent(NodeStatusEvent.EventType.REPORT_RESOURCE));
    }
  }

  protected Dispatcher getDispatcher() {
    return dispatcher;
  }

  protected NodeResource getTotalResource() {
    return totalResource;
  }

  protected NodeResource getAvailableResource() {
    return availableResource;
  }

  private boolean allocate(NodeResource resource) {
    //TODO consider the jvm free memory
    if (NodeResources.fitsIn(resource, availableResource)) {
      NodeResources.subtractFrom(availableResource, resource);
      return true;
    }
    return false;
  }

  protected void startExecutionBlock(RunExecutionBlockRequestProto request) {
    taskEventHandler.handle(new ExecutionBlockStartEvent(request));
  }

  protected void startTask(TaskRequestProto request, NodeResource resource) {
    taskEventHandler.handle(new TaskStartEvent(request, resource));
  }

  private void release(NodeResource resource) {
    NodeResources.addTo(availableResource, resource);
  }

  private NodeResource createWorkerResource(TajoConf conf) {
    int memoryMb;

    if (conf.get(CommonTestingUtil.TAJO_TEST_KEY, "FALSE").equalsIgnoreCase("TRUE")) {
      memoryMb = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB);
    } else {
      memoryMb = Math.min((int) (Runtime.getRuntime().maxMemory() / StorageUnit.MB),
          conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB));
    }

    int vCores = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    int disks = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISKS_NUM);

    int dataNodeStorageSize = DiskUtil.getDataNodeStorageSize();
    if (conf.getBoolVar(TajoConf.ConfVars.WORKER_RESOURCE_DFS_DIR_AWARE) && dataNodeStorageSize > 0) {
      disks = dataNodeStorageSize;
    }

    int diskParallels = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM);
    return NodeResource.createResource(memoryMb, disks * diskParallels, vCores);
  }
}
