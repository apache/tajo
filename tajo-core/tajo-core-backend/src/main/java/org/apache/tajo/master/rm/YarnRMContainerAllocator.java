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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.querymaster.Query;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.util.ApplicationIdUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class YarnRMContainerAllocator extends AMRMClientImpl
    implements EventHandler<ContainerAllocationEvent> {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(YarnRMContainerAllocator.
      class.getName());

  private QueryMasterTask.QueryContext context;
  private final EventHandler eventHandler;

  public YarnRMContainerAllocator(QueryMasterTask.QueryContext context) {
    super(ApplicationIdUtils.createApplicationAttemptId(context.getQueryId()));
    this.context = context;
    this.eventHandler = context.getDispatcher().getEventHandler();
  }

  public void init(Configuration conf) {
    super.init(conf);
  }

  private static final int WAIT_INTERVAL_AVAILABLE_NODES = 500; // 0.5 second
  public void start() {
    super.start();

    RegisterApplicationMasterResponse response;
    try {
      response = registerApplicationMaster("localhost", 10080, "http://localhost:1234");
      context.getResourceAllocator().setMaxContainerCapability(response.getMaximumResourceCapability().getMemory());
      context.getResourceAllocator().setMinContainerCapability(response.getMinimumResourceCapability().getMemory());

      // If the number of cluster nodes is ZERO, it waits for available nodes.
      AllocateResponse allocateResponse = allocate(0.0f);
      while(allocateResponse.getNumClusterNodes() < 1) {
        try {
          Thread.sleep(WAIT_INTERVAL_AVAILABLE_NODES);
          LOG.info("Waiting for Available Cluster Nodes");
          allocateResponse = allocate(0);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
      context.getResourceAllocator().setNumClusterNodes(allocateResponse.getNumClusterNodes());
    } catch (YarnRemoteException e) {
      LOG.error(e);
    }

    startAllocatorThread();
  }

  protected Thread allocatorThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private int rmPollInterval = 1000;//millis

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            try {
              heartbeat();
            } catch (YarnException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              // TODO: for other exceptions
              if(stopped.get()) {
                break;
              }
            }
            Thread.sleep(rmPollInterval);
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            break;
          }
        }
        LOG.info("Allocated thread stopped");
      }
    });
    allocatorThread.setName("YarnRMContainerAllocator");
    allocatorThread.start();
  }

  public void stop() {
    if(stopped.get()) {
      return;
    }
    LOG.info("un-registering ApplicationMaster(QueryMaster):" + appAttemptId);
    stopped.set(true);

    try {
      FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;
      Query query = context.getQuery();
      if (query != null) {
        TajoProtos.QueryState state = query.getState();
        if (state == TajoProtos.QueryState.QUERY_SUCCEEDED) {
          status = FinalApplicationStatus.SUCCEEDED;
        } else if (state == TajoProtos.QueryState.QUERY_FAILED || state == TajoProtos.QueryState.QUERY_ERROR) {
          status = FinalApplicationStatus.FAILED;
        } else if (state == TajoProtos.QueryState.QUERY_ERROR) {
          status = FinalApplicationStatus.FAILED;
        }
      }
      unregisterApplicationMaster(status, "tajo query finished", null);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    allocatorThread.interrupt();
    LOG.info("un-registered ApplicationMAster(QueryMaster) stopped:" + appAttemptId);

    super.stop();
  }

  private final Map<Priority, ExecutionBlockId> subQueryMap =
      new HashMap<Priority, ExecutionBlockId>();

  public void heartbeat() throws Exception {
    AllocateResponse allocateResponse = allocate(context.getProgress());
    AMResponse response = allocateResponse.getAMResponse();
    if(response == null) {
      LOG.warn("AM Response is null");
      return;
    }
    List<Container> allocatedContainers = response.getAllocatedContainers();

    LOG.info("Available Cluster Nodes: " + allocateResponse.getNumClusterNodes());
    LOG.info("Available Resource: " + response.getAvailableResources());
    LOG.info("Num of Allocated Containers: " + response.getAllocatedContainers().size());
    if (response.getAllocatedContainers().size() > 0) {
      LOG.info("================================================================");
      for (Container container : response.getAllocatedContainers()) {
        LOG.info("> Container Id: " + container.getId());
        LOG.info("> Node Id: " + container.getNodeId());
        LOG.info("> Resource (Mem): " + container.getResource().getMemory());
        LOG.info("> State : " + container.getState());
        LOG.info("> Priority: " + container.getPriority());
      }
      LOG.info("================================================================");
    }

    Map<ExecutionBlockId, List<Container>> allocated = new HashMap<ExecutionBlockId, List<Container>>();
    if (allocatedContainers.size() > 0) {
      for (Container container : allocatedContainers) {
        ExecutionBlockId executionBlockId = subQueryMap.get(container.getPriority());
        SubQueryState state = context.getSubQuery(executionBlockId).getState();
        if (!(SubQuery.isRunningState(state) && subQueryMap.containsKey(container.getPriority()))) {
          releaseAssignedContainer(container.getId());
          synchronized (subQueryMap) {
            subQueryMap.remove(container.getPriority());
          }
        } else {
          if (allocated.containsKey(executionBlockId)) {
            allocated.get(executionBlockId).add(container);
          } else {
            allocated.put(executionBlockId, Lists.newArrayList(container));
          }
        }
      }

      for (Entry<ExecutionBlockId, List<Container>> entry : allocated.entrySet()) {
        eventHandler.handle(new SubQueryContainerAllocationEvent(entry.getKey(), entry.getValue()));
      }
    }
  }

  @Override
  public void handle(ContainerAllocationEvent event) {

    if (event.getType() == ContainerAllocatorEventType.CONTAINER_REQ) {
      LOG.info(event);
      subQueryMap.put(event.getPriority(), event.getExecutionBlockId());
      addContainerRequest(new ContainerRequest(event.getCapability(), null, null,
          event.getPriority(), event.getRequiredNum()));

    } else if (event.getType() == ContainerAllocatorEventType.CONTAINER_DEALLOCATE) {
      LOG.info(event);
    } else {
      LOG.info(event);
    }
  }
}
