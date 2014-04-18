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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class YarnRMContainerAllocator extends AMRMClientImpl
    implements EventHandler<ContainerAllocationEvent> {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(YarnRMContainerAllocator.
      class.getName());

  private QueryMasterTask.QueryMasterTaskContext context;
  private ApplicationAttemptId appAttemptId;
  private final EventHandler eventHandler;

  public YarnRMContainerAllocator(QueryMasterTask.QueryMasterTaskContext context) {
    super();
    this.context = context;
    this.appAttemptId = ApplicationIdUtils.createApplicationAttemptId(context.getQueryId());
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
      context.getQueryMasterContext().getWorkerContext().setNumClusterNodes(allocateResponse.getNumClusterNodes());
    } catch (IOException e) {
      LOG.error(e);
    } catch (YarnException e) {
      LOG.error(e);
    }

    startAllocatorThread();
  }

  protected Thread allocatorThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private int rmPollInterval = 100;//millis

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

  private AtomicLong prevReportTime = new AtomicLong(0);
  private int reportInterval = 5 * 1000; // second

  public void heartbeat() throws Exception {
    AllocateResponse allocateResponse = allocate(context.getProgress());

    List<Container> allocatedContainers = allocateResponse.getAllocatedContainers();

    long currentTime = System.currentTimeMillis();
    if ((currentTime - prevReportTime.longValue()) >= reportInterval) {
      LOG.debug("Available Cluster Nodes: " + allocateResponse.getNumClusterNodes());
      LOG.debug("Num of Allocated Containers: " + allocatedContainers.size());
      LOG.info("Available Resource: " + allocateResponse.getAvailableResources());
      prevReportTime.set(currentTime);
    }

    if (allocatedContainers.size() > 0) {
      LOG.info("================================================================");
      for (Container container : allocateResponse.getAllocatedContainers()) {
        LOG.info("> Container Id: " + container.getId());
        LOG.info("> Node Id: " + container.getNodeId());
        LOG.info("> Resource (Mem): " + container.getResource().getMemory());
        LOG.info("> Priority: " + container.getPriority());
      }
      LOG.info("================================================================");

      Map<ExecutionBlockId, List<Container>> allocated = new HashMap<ExecutionBlockId, List<Container>>();
      for (Container container : allocatedContainers) {
        ExecutionBlockId executionBlockId = subQueryMap.get(container.getPriority());
        SubQueryState state = context.getSubQuery(executionBlockId).getState();
        if (!(SubQuery.isRunningState(state))) {
          releaseAssignedContainer(container.getId());
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
          event.getPriority()));

    } else if (event.getType() == ContainerAllocatorEventType.CONTAINER_DEALLOCATE) {
      LOG.info(event);
    } else {
      LOG.info(event);
    }
  }
}
