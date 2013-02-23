/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master.rm;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import tajo.SubQueryId;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.SubQueryState;
import tajo.master.event.*;

import java.util.*;
import java.util.Map.Entry;

public class RMContainerAllocator extends RMCommunicator
    implements EventHandler<ContainerAllocationEvent> {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.
      class.getName());

  private final EventHandler eventHandler;

  public RMContainerAllocator(QueryContext context) {
    super(context);
    this.eventHandler = context.getDispatcher().getEventHandler();
  }

  private Map<Priority, SubQueryId> subQueryMap
      = new HashMap<Priority, SubQueryId>();

  @Override
  public void heartbeat() throws Exception {
    List<Container> allocatedContainers = getResources();
    Map<SubQueryId, List<Container>> allocated = new HashMap<SubQueryId, List<Container>>();
    if (allocatedContainers.size() > 0) {
      for (Container container : allocatedContainers) {
        SubQueryId subQueryId = subQueryMap.get(container.getPriority());
        if (!subQueryMap.containsKey(container.getPriority()) ||
            query.getSubQuery(subQueryId).getState() == SubQueryState.SUCCEEDED) {
          release.add(container.getId());
          synchronized (subQueryMap) {
            subQueryMap.remove(container.getPriority());
          }
        } else {
          if (allocated.containsKey(subQueryId)) {
            allocated.get(subQueryId).add(container);
          } else {
            allocated.put(subQueryId, Lists.newArrayList(container));
          }
        }
      }

      for (Entry<SubQueryId, List<Container>> entry : allocated.entrySet()) {
        eventHandler.handle(
            new SubQueryContainerAllocationEvent(entry.getKey(),
            entry.getValue()));
      }
    }
  }

  @Override
  public void handle(ContainerAllocationEvent event) {

    if (event.getType() == ContainerAllocatorEventType.CONTAINER_REQ) {
      LOG.info(event);
      assign(event);

    } else if (event.getType() == ContainerAllocatorEventType.CONTAINER_DEALLOCATE) {
      LOG.info(event);
    } else {
      LOG.info(event);
    }
  }

  public void assign(ContainerAllocationEvent event) {
    SubQueryId subQueryId = event.getSubQueryId();
    subQueryMap.put(event.getPriority(), event.getSubQueryId());

    int memRequred;
    int minContainerCapability;
    int supportedMaxContainerCapability =
        getMaxContainerCapability().getMemory();

    memRequred = event.getCapability().getMemory();
    minContainerCapability = getMinContainerCapability().getMemory();
    if (memRequred < minContainerCapability) {
      memRequred = minContainerCapability;
    }

    if (memRequred > getMaxContainerCapability().getMemory()) {
      String diagMsg = "Task capability required is more than the supported " +
          "max container capability in the cluster. Killing the Job. mapResourceReqt: " +
          memRequred + " maxContainerCapability:" + supportedMaxContainerCapability;
      LOG.info(diagMsg);
      eventHandler.handle(new QueryDiagnosticsUpdateEvent(
          subQueryId.getQueryId(), diagMsg));
      eventHandler.handle(new QueryEvent(subQueryId.getQueryId(),
          QueryEventType.KILL));
    }
    LOG.info("mapResourceReqt:"+memRequred);
    /*
    if (event.isLeafQuery() && event instanceof GrouppedContainerAllocatorEvent) {
      GrouppedContainerAllocatorEvent allocatorEvent =
          (GrouppedContainerAllocatorEvent) event;
      List<ResourceRequest> requestList = new ArrayList<>();
      for (Entry<String, Integer> request :
          allocatorEvent.getRequestMap().entrySet()) {

        ResourceRequest resReq = Records.newRecord(ResourceRequest.class);
        // TODO - to consider the data locality
        resReq.setHostName("*");
        resReq.setCapability(allocatorEvent.getCapability());
        resReq.setNumContainers(request.getValue());
        resReq.setPriority(allocatorEvent.getPriority());
        requestList.add(resReq);
      }

      ask.addAll(new ArrayList<>(requestList));
      LOG.info(requestList.size());
      LOG.info(ask.size());
    } else {*/
      ResourceRequest resReq = Records.newRecord(ResourceRequest.class);
      resReq.setHostName("*");
      resReq.setCapability(event.getCapability());
      resReq.setNumContainers(event.getRequiredNum());
      resReq.setPriority(event.getPriority());
      ask.add(resReq);
    //}
  }

  Set<ResourceRequest> ask = new HashSet<ResourceRequest>();
  Set<ContainerId> release = new HashSet<ContainerId>();
  Resource availableResources;
  int lastClusterNmCount = 0;
  int clusterNmCount = 0;
  int lastResponseID = 1;

  protected AMResponse makeRemoteRequest() throws YarnException, YarnRemoteException {
    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        applicationAttemptId, lastResponseID, 0.0f,
        new ArrayList<ResourceRequest>(ask), new ArrayList<ContainerId>(release));
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response = allocateResponse.getAMResponse();
    lastResponseID = response.getResponseId();
    availableResources = response.getAvailableResources();
    lastClusterNmCount = clusterNmCount;
    clusterNmCount = allocateResponse.getNumClusterNodes();

    //LOG.info("Response Id: " + response.getResponseId());
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
    /*
    LOG.info("Reboot: " + response.getReboot());
    LOG.info("Num of Updated Node: " + response.getUpdatedNodes());
    for (NodeReport nodeReport : response.getUpdatedNodes()) {
      LOG.info("> Node Id: " + nodeReport.getNodeId());
      LOG.info("> Node State: " + nodeReport.getNodeState());
      LOG.info("> Rack Name: " + nodeReport.getRackName());
      LOG.info("> Used: " + nodeReport.getUsed());
    }
    */


    if (ask.size() > 0 || release.size() > 0) {
      LOG.info("getResources() for " + applicationId + ":" + " ask="
          + ask.size() + " release= " + release.size() + " newContainers="
          + response.getAllocatedContainers().size() + " finishedContainers="
          + response.getCompletedContainersStatuses().size()
          + " resourcelimit=" + availableResources + " knownNMs="
          + clusterNmCount);
    }

    ask.clear();
    release.clear();
    return response;
  }

  public Resource getAvailableResources() {
    return availableResources;
  }


  long retrystartTime;
  long retryInterval = 3000;
  private List<Container> getResources() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    AMResponse response;

    /*
    * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
    * milliseconds before aborting. During this interval, AM will still try
    * to contact the RM.
    */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new QueryEvent(query.getId(),
            QueryEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " +
            retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }

    if (response.getReboot()) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new QueryEvent(query.getId(),
          QueryEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: " +
          context.getApplicationId());
    }

    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    List<Container> newContainers = response.getAllocatedContainers();

    return newContainers;
  }
}
