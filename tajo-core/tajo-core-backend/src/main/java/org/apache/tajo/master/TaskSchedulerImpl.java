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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.protocolrecords.QueryUnitRequest;
import org.apache.tajo.master.event.TaskAttemptAssignedEvent;
import org.apache.tajo.master.event.TaskRequestEvent;
import org.apache.tajo.master.event.TaskScheduleEvent;
import org.apache.tajo.master.event.TaskSchedulerEvent;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.util.NetUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskSchedulerImpl extends AbstractService
    implements TaskScheduler {
  private static final Log LOG = LogFactory.getLog(TaskScheduleEvent.class);

  private final QueryMasterTask.QueryContext context;
  private TajoAsyncDispatcher dispatcher;

  private Thread eventHandlingThread;
  private Thread schedulingThread;
  private volatile boolean stopEventHandling;

  BlockingQueue<TaskSchedulerEvent> eventQueue
      = new LinkedBlockingQueue<TaskSchedulerEvent>();

  private ScheduledRequests scheduledRequests;
  private TaskRequests taskRequests;

  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int totalAssigned = 0;

  public TaskSchedulerImpl(QueryMasterTask.QueryContext context) {
    super(TaskSchedulerImpl.class.getName());
    this.context = context;
    this.dispatcher = context.getDispatcher();
  }

  @Override
  public void init(Configuration conf) {

    scheduledRequests = new ScheduledRequests();
    taskRequests  = new TaskRequests();

    super.init(conf);
  }

  @Override
  public void start() {
    LOG.info("Start TaskScheduler");
    this.eventHandlingThread = new Thread() {
      public void run() {

        TaskSchedulerEvent event;
        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
            handleEvent(event);
          } catch (InterruptedException e) {
            //LOG.error("Returning, iterrupted : " + e);
            break;
          }
        }
        LOG.info("TaskScheduler eventHandlingThread stopped");
      }
    };

    this.eventHandlingThread.start();

    this.schedulingThread = new Thread() {
      public void run() {

        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            break;
          }

          schedule();
        }
        //req.getCallback().run(stopTaskRunnerReq);
        LOG.info("TaskScheduler schedulingThread stopped");
      }
    };

    this.schedulingThread.start();
    super.start();
  }

  private static final QueryUnitAttemptId NULL_ATTEMPT_ID;
  public static final TajoWorkerProtocol.QueryUnitRequestProto stopTaskRunnerReq;
  static {
    ExecutionBlockId nullSubQuery = QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0);
    NULL_ATTEMPT_ID = QueryIdFactory.newQueryUnitAttemptId(QueryIdFactory.newQueryUnitId(nullSubQuery, 0), 0);

    TajoWorkerProtocol.QueryUnitRequestProto.Builder builder =
        TajoWorkerProtocol.QueryUnitRequestProto.newBuilder();
    builder.setId(NULL_ATTEMPT_ID.getProto());
    builder.setShouldDie(true);
    builder.setOutputTable("");
    builder.setSerializedData("");
    builder.setClusteredOutput(false);
    stopTaskRunnerReq = builder.build();
  }

  @Override
  public void stop() {
    stopEventHandling = true;
    eventHandlingThread.interrupt();
    schedulingThread.interrupt();

    // Return all of request callbacks instantly.
    for (TaskRequestEvent req : taskRequests.taskRequestQueue) {
      req.getCallback().run(stopTaskRunnerReq);
    }

    LOG.info("Task Scheduler stopped");
    super.stop();
  }

  private void handleEvent(TaskSchedulerEvent event) {
    if (event.getType() == EventType.T_SCHEDULE) {
      TaskScheduleEvent castEvent = (TaskScheduleEvent) event;
      if (castEvent.isLeafQuery()) {
        scheduledRequests.addLeafTask(castEvent);
      } else {
        scheduledRequests.addNonLeafTask(castEvent);
      }
    }
  }

  List<TaskRequestEvent> taskRequestEvents = new ArrayList<TaskRequestEvent>();
  public void schedule() {

    if (taskRequests.size() > 0) {
      if (scheduledRequests.leafTaskNum() > 0) {
        LOG.info("Try to schedule tasks with taskRequestEvents: " +
            taskRequests.size() + ", LeafTask Schedule Request: " +
            scheduledRequests.leafTaskNum());
        taskRequests.getTaskRequests(taskRequestEvents,
            scheduledRequests.leafTaskNum());
        LOG.info("Get " + taskRequestEvents.size() + " taskRequestEvents ");
        if (taskRequestEvents.size() > 0) {
          scheduledRequests.assignToLeafTasks(taskRequestEvents);
          taskRequestEvents.clear();
        }
      }
    }

    if (taskRequests.size() > 0) {
      if (scheduledRequests.nonLeafTaskNum() > 0) {
        LOG.info("Try to schedule tasks with taskRequestEvents: " +
            taskRequests.size() + ", NonLeafTask Schedule Request: " +
            scheduledRequests.nonLeafTaskNum());
        taskRequests.getTaskRequests(taskRequestEvents,
            scheduledRequests.nonLeafTaskNum());
        scheduledRequests.assignToNonLeafTasks(taskRequestEvents);
        taskRequestEvents.clear();
      }
    }
  }

  @Override
  public void handle(TaskSchedulerEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in YarnRMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of YarnRMContainerAllocator: " + remCapacity);
    }

    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new InternalError(e.getMessage());
    }
  }

  public void handleTaskRequestEvent(TaskRequestEvent event) {
    taskRequests.handle(event);
  }

  private class TaskRequests implements EventHandler<TaskRequestEvent> {
    private final LinkedBlockingQueue<TaskRequestEvent> taskRequestQueue =
        new LinkedBlockingQueue<TaskRequestEvent>();

    @Override
    public void handle(TaskRequestEvent event) {
      LOG.info("====>TaskRequest:" + event.getContainerId() + "," + event.getExecutionBlockId());
      if(stopEventHandling) {
        event.getCallback().run(stopTaskRunnerReq);
        return;
      }
      int qSize = taskRequestQueue.size();
      if (qSize != 0 && qSize % 1000 == 0) {
        LOG.info("Size of event-queue in YarnRMContainerAllocator is " + qSize);
      }
      int remCapacity = taskRequestQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue "
            + "of YarnRMContainerAllocator: " + remCapacity);
      }

      taskRequestQueue.add(event);
    }

    public void getTaskRequests(final Collection<TaskRequestEvent> taskRequests,
                                int num) {
      taskRequestQueue.drainTo(taskRequests, num);
    }

    public int size() {
      return taskRequestQueue.size();
    }
  }

  public static class TaskBlockLocation {
    private HashMap<Integer, LinkedList<QueryUnitAttemptId>> unAssignedTaskMap =
        new HashMap<Integer, LinkedList<QueryUnitAttemptId>>();
    private HashMap<ContainerId, Integer> assignedContainerMap = new HashMap<ContainerId, Integer>();
    private TreeMap<Integer, Integer> volumeUsageMap = new TreeMap<Integer, Integer>();
    private String host;

    public TaskBlockLocation(String host){
      this.host = host;
    }

    public void addQueryUnitAttemptId(Integer volumeId, QueryUnitAttemptId attemptId){
      LinkedList<QueryUnitAttemptId> list = unAssignedTaskMap.get(volumeId);
      if (list == null) {
        list = new LinkedList<QueryUnitAttemptId>();
        unAssignedTaskMap.put(volumeId, list);
      }
      list.add(attemptId);

      if(!volumeUsageMap.containsKey(volumeId)) volumeUsageMap.put(volumeId, 0);
    }

    public LinkedList<QueryUnitAttemptId> getQueryUnitAttemptIdList(ContainerId containerId){
      Integer volumeId;

      if (!assignedContainerMap.containsKey(containerId)) {
        volumeId = assignVolumeId();
        assignedContainerMap.put(containerId, volumeId);
      } else {
        volumeId = assignedContainerMap.get(containerId);
      }

      LinkedList<QueryUnitAttemptId> list = null;
      if (unAssignedTaskMap.size() >  0) {
        int retry = unAssignedTaskMap.size();
        do {
          list = unAssignedTaskMap.get(volumeId);
          if (list == null || list.size() == 0) {
            //clean and reassign remaining volume
            unAssignedTaskMap.remove(volumeId);
            volumeUsageMap.remove(volumeId);
            if (volumeId < 0) break; //  processed all block on disk

            volumeId = assignVolumeId();
            assignedContainerMap.put(containerId, volumeId);
            retry--;
          } else {
            break;
          }
        } while (retry > 0);
      }
      return list;
    }

    public Integer assignVolumeId(){
      Map.Entry<Integer, Integer> volumeEntry = null;

      for (Map.Entry<Integer, Integer> entry : volumeUsageMap.entrySet()) {
        if(volumeEntry == null) volumeEntry = entry;

        if (volumeEntry.getValue() >= entry.getValue()) {
          volumeEntry = entry;
        }
      }

      if(volumeEntry != null){
        volumeUsageMap.put(volumeEntry.getKey(), volumeEntry.getValue() + 1);
        LOG.info("Assigned host : " + host + " Volume : " + volumeEntry.getKey() + ", concurrency : "
            + volumeUsageMap.get(volumeEntry.getKey()));
        return volumeEntry.getKey();
      } else {
         return -1;  // processed all block on disk
      }
    }

    public String getHost() {
      return host;
    }
  }

  private class ScheduledRequests {
    private final HashSet<QueryUnitAttemptId> leafTasks = new HashSet<QueryUnitAttemptId>();
    private final HashSet<QueryUnitAttemptId> nonLeafTasks = new HashSet<QueryUnitAttemptId>();
    private Map<String, TaskBlockLocation> leafTaskHostMapping = new HashMap<String, TaskBlockLocation>();
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksRackMapping =
        new HashMap<String, LinkedList<QueryUnitAttemptId>>();

    public void addLeafTask(TaskScheduleEvent event) {
      List<QueryUnit.DataLocation> locations = event.getDataLocations();

      for (QueryUnit.DataLocation location : locations) {
        String host = location.getHost();

        TaskBlockLocation taskBlockLocation = leafTaskHostMapping.get(host);
        if (taskBlockLocation == null) {
          taskBlockLocation = new TaskBlockLocation(host);
          leafTaskHostMapping.put(host, taskBlockLocation);
        }
        taskBlockLocation.addQueryUnitAttemptId(location.getVolumeId(), event.getAttemptId());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to host " + host);
        }
      }
      for (String rack : event.getRacks()) {
        LinkedList<QueryUnitAttemptId> list = leafTasksRackMapping.get(rack);
        if (list == null) {
          list = new LinkedList<QueryUnitAttemptId>();
          leafTasksRackMapping.put(rack, list);
        }
        list.add(event.getAttemptId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to rack " + rack);
        }
      }

      leafTasks.add(event.getAttemptId());
    }

    public void addNonLeafTask(TaskScheduleEvent event) {
      nonLeafTasks.add(event.getAttemptId());
    }

    public int leafTaskNum() {
      return leafTasks.size();
    }

    public int nonLeafTaskNum() {
      return nonLeafTasks.size();
    }

    public Set<QueryUnitAttemptId> assignedRequest = new HashSet<QueryUnitAttemptId>();

    public void assignToLeafTasks(List<TaskRequestEvent> taskRequests) {
      Iterator<TaskRequestEvent> it = taskRequests.iterator();

      TaskRequestEvent taskRequest;
      while (it.hasNext() && leafTasks.size() > 0) {
        taskRequest = it.next();
        LOG.info("====> assignToLeafTasks: " + taskRequest.getExecutionBlockId());
        ContainerProxy container = context.getResourceAllocator().getContainer(taskRequest.getContainerId());
        String host = container.getTaskHostName();

        QueryUnitAttemptId attemptId = null;
        LinkedList<QueryUnitAttemptId> list = null;

        // local disk allocation
        if(!leafTaskHostMapping.containsKey(host)){
          host = NetUtils.normalizeHost(host);
        }

        TaskBlockLocation taskBlockLocation = leafTaskHostMapping.get(host);
        if (taskBlockLocation != null) {
          list = taskBlockLocation.getQueryUnitAttemptIdList(taskRequest.getContainerId());
        }

        while (list != null && list.size() > 0) {
          QueryUnitAttemptId tId = list.removeFirst();

          if (leafTasks.contains(tId)) {
            leafTasks.remove(tId);
            attemptId = tId;
            //LOG.info(attemptId + " Assigned based on host match " + hostName);
            hostLocalAssigned++;
            break;
          }
        }

        // rack allocation
        if (attemptId == null) {
          String rack = RackResolver.resolve(host).getNetworkLocation();
          list = leafTasksRackMapping.get(rack);
          while(list != null && list.size() > 0) {

            QueryUnitAttemptId tId = list.removeFirst();

            if (leafTasks.contains(tId)) {
              leafTasks.remove(tId);
              attemptId = tId;
              //LOG.info(attemptId + "Assigned based on rack match " + rack);
              rackLocalAssigned++;
              break;
            }
          }

          // random allocation
          if (attemptId == null && leafTaskNum() > 0) {
            attemptId = leafTasks.iterator().next();
            leafTasks.remove(attemptId);
            //LOG.info(attemptId + " Assigned based on * match");
          }
        }

        if (attemptId != null) {
          QueryUnit task = context.getQuery()
              .getSubQuery(attemptId.getQueryUnitId().getExecutionBlockId()).getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              new ArrayList<Fragment>(task.getAllFragments()),
              task.getOutputName(),
              false,
              task.getLogicalPlan().toJson());
          if (task.getStoreTableNode().isLocal()) {
            taskAssign.setInterQuery();
          }

          context.getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(),
              host, container.getTaskPort()));
          assignedRequest.add(attemptId);

          totalAssigned++;
          taskRequest.getCallback().run(taskAssign.getProto());
        } else {
          throw new RuntimeException("Illegal State!!!!!!!!!!!!!!!!!!!!!");
        }
      }

      LOG.info("HostLocalAssigned / Total: " + hostLocalAssigned + " / " + totalAssigned);
      LOG.info("RackLocalAssigned: " + rackLocalAssigned + " / " + totalAssigned);
    }

    public void assignToNonLeafTasks(List<TaskRequestEvent> taskRequests) {
      Iterator<TaskRequestEvent> it = taskRequests.iterator();

      TaskRequestEvent taskRequest;
      while (it.hasNext()) {
        taskRequest = it.next();
        LOG.info("====> assignToNonLeafTasks: " + taskRequest.getExecutionBlockId());

        QueryUnitAttemptId attemptId;
        // random allocation
        if (nonLeafTasks.size() > 0) {
          attemptId = nonLeafTasks.iterator().next();
          nonLeafTasks.remove(attemptId);
          LOG.debug("Assigned based on * match");

          QueryUnit task;
          task = context.getSubQuery(
              attemptId.getQueryUnitId().getExecutionBlockId()).getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              Lists.newArrayList(task.getAllFragments()),
              task.getOutputName(),
              false,
              task.getLogicalPlan().toJson());
          if (task.getStoreTableNode().isLocal()) {
            taskAssign.setInterQuery();
          }
          for (ScanNode scan : task.getScanNodes()) {
            Collection<URI> fetches = task.getFetch(scan);
            if (fetches != null) {
              for (URI fetch : fetches) {
                taskAssign.addFetch(scan.getTableId(), fetch);
              }
            }
          }

          ContainerProxy container = context.getResourceAllocator().getContainer(
              taskRequest.getContainerId());
          context.getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(), container.getTaskHostName(), container.getTaskPort()));
          taskRequest.getCallback().run(taskAssign.getProto());
        }
      }
    }
  }
}
