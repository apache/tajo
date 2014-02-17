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
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.query.QueryUnitRequest;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.master.querymaster.QueryUnitAttempt;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.NetUtils;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class DefaultTaskScheduler extends AbstractTaskScheduler {
  private static final Log LOG = LogFactory.getLog(DefaultTaskScheduler.class);

  private final TaskSchedulerContext context;
  private SubQuery subQuery;

  private Thread schedulingThread;
  private AtomicBoolean stopEventHandling = new AtomicBoolean(false);

  private ScheduledRequests scheduledRequests;
  private TaskRequests taskRequests;

  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int totalAssigned = 0;
  private int nextTaskId = 0;
  private int scheduledObjectNum = 0;

  public DefaultTaskScheduler(TaskSchedulerContext context, SubQuery subQuery) {
    super(DefaultTaskScheduler.class.getName());
    this.context = context;
    this.subQuery = subQuery;
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

    this.schedulingThread = new Thread() {
      public void run() {

        while(!stopEventHandling.get() && !Thread.currentThread().isInterrupted()) {
          schedule();
          try {
            synchronized (schedulingThread){
              schedulingThread.wait(100);
            }
          } catch (InterruptedException e) {
            break;
          }
        }
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
    if(stopEventHandling.getAndSet(true)){
      return;
    }

    if (schedulingThread != null) {
      synchronized (schedulingThread) {
        schedulingThread.notifyAll();
      }
    }

    // Return all of request callbacks instantly.
    for (TaskRequestEvent req : taskRequests.taskRequestQueue) {
      req.getCallback().run(stopTaskRunnerReq);
    }

    LOG.info("Task Scheduler stopped");
    super.stop();
  }

  private FileFragment[] fragmentsForNonLeafTask;

  List<TaskRequestEvent> taskRequestEvents = new ArrayList<TaskRequestEvent>();
  public void schedule() {

    if (taskRequests.size() > 0) {
      if (scheduledRequests.leafTaskNum() > 0) {
        LOG.debug("Try to schedule tasks with taskRequestEvents: " +
            taskRequests.size() + ", LeafTask Schedule Request: " +
            scheduledRequests.leafTaskNum());
        taskRequests.getTaskRequests(taskRequestEvents,
            scheduledRequests.leafTaskNum());
        LOG.debug("Get " + taskRequestEvents.size() + " taskRequestEvents ");
        if (taskRequestEvents.size() > 0) {
          scheduledRequests.assignToLeafTasks(taskRequestEvents);
          taskRequestEvents.clear();
        }
      }
    }

    if (taskRequests.size() > 0) {
      if (scheduledRequests.nonLeafTaskNum() > 0) {
        LOG.debug("Try to schedule tasks with taskRequestEvents: " +
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
    if (event.getType() == EventType.T_SCHEDULE) {
      if (event instanceof FragmentScheduleEvent) {
        FragmentScheduleEvent castEvent = (FragmentScheduleEvent) event;
        if (context.isLeafQuery()) {
          QueryUnitAttemptScheduleContext queryUnitContext = new QueryUnitAttemptScheduleContext();
          QueryUnit task = SubQuery.newEmptyQueryUnit(context, queryUnitContext, subQuery, nextTaskId++);
          task.setFragment(castEvent.getLeftFragment());
          scheduledObjectNum++;
          if (castEvent.getRightFragment() != null) {
            task.setFragment(castEvent.getRightFragment());
            scheduledObjectNum++;
          }
          subQuery.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
        } else {
          fragmentsForNonLeafTask = new FileFragment[2];
          fragmentsForNonLeafTask[0] = castEvent.getLeftFragment();
          fragmentsForNonLeafTask[1] = castEvent.getRightFragment();
        }
      } else if (event instanceof FetchScheduleEvent) {
        FetchScheduleEvent castEvent = (FetchScheduleEvent) event;
        Map<String, List<URI>> fetches = castEvent.getFetches();
        QueryUnitAttemptScheduleContext queryUnitContext = new QueryUnitAttemptScheduleContext();
        QueryUnit task = SubQuery.newEmptyQueryUnit(context, queryUnitContext, subQuery, nextTaskId++);
        scheduledObjectNum++;
        for (Entry<String, List<URI>> eachFetch : fetches.entrySet()) {
          task.addFetches(eachFetch.getKey(), eachFetch.getValue());
          task.setFragment(fragmentsForNonLeafTask[0]);
          if (fragmentsForNonLeafTask[1] != null) {
            task.setFragment(fragmentsForNonLeafTask[1]);
          }
        }
        subQuery.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
      } else if (event instanceof QueryUnitAttemptScheduleEvent) {
        QueryUnitAttemptScheduleEvent castEvent = (QueryUnitAttemptScheduleEvent) event;
        if (context.isLeafQuery()) {
          scheduledRequests.addLeafTask(castEvent);
        } else {
          scheduledRequests.addNonLeafTask(castEvent);
        }
      }
    }
  }

  @Override
  public void handleTaskRequestEvent(TaskRequestEvent event) {

    taskRequests.handle(event);
    int hosts = scheduledRequests.leafTaskHostMapping.size();

    // if available cluster resource are large then tasks, the scheduler thread are working immediately.
    if(remainingScheduledObjectNum() > 0 &&
        (remainingScheduledObjectNum() <= hosts || hosts / 2 < taskRequests.size())){
      synchronized (schedulingThread){
        schedulingThread.notifyAll();
      }
    }
  }

  @Override
  public int remainingScheduledObjectNum() {
    return scheduledObjectNum;
  }

  private class TaskRequests implements EventHandler<TaskRequestEvent> {
    private final LinkedBlockingQueue<TaskRequestEvent> taskRequestQueue =
        new LinkedBlockingQueue<TaskRequestEvent>();

    @Override
    public void handle(TaskRequestEvent event) {
      LOG.info("TaskRequest: " + event.getContainerId() + "," + event.getExecutionBlockId());
      if(stopEventHandling.get()) {
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
        LOG.info("Assigned host : " + host + " Volume : " + volumeEntry.getKey() + ", Concurrency : "
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
    private final Set<QueryUnitAttemptId> leafTasks = Collections.synchronizedSet(new HashSet<QueryUnitAttemptId>());
    private final Set<QueryUnitAttemptId> nonLeafTasks = Collections.synchronizedSet(new HashSet<QueryUnitAttemptId>());
    private Map<String, TaskBlockLocation> leafTaskHostMapping = new HashMap<String, TaskBlockLocation>();
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksRackMapping =
        new HashMap<String, LinkedList<QueryUnitAttemptId>>();

    private void addLeafTask(QueryUnitAttemptScheduleEvent event) {
      QueryUnitAttempt queryUnitAttempt = event.getQueryUnitAttempt();
      DataLocation[] locations = queryUnitAttempt.getQueryUnit().getDataLocations();

      for (DataLocation location : locations) {
        String host = location.getHost();

        TaskBlockLocation taskBlockLocation = leafTaskHostMapping.get(host);
        if (taskBlockLocation == null) {
          taskBlockLocation = new TaskBlockLocation(host);
          leafTaskHostMapping.put(host, taskBlockLocation);
        }
        taskBlockLocation.addQueryUnitAttemptId(location.getVolumeId(), queryUnitAttempt.getId());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to host " + host);
        }

        String rack = RackResolver.resolve(host).getNetworkLocation();
        LinkedList<QueryUnitAttemptId> list = leafTasksRackMapping.get(rack);
        if (list == null) {
          list = new LinkedList<QueryUnitAttemptId>();
          leafTasksRackMapping.put(rack, list);
        }
        list.add(queryUnitAttempt.getId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to rack " + rack);
        }
      }

      leafTasks.add(queryUnitAttempt.getId());
    }

    private void addNonLeafTask(QueryUnitAttemptScheduleEvent event) {
      nonLeafTasks.add(event.getQueryUnitAttempt().getId());
    }

    public int leafTaskNum() {
      return leafTasks.size();
    }

    public int nonLeafTaskNum() {
      return nonLeafTasks.size();
    }

    public Set<QueryUnitAttemptId> assignedRequest = new HashSet<QueryUnitAttemptId>();

    public void assignToLeafTasks(List<TaskRequestEvent> taskRequests) {
      Collections.shuffle(taskRequests);
      Iterator<TaskRequestEvent> it = taskRequests.iterator();

      TaskRequestEvent taskRequest;
      while (it.hasNext() && leafTasks.size() > 0) {
        taskRequest = it.next();
        LOG.debug("assignToLeafTasks: " + taskRequest.getExecutionBlockId() + "," +
            "containerId=" + taskRequest.getContainerId());
        ContainerProxy container = context.getMasterContext().getResourceAllocator()
            .getContainer(taskRequest.getContainerId());
        if(container == null) {
          continue;
        }
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
            totalAssigned++;
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

              rackLocalAssigned++;
              totalAssigned++;

              LOG.info(String.format("Assigned Local/Rack/Total: (%d/%d/%d), Locality: %.2f%%, Rack host: %s",
                  hostLocalAssigned, rackLocalAssigned, totalAssigned,
                  ((double) hostLocalAssigned / (double) totalAssigned) * 100, host));
              break;
            }
          }

          // random allocation
          if (attemptId == null && leafTaskNum() > 0) {
            synchronized (leafTasks){
              attemptId = leafTasks.iterator().next();
              leafTasks.remove(attemptId);
            }
            //LOG.info(attemptId + " Assigned based on * match");
          }
        }

        if (attemptId != null) {
          QueryUnit task = subQuery.getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              new ArrayList<FragmentProto>(task.getAllFragments()),
              "",
              false,
              task.getLogicalPlan().toJson(),
              context.getMasterContext().getQueryContext(),
              subQuery.getDataChannel(), subQuery.getBlock().getEnforcer());
          if (checkIfInterQuery(subQuery.getMasterPlan(), subQuery.getBlock())) {
            taskAssign.setInterQuery();
          }

          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(),
              host, container.getTaskPort()));
          assignedRequest.add(attemptId);

          scheduledObjectNum -= task.getAllFragments().size();
          taskRequest.getCallback().run(taskAssign.getProto());
        } else {
          throw new RuntimeException("Illegal State!!!!!!!!!!!!!!!!!!!!!");
        }
      }
    }

    private boolean checkIfInterQuery(MasterPlan masterPlan, ExecutionBlock block) {
      if (masterPlan.isRoot(block)) {
        return false;
      }

      ExecutionBlock parent = masterPlan.getParent(block);
      if (masterPlan.isRoot(parent) && parent.hasUnion()) {
        return false;
      }

      return true;
    }

    public void assignToNonLeafTasks(List<TaskRequestEvent> taskRequests) {
      Iterator<TaskRequestEvent> it = taskRequests.iterator();

      TaskRequestEvent taskRequest;
      while (it.hasNext()) {
        taskRequest = it.next();
        LOG.debug("assignToNonLeafTasks: " + taskRequest.getExecutionBlockId());

        QueryUnitAttemptId attemptId;
        // random allocation
        if (nonLeafTasks.size() > 0) {
          synchronized (nonLeafTasks){
            attemptId = nonLeafTasks.iterator().next();
            nonLeafTasks.remove(attemptId);
          }
          LOG.debug("Assigned based on * match");

          QueryUnit task;
          task = subQuery.getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              Lists.newArrayList(task.getAllFragments()),
              "",
              false,
              task.getLogicalPlan().toJson(),
              context.getMasterContext().getQueryContext(),
              subQuery.getDataChannel(),
              subQuery.getBlock().getEnforcer());
          if (checkIfInterQuery(subQuery.getMasterPlan(), subQuery.getBlock())) {
            taskAssign.setInterQuery();
          }
          for (ScanNode scan : task.getScanNodes()) {
            Collection<URI> fetches = task.getFetch(scan);
            if (fetches != null) {
              for (URI fetch : fetches) {
                taskAssign.addFetch(scan.getTableName(), fetch);
              }
            }
          }

          ContainerProxy container = context.getMasterContext().getResourceAllocator().getContainer(
              taskRequest.getContainerId());
          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(), container.getTaskHostName(), container.getTaskPort()));
          taskRequest.getCallback().run(taskAssign.getProto());
          totalAssigned++;
          scheduledObjectNum--;
        }
      }
    }
  }
}
