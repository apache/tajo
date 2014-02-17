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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryUnitRequest;
import org.apache.tajo.engine.query.QueryUnitRequestImpl;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.master.querymaster.QueryUnitAttempt;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.util.NetUtils;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class LazyTaskScheduler extends AbstractTaskScheduler {
  private static final Log LOG = LogFactory.getLog(LazyTaskScheduler.class);

  private final TaskSchedulerContext context;
  private final SubQuery subQuery;

  private Thread schedulingThread;
  private volatile boolean stopEventHandling;

  BlockingQueue<TaskSchedulerEvent> eventQueue
      = new LinkedBlockingQueue<TaskSchedulerEvent>();

  private TaskRequests taskRequests;
  private FragmentScheduleAlgorithm scheduledFragments;
  private ScheduledFetches scheduledFetches;

  private int diskLocalAssigned = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int totalAssigned = 0;

  private int nextTaskId = 0;
  private int containerNum;

  public LazyTaskScheduler(TaskSchedulerContext context, SubQuery subQuery) {
    super(LazyTaskScheduler.class.getName());
    this.context = context;
    this.subQuery = subQuery;
  }

  @Override
  public void init(Configuration conf) {
    taskRequests  = new TaskRequests();
    try {
      scheduledFragments = FragmentScheduleAlgorithmFactory.get(conf);
      LOG.info(scheduledFragments.getClass().getSimpleName() + " is selected for the scheduling algorithm.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (!context.isLeafQuery()) {
      scheduledFetches = new ScheduledFetches();
    }

    super.init(conf);
  }

  @Override
  public void start() {
    containerNum = subQuery.getContext().getResourceAllocator().calculateNumRequestContainers(
        subQuery.getContext().getQueryMasterContext().getWorkerContext(),
        context.getEstimatedTaskNum(), 512);

    LOG.info("Start TaskScheduler");
    this.schedulingThread = new Thread() {
      public void run() {

        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            break;
          }

          schedule();
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
    stopEventHandling = true;
    schedulingThread.interrupt();

    // Return all of request callbacks instantly.
    for (TaskRequestEvent req : taskRequests.taskRequestQueue) {
      req.getCallback().run(stopTaskRunnerReq);
    }

    LOG.info("Task Scheduler stopped");
    super.stop();
  }

  List<TaskRequestEvent> taskRequestEvents = new ArrayList<TaskRequestEvent>();
  public void schedule() {
    if (taskRequests.size() > 0) {
      if (context.isLeafQuery()) {
        LOG.debug("Try to schedule tasks with taskRequestEvents: " +
            taskRequests.size() + ", Fragment Schedule Request: " +
            scheduledFragments.size());
        taskRequests.getTaskRequests(taskRequestEvents,
            scheduledFragments.size());
        LOG.debug("Get " + taskRequestEvents.size() + " taskRequestEvents ");
        if (taskRequestEvents.size() > 0) {
          assignLeafTasks(taskRequestEvents);
        }
        taskRequestEvents.clear();
      } else {
        LOG.debug("Try to schedule tasks with taskRequestEvents: " +
            taskRequests.size() + ", Fetch Schedule Request: " +
            scheduledFetches.size());
        taskRequests.getTaskRequests(taskRequestEvents,
            scheduledFetches.size());
        LOG.debug("Get " + taskRequestEvents.size() + " taskRequestEvents ");
        if (taskRequestEvents.size() > 0) {
          assignNonLeafTasks(taskRequestEvents);
        }
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

    if (event.getType() == EventType.T_SCHEDULE) {
      if (event instanceof FragmentScheduleEvent) {
        FragmentScheduleEvent castEvent = (FragmentScheduleEvent) event;
        scheduledFragments.addFragment(new FragmentPair(castEvent.getLeftFragment(), castEvent.getRightFragment()));
        initDiskBalancer(castEvent.getLeftFragment().getHosts(), castEvent.getLeftFragment().getDiskIds());
      } else if (event instanceof FetchScheduleEvent) {
        FetchScheduleEvent castEvent = (FetchScheduleEvent) event;
        scheduledFetches.addFetch(castEvent.getFetches());
      } else if (event instanceof QueryUnitAttemptScheduleEvent) {
        QueryUnitAttemptScheduleEvent castEvent = (QueryUnitAttemptScheduleEvent) event;
        assignTask(castEvent.getContext(), castEvent.getQueryUnitAttempt());
      }
    }
  }

  public void handleTaskRequestEvent(TaskRequestEvent event) {
    taskRequests.handle(event);
  }

  @Override
  public int remainingScheduledObjectNum() {
    if (context.isLeafQuery()) {
      return scheduledFragments.size();
    } else {
      return scheduledFetches.size();
    }
  }

  private Map<String, DiskBalancer> hostDiskBalancerMap = new HashMap<String, DiskBalancer>();

  private void initDiskBalancer(String[] hosts, int[] diskIds) {
    for (int i = 0; i < hosts.length; i++) {
      DiskBalancer diskBalancer;
      String normalized = NetUtils.normalizeHost(hosts[i]);
      if (hostDiskBalancerMap.containsKey(normalized)) {
        diskBalancer = hostDiskBalancerMap.get(normalized);
      } else {
        diskBalancer = new DiskBalancer(normalized);
        hostDiskBalancerMap.put(normalized, diskBalancer);
      }
      diskBalancer.addDiskId(diskIds[i]);
    }
  }

  private static class DiskBalancer {
    private HashMap<ContainerId, Integer> containerDiskMap = new HashMap<ContainerId, Integer>();
    private HashMap<Integer, Integer> diskReferMap = new HashMap<Integer, Integer>();
    private String host;

    public DiskBalancer(String host){
      this.host = host;
    }

    public void addDiskId(Integer diskId) {
      if (!diskReferMap.containsKey(diskId)) {
        diskReferMap.put(diskId, 0);
      }
    }

    public Integer getDiskId(ContainerId containerId) {
      if (!containerDiskMap.containsKey(containerId)) {
        assignVolumeId(containerId);
      }

      return containerDiskMap.get(containerId);
    }

    public void assignVolumeId(ContainerId containerId){
      Map.Entry<Integer, Integer> volumeEntry = null;

      for (Map.Entry<Integer, Integer> entry : diskReferMap.entrySet()) {
        if(volumeEntry == null) volumeEntry = entry;

        if (volumeEntry.getValue() >= entry.getValue()) {
          volumeEntry = entry;
        }
      }

      if(volumeEntry != null){
        diskReferMap.put(volumeEntry.getKey(), volumeEntry.getValue() + 1);
        LOG.info("Assigned host : " + host + " Volume : " + volumeEntry.getKey() + ", Concurrency : "
            + diskReferMap.get(volumeEntry.getKey()));
        containerDiskMap.put(containerId, volumeEntry.getKey());
      }
    }

    public String getHost() {
      return host;
    }
  }

  private class TaskRequests implements EventHandler<TaskRequestEvent> {
    private final LinkedBlockingQueue<TaskRequestEvent> taskRequestQueue =
        new LinkedBlockingQueue<TaskRequestEvent>();

    @Override
    public void handle(TaskRequestEvent event) {
      LOG.info("TaskRequest: " + event.getContainerId() + "," + event.getExecutionBlockId());
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

  private long adjustTaskSize() {
    long originTaskSize = context.getMasterContext().getConf().getIntVar(ConfVars.TASK_DEFAULT_SIZE) * 1024 * 1024;
    long fragNumPerTask = context.getTaskSize() / originTaskSize;
    if (fragNumPerTask * containerNum > remainingScheduledObjectNum()) {
      return context.getTaskSize();
    } else {
      fragNumPerTask = (long) Math.ceil((double)remainingScheduledObjectNum() / (double)containerNum);
      return originTaskSize * fragNumPerTask;
    }
  }

  private void assignLeafTasks(List<TaskRequestEvent> taskRequests) {
    Collections.shuffle(taskRequests);
    Iterator<TaskRequestEvent> it = taskRequests.iterator();

    TaskRequestEvent taskRequest;
    while (it.hasNext() && scheduledFragments.size() > 0) {
      taskRequest = it.next();
      LOG.debug("assignToLeafTasks: " + taskRequest.getExecutionBlockId() + "," +
          "containerId=" + taskRequest.getContainerId());
      ContainerProxy container = context.getMasterContext().getResourceAllocator().
          getContainer(taskRequest.getContainerId());

      if(container == null) {
        continue;
      }

      String host = container.getTaskHostName();
      QueryUnitAttemptScheduleContext queryUnitContext = new QueryUnitAttemptScheduleContext(container.containerID,
          host, taskRequest.getCallback());
      QueryUnit task = SubQuery.newEmptyQueryUnit(context, queryUnitContext, subQuery, nextTaskId++);

      FragmentPair fragmentPair;
      List<FragmentPair> fragmentPairs = new ArrayList<FragmentPair>();
      boolean diskLocal = false;
      long assignedFragmentSize = 0;
      long taskSize = adjustTaskSize();
      LOG.info("Adjusted task size: " + taskSize);

      // host local, disk local
      String normalized = NetUtils.normalizeHost(host);
      Integer diskId = hostDiskBalancerMap.get(normalized).getDiskId(container.containerID);
      if (diskId != null && diskId != -1) {
        do {
          fragmentPair = scheduledFragments.getHostLocalFragment(host, diskId);
          if (fragmentPair == null || fragmentPair.getLeftFragment() == null) {
            break;
          }

          if (assignedFragmentSize + fragmentPair.getLeftFragment().getEndKey() > taskSize) {
            break;
          } else {
            fragmentPairs.add(fragmentPair);
            assignedFragmentSize += fragmentPair.getLeftFragment().getEndKey();
            if (fragmentPair.getRightFragment() != null) {
              assignedFragmentSize += fragmentPair.getRightFragment().getEndKey();
            }
          }
          scheduledFragments.removeFragment(fragmentPair);
          diskLocal = true;
        } while (scheduledFragments.size() > 0 && assignedFragmentSize < taskSize);
      }

      if (assignedFragmentSize < taskSize) {
        // host local
        do {
          fragmentPair = scheduledFragments.getHostLocalFragment(host);
          if (fragmentPair == null || fragmentPair.getLeftFragment() == null) {
            break;
          }

          if (assignedFragmentSize + fragmentPair.getLeftFragment().getEndKey() > taskSize) {
            break;
          } else {
            fragmentPairs.add(fragmentPair);
            assignedFragmentSize += fragmentPair.getLeftFragment().getEndKey();
            if (fragmentPair.getRightFragment() != null) {
              assignedFragmentSize += fragmentPair.getRightFragment().getEndKey();
            }
          }
          scheduledFragments.removeFragment(fragmentPair);
          diskLocal = false;
        } while (scheduledFragments.size() > 0 && assignedFragmentSize < taskSize);
      }

      // rack local
      if (fragmentPairs.size() == 0) {
        fragmentPair = scheduledFragments.getRackLocalFragment(host);

        // random
        if (fragmentPair == null) {
          fragmentPair = scheduledFragments.getRandomFragment();
        } else {
          rackLocalAssigned++;
        }

        if (fragmentPair != null) {
          fragmentPairs.add(fragmentPair);
          scheduledFragments.removeFragment(fragmentPair);
        }
      } else {
        if (diskLocal) {
          diskLocalAssigned++;
        } else {
          hostLocalAssigned++;
        }
      }

      if (fragmentPairs.size() == 0) {
        throw new RuntimeException("Illegal State!!!!!!!!!!!!!!!!!!!!!");
      }

      LOG.info("host: " + host + " disk id: " + diskId + " fragment num: " + fragmentPairs.size());

      task.setFragment(fragmentPairs.toArray(new FragmentPair[fragmentPairs.size()]));
      subQuery.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
    }
  }

  private void assignNonLeafTasks(List<TaskRequestEvent> taskRequests) {
    Iterator<TaskRequestEvent> it = taskRequests.iterator();

    TaskRequestEvent taskRequest;
    while (it.hasNext()) {
      taskRequest = it.next();
      LOG.debug("assignToNonLeafTasks: " + taskRequest.getExecutionBlockId());

      // random allocation
      if (scheduledFetches.size() > 0) {
        LOG.debug("Assigned based on * match");
        ContainerProxy container = context.getMasterContext().getResourceAllocator().getContainer(
            taskRequest.getContainerId());
        QueryUnitAttemptScheduleContext queryUnitContext = new QueryUnitAttemptScheduleContext(container.containerID,
            container.getTaskHostName(), taskRequest.getCallback());
        QueryUnit task = SubQuery.newEmptyQueryUnit(context, queryUnitContext, subQuery, nextTaskId++);
        task.setFragment(scheduledFragments.getAllFragments());
        subQuery.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
      }
    }
  }

  private void assignTask(QueryUnitAttemptScheduleContext attemptContext, QueryUnitAttempt taskAttempt) {
    QueryUnitAttemptId attemptId = taskAttempt.getId();
    ContainerProxy containerProxy = context.getMasterContext().getResourceAllocator().
        getContainer(attemptContext.getContainerId());
    QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
        attemptId,
        new ArrayList<FragmentProto>(taskAttempt.getQueryUnit().getAllFragments()),
        "",
        false,
        taskAttempt.getQueryUnit().getLogicalPlan().toJson(),
        context.getMasterContext().getQueryContext(),
        subQuery.getDataChannel(), subQuery.getBlock().getEnforcer());
    if (checkIfInterQuery(subQuery.getMasterPlan(), subQuery.getBlock())) {
      taskAssign.setInterQuery();
    }

    if (!context.isLeafQuery()) {
      Map<String, List<URI>> fetch = scheduledFetches.getNextFetch();
      scheduledFetches.popNextFetch();

      for (Entry<String, List<URI>> fetchEntry : fetch.entrySet()) {
        for (URI eachValue : fetchEntry.getValue()) {
          taskAssign.addFetch(fetchEntry.getKey(), eachValue);
        }
      }
    }

    context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
        attemptContext.getContainerId(), attemptContext.getHost(), containerProxy.getTaskPort()));

    totalAssigned++;
    attemptContext.getCallback().run(taskAssign.getProto());

    if (context.isLeafQuery()) {
      LOG.debug("DiskLocalAssigned / Total: " + diskLocalAssigned + " / " + totalAssigned);
      LOG.debug("HostLocalAssigned / Total: " + hostLocalAssigned + " / " + totalAssigned);
      LOG.debug("RackLocalAssigned: " + rackLocalAssigned + " / " + totalAssigned);
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
}