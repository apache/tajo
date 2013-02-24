/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;
import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;
import tajo.SubQueryId;
import tajo.engine.MasterWorkerProtos;
import tajo.engine.planner.logical.ScanNode;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.TaskRunnerLauncherImpl.Container;
import tajo.master.event.TaskAttemptAssignedEvent;
import tajo.master.event.TaskRequestEvent;
import tajo.master.event.TaskRequestEvent.TaskRequestEventType;
import tajo.master.event.TaskScheduleEvent;
import tajo.master.event.TaskSchedulerEvent;
import tajo.master.event.TaskSchedulerEvent.EventType;
import tajo.storage.Fragment;
import tajo.util.TajoIdUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskSchedulerImpl extends AbstractService
    implements TaskScheduler {
  private static final Log LOG = LogFactory.getLog(TaskScheduleEvent.class);

  private final QueryContext context;
  private AsyncDispatcher dispatcher;

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

  public TaskSchedulerImpl(QueryContext context) {
    super(TaskSchedulerImpl.class.getName());
    this.context = context;
    this.dispatcher = context.getDispatcher();
  }

  public void init(Configuration conf) {

    scheduledRequests = new ScheduledRequests();
    taskRequests  = new TaskRequests();
    dispatcher.register(TaskRequestEventType.class, taskRequests);

    super.init(conf);
  }

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
            LOG.error("Returning, iterrupted : " + e);
          }
        }
      }
    };

    this.eventHandlingThread.start();

    this.schedulingThread = new Thread() {
      public void run() {

        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn(e);
          }

          schedule();
        }
      }
    };

    this.schedulingThread.start();
    super.start();
  }

  private static final QueryUnitAttemptId NULL_ID;
  private static final MasterWorkerProtos.QueryUnitRequestProto stopTaskRunnerReq;
  static {
    SubQueryId nullSubQuery =
        QueryIdFactory.newSubQueryId(TajoIdUtils.NullQueryId);
    NULL_ID = QueryIdFactory.newQueryUnitAttemptId(QueryIdFactory.newQueryUnitId(nullSubQuery, 0), 0);

    MasterWorkerProtos.QueryUnitRequestProto.Builder builder =
                MasterWorkerProtos.QueryUnitRequestProto.newBuilder();
    builder.setId(NULL_ID.getProto());
    builder.setShouldDie(true);
    builder.setOutputTable("");
    builder.setSerializedData("");
    builder.setClusteredOutput(false);
    stopTaskRunnerReq = builder.build();
  }


  public void stop() {
    stopEventHandling = true;
    eventHandlingThread.interrupt();
    schedulingThread.interrupt();

    // Return all of request callbacks instantly.
    for (TaskRequestEvent req : taskRequests.taskRequestQueue) {
      req.getCallback().run(stopTaskRunnerReq);
    }

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
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }

    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new InternalError(e.getMessage());
    }
  }

  private class TaskRequests implements EventHandler<TaskRequestEvent> {
    private final LinkedBlockingQueue<TaskRequestEvent> taskRequestQueue =
        new LinkedBlockingQueue<TaskRequestEvent>();

    @Override
    public void handle(TaskRequestEvent event) {
      int qSize = taskRequestQueue.size();
      if (qSize != 0 && qSize % 1000 == 0) {
        LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
      }
      int remCapacity = taskRequestQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue "
            + "of RMContainerAllocator: " + remCapacity);
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

  private class ScheduledRequests {
    private final HashSet<QueryUnitAttemptId> leafTasks = new HashSet<QueryUnitAttemptId>();
    private final HashSet<QueryUnitAttemptId> nonLeafTasks = new HashSet<QueryUnitAttemptId>();
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksHostMapping =
        new HashMap<String, LinkedList<QueryUnitAttemptId>>();
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksRackMapping =
        new HashMap<String, LinkedList<QueryUnitAttemptId>>();

    public void addLeafTask(TaskScheduleEvent event) {
      for (String host : event.getHosts()) {
        LinkedList<QueryUnitAttemptId> list = leafTasksHostMapping.get(host);
        if (list == null) {
          list = new LinkedList<QueryUnitAttemptId>();
          leafTasksHostMapping.put(host, list);
        }
        list.add(event.getAttemptId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to host " + host);
        }
      }
      for (String rack: event.getRacks()) {
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

    public Set<QueryUnitAttemptId> AssignedRequest = new HashSet<QueryUnitAttemptId>();

    public void assignToLeafTasks(List<TaskRequestEvent> taskRequests) {
      Iterator<TaskRequestEvent> it = taskRequests.iterator();
      LOG.info("Got task requests " + taskRequests.size());

      TaskRequestEvent taskRequest;
      while (it.hasNext() && leafTasks.size() > 0) {
        taskRequest = it.next();
        Container container = context.getContainer(taskRequest.getContainerId());
        String hostName = container.getHostName();

        QueryUnitAttemptId attemptId = null;

        // local allocation
        LinkedList<QueryUnitAttemptId> list = leafTasksHostMapping.get(hostName);
        while(list != null && list.size() > 0) {

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
          String rack = RackResolver.resolve(hostName).getNetworkLocation();
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
              .getSubQuery(attemptId.getSubQueryId()).getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              new ArrayList<Fragment>(task.getAllFragments()),
              task.getOutputName(),
              false,
              task.getLogicalPlan().toJSON());
          if (task.getStoreTableNode().isLocal()) {
            taskAssign.setInterQuery();
          }

          context.getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(),
              container.getHostName(), container.getPullServerPort()));
          AssignedRequest.add(attemptId);

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

        QueryUnitAttemptId attemptId;
        // random allocation
        if (nonLeafTasks.size() > 0) {
          attemptId = nonLeafTasks.iterator().next();
          nonLeafTasks.remove(attemptId);
          LOG.debug("Assigned based on * match");

          QueryUnit task;
          task = context.getQuery()
              .getSubQuery(attemptId.getSubQueryId()).getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              Lists.newArrayList(task.getAllFragments()),
              task.getOutputName(),
              false,
              task.getLogicalPlan().toJSON());
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

          Container container = context.getContainer(
              taskRequest.getContainerId());
          context.getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(), container.getHostName(), container.getPullServerPort()));
          taskRequest.getCallback().run(taskAssign.getProto());
        }
      }
    }
  }
}
