/*
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

package org.apache.tajo.querymaster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.TaskRequest;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.FetchImpl;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class DefaultTaskScheduler extends AbstractTaskScheduler {
  private static final Log LOG = LogFactory.getLog(DefaultTaskScheduler.class);

  private final TaskSchedulerContext context;
  private Stage stage;

  private Thread schedulingThread;
  private AtomicBoolean stopEventHandling = new AtomicBoolean(false);

  private ScheduledRequests scheduledRequests;
  private TaskRequests taskRequests;

  private int nextTaskId = 0;
  private int scheduledObjectNum = 0;

  public DefaultTaskScheduler(TaskSchedulerContext context, Stage stage) {
    super(DefaultTaskScheduler.class.getName());
    this.context = context;
    this.stage = stage;
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
          try {
            synchronized (schedulingThread){
              schedulingThread.wait(100);
            }
            schedule();
          } catch (InterruptedException e) {
            break;
          } catch (Throwable e) {
            LOG.fatal(e.getMessage(), e);
            break;
          }
        }
        LOG.info("TaskScheduler schedulingThread stopped");
      }
    };

    this.schedulingThread.start();
    super.start();
  }

  private static final TaskAttemptId NULL_ATTEMPT_ID;
  public static final TajoWorkerProtocol.TaskRequestProto stopTaskRunnerReq;
  static {
    ExecutionBlockId nullStage = QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0);
    NULL_ATTEMPT_ID = QueryIdFactory.newTaskAttemptId(QueryIdFactory.newTaskId(nullStage, 0), 0);

    TajoWorkerProtocol.TaskRequestProto.Builder builder =
        TajoWorkerProtocol.TaskRequestProto.newBuilder();
    builder.setId(NULL_ATTEMPT_ID.getProto());
    builder.setShouldDie(true);
    builder.setOutputTable("");
    builder.setPlan(PlanProto.LogicalNodeTree.newBuilder());
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
    if(taskRequests != null){
      for (TaskRequestEvent req : taskRequests.taskRequestQueue) {
        req.getCallback().run(stopTaskRunnerReq);
      }
    }

    LOG.info("Task Scheduler stopped");
    super.stop();
  }

  private Fragment[] fragmentsForNonLeafTask;
  private Fragment[] broadcastFragmentsForNonLeafTask;

  LinkedList<TaskRequestEvent> taskRequestEvents = new LinkedList<TaskRequestEvent>();
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
          TaskAttemptScheduleContext taskContext = new TaskAttemptScheduleContext();
          Task task = Stage.newEmptyTask(context, taskContext, stage, nextTaskId++);
          task.addFragment(castEvent.getLeftFragment(), true);
          scheduledObjectNum++;
          if (castEvent.hasRightFragments()) {
            task.addFragments(castEvent.getRightFragments());
          }
          stage.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
        } else {
          fragmentsForNonLeafTask = new FileFragment[2];
          fragmentsForNonLeafTask[0] = castEvent.getLeftFragment();
          if (castEvent.hasRightFragments()) {
            FileFragment[] rightFragments = castEvent.getRightFragments().toArray(new FileFragment[]{});
            fragmentsForNonLeafTask[1] = rightFragments[0];
            if (rightFragments.length > 1) {
              broadcastFragmentsForNonLeafTask = new FileFragment[rightFragments.length - 1];
              System.arraycopy(rightFragments, 1, broadcastFragmentsForNonLeafTask, 0, broadcastFragmentsForNonLeafTask.length);
            } else {
              broadcastFragmentsForNonLeafTask = null;
            }
          }
        }
      } else if (event instanceof FetchScheduleEvent) {
        FetchScheduleEvent castEvent = (FetchScheduleEvent) event;
        Map<String, List<FetchImpl>> fetches = castEvent.getFetches();
        TaskAttemptScheduleContext taskScheduleContext = new TaskAttemptScheduleContext();
        Task task = Stage.newEmptyTask(context, taskScheduleContext, stage, nextTaskId++);
        scheduledObjectNum++;
        for (Entry<String, List<FetchImpl>> eachFetch : fetches.entrySet()) {
          task.addFetches(eachFetch.getKey(), eachFetch.getValue());
          task.addFragment(fragmentsForNonLeafTask[0], true);
          if (fragmentsForNonLeafTask[1] != null) {
            task.addFragment(fragmentsForNonLeafTask[1], true);
          }
        }
        if (broadcastFragmentsForNonLeafTask != null && broadcastFragmentsForNonLeafTask.length > 0) {
          task.addFragments(Arrays.asList(broadcastFragmentsForNonLeafTask));
        }
        stage.getEventHandler().handle(new TaskEvent(task.getId(), TaskEventType.T_SCHEDULE));
      } else if (event instanceof TaskAttemptToSchedulerEvent) {
        TaskAttemptToSchedulerEvent castEvent = (TaskAttemptToSchedulerEvent) event;
        if (context.isLeafQuery()) {
          scheduledRequests.addLeafTask(castEvent);
        } else {
          scheduledRequests.addNonLeafTask(castEvent);
        }
      }
    } else if (event.getType() == EventType.T_SCHEDULE_CANCEL) {
      // when a stage is killed, unassigned query unit attmpts are canceled from the scheduler.
      // This event is triggered by TaskAttempt.
      TaskAttemptToSchedulerEvent castedEvent = (TaskAttemptToSchedulerEvent) event;
      scheduledRequests.leafTasks.remove(castedEvent.getTaskAttempt().getId());
      LOG.info(castedEvent.getTaskAttempt().getId() + " is canceled from " + this.getClass().getSimpleName());
      ((TaskAttemptToSchedulerEvent) event).getTaskAttempt().handle(
          new TaskAttemptEvent(castedEvent.getTaskAttempt().getId(), TaskAttemptEventType.TA_SCHEDULE_CANCELED));
    }
  }

  @Override
  public void handleTaskRequestEvent(TaskRequestEvent event) {

    taskRequests.handle(event);
    int hosts = scheduledRequests.leafTaskHostMapping.size();

    // if available cluster resource are large then tasks, the scheduler thread are working immediately.
    if(remainingScheduledObjectNum() > 0 &&
        (remainingScheduledObjectNum() <= hosts || hosts <= taskRequests.size())){
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
      if(LOG.isDebugEnabled()){
        LOG.debug("TaskRequest: " + event.getContainerId() + "," + event.getExecutionBlockId());
      }

      if(stopEventHandling.get()) {
        event.getCallback().run(stopTaskRunnerReq);
        return;
      }
      int qSize = taskRequestQueue.size();
      if (qSize != 0 && qSize % 1000 == 0) {
        LOG.info("Size of event-queue in DefaultTaskScheduler is " + qSize);
      }
      int remCapacity = taskRequestQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue "
            + "of DefaultTaskScheduler: " + remCapacity);
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

  /**
   * One worker can have multiple running task runners. <code>HostVolumeMapping</code>
   * describes various information for one worker, including :
   * <ul>
   *  <li>host name</li>
   *  <li>rack name</li>
   *  <li>unassigned tasks for each disk volume</li>
   *  <li>last assigned volume id - it can be used for assigning task in a round-robin manner</li>
   *  <li>the number of running tasks for each volume</li>
   * </ul>, each task runner and the concurrency number of running tasks for volumes.
   *
   * Here, we identifier a task runner by {@link ContainerId}, and we use volume ids to identify
   * all disks in this node. Actually, each volume is only used to distinguish disks, and we don't
   * know a certain volume id indicates a certain disk. If you want to know volume id, please read the below section.
   *
   * <h3>Volume id</h3>
   * Volume id is an integer. Each volume id identifies each disk volume.
   *
   * This volume id can be obtained from org.apache.hadoop.fs.BlockStorageLocation#getVolumeIds()}.   *
   * HDFS cannot give any volume id due to unknown reason and disabled config 'dfs.client.file-block-locations.enabled'.
   * In this case, the volume id will be -1 or other native integer.
   *
   * <h3>See Also</h3>
   * <ul>
   *   <li>HDFS-3672 (https://issues.apache.org/jira/browse/HDFS-3672).</li>
   * </ul>
   */
  public class HostVolumeMapping {
    private final String host;
    private final String rack;
    /** A key is disk volume, and a value is a list of tasks to be scheduled. */
    private Map<Integer, LinkedHashSet<TaskAttempt>> unassignedTaskForEachVolume =
        Collections.synchronizedMap(new HashMap<Integer, LinkedHashSet<TaskAttempt>>());
    /** A value is last assigned volume id for each task runner */
    private HashMap<TajoContainerId, Integer> lastAssignedVolumeId = new HashMap<TajoContainerId,
      Integer>();
    /**
     * A key is disk volume id, and a value is the load of this volume.
     * This load is measured by counting how many number of tasks are running.
     *
     * These disk volumes are kept in an order of ascending order of the volume id.
     * In other words, the head volume ids are likely to -1, meaning no given volume id.
     */
    private SortedMap<Integer, Integer> diskVolumeLoads = new TreeMap<Integer, Integer>();
    /** The total number of remain tasks in this host */
    private AtomicInteger remainTasksNum = new AtomicInteger(0);
    public static final int REMOTE = -2;


    public HostVolumeMapping(String host, String rack){
      this.host = host;
      this.rack = rack;
    }

    public synchronized void addTaskAttempt(int volumeId, TaskAttempt attemptId){
      synchronized (unassignedTaskForEachVolume){
        LinkedHashSet<TaskAttempt> list = unassignedTaskForEachVolume.get(volumeId);
        if (list == null) {
          list = new LinkedHashSet<TaskAttempt>();
          unassignedTaskForEachVolume.put(volumeId, list);
        }
        list.add(attemptId);
      }

      remainTasksNum.incrementAndGet();

      if(!diskVolumeLoads.containsKey(volumeId)) diskVolumeLoads.put(volumeId, 0);
    }

    /**
     *  Priorities
     *  1. a task list in a volume of host
     *  2. unknown block or Non-splittable task in host
     *  3. remote tasks. unassignedTaskForEachVolume is only contained local task. so it will be null
     */
    public synchronized TaskAttemptId getLocalTask(TajoContainerId containerId) {
      int volumeId;
      TaskAttemptId taskAttemptId = null;

      if (!lastAssignedVolumeId.containsKey(containerId)) {
        volumeId = getLowestVolumeId();
        increaseConcurrency(containerId, volumeId);
      } else {
        volumeId = lastAssignedVolumeId.get(containerId);
      }

      if (unassignedTaskForEachVolume.size() >  0) {
        int retry = unassignedTaskForEachVolume.size();
        do {
          //clean and get a remaining local task
          taskAttemptId = getAndRemove(volumeId);
          if(!unassignedTaskForEachVolume.containsKey(volumeId)) {
            decreaseConcurrency(containerId);
            if (volumeId > REMOTE) {
              diskVolumeLoads.remove(volumeId);
            }
          }

          if (taskAttemptId == null) {
            //reassign next volume
            volumeId = getLowestVolumeId();
            increaseConcurrency(containerId, volumeId);
            retry--;
          } else {
            break;
          }
        } while (retry > 0);
      } else {
        this.remainTasksNum.set(0);
      }
      return taskAttemptId;
    }

    public synchronized TaskAttemptId getTaskAttemptIdByRack(String rack) {
      TaskAttemptId taskAttemptId = null;

      if (unassignedTaskForEachVolume.size() > 0 && this.rack.equals(rack)) {
        int retry = unassignedTaskForEachVolume.size();
        do {
          //clean and get a remaining task
          int volumeId = getLowestVolumeId();
          taskAttemptId = getAndRemove(volumeId);
          if (taskAttemptId == null) {
            if (volumeId > REMOTE) {
              diskVolumeLoads.remove(volumeId);
            }
            retry--;
          } else {
            break;
          }
        } while (retry > 0);
      }
      return taskAttemptId;
    }

    private synchronized TaskAttemptId getAndRemove(int volumeId){
      TaskAttemptId taskAttemptId = null;
      if(!unassignedTaskForEachVolume.containsKey(volumeId)) return taskAttemptId;

      LinkedHashSet<TaskAttempt> list = unassignedTaskForEachVolume.get(volumeId);
      if(list != null && list.size() > 0){
        TaskAttempt taskAttempt;
        synchronized (unassignedTaskForEachVolume) {
          Iterator<TaskAttempt> iterator = list.iterator();
          taskAttempt = iterator.next();
          iterator.remove();
        }

        this.remainTasksNum.getAndDecrement();
        taskAttemptId = taskAttempt.getId();
        for (DataLocation location : taskAttempt.getTask().getDataLocations()) {
          if (!this.getHost().equals(location.getHost())) {
            HostVolumeMapping volumeMapping = scheduledRequests.leafTaskHostMapping.get(location.getHost());
            if (volumeMapping != null) {
              volumeMapping.removeTaskAttempt(location.getVolumeId(), taskAttempt);
            }
          }
        }
      }

      if(list == null || list.isEmpty()) {
        unassignedTaskForEachVolume.remove(volumeId);
      }
      return taskAttemptId;
    }

    private synchronized void removeTaskAttempt(int volumeId, TaskAttempt taskAttempt){
      if(!unassignedTaskForEachVolume.containsKey(volumeId)) return;

      LinkedHashSet<TaskAttempt> tasks  = unassignedTaskForEachVolume.get(volumeId);

      if(tasks != null && tasks.size() > 0){
        tasks.remove(taskAttempt);
        remainTasksNum.getAndDecrement();
      } else {
        unassignedTaskForEachVolume.remove(volumeId);
      }
    }

    /**
     * Increase the count of running tasks and disk loads for a certain task runner.
     *
     * @param containerId The task runner identifier
     * @param volumeId Volume identifier
     * @return the volume load (i.e., how many running tasks use this volume)
     */
    private synchronized int increaseConcurrency(TajoContainerId containerId, int volumeId) {

      int concurrency = 1;
      if (diskVolumeLoads.containsKey(volumeId)) {
        concurrency = diskVolumeLoads.get(volumeId) + 1;
      }

      if (volumeId > -1) {
        LOG.info("Assigned host : " + host + ", Volume : " + volumeId + ", Concurrency : " + concurrency);
      } else if (volumeId == -1) {
        // this case is disabled namenode block meta or compressed text file or amazon s3
        LOG.info("Assigned host : " + host + ", Unknown Volume : " + volumeId + ", Concurrency : " + concurrency);
      } else if (volumeId == REMOTE) {
        // this case has processed all block on host and it will be assigned to remote
        LOG.info("Assigned host : " + host + ", Remaining local tasks : " + getRemainingLocalTaskSize()
            + ", Remote Concurrency : " + concurrency);
      }
      diskVolumeLoads.put(volumeId, concurrency);
      lastAssignedVolumeId.put(containerId, volumeId);
      return concurrency;
    }

    /**
     * Decrease the count of running tasks of a certain task runner
     */
    private synchronized void decreaseConcurrency(TajoContainerId containerId){
      Integer volumeId = lastAssignedVolumeId.get(containerId);
      if(volumeId != null && diskVolumeLoads.containsKey(volumeId)){
        Integer concurrency = diskVolumeLoads.get(volumeId);
        if(concurrency > 0){
          diskVolumeLoads.put(volumeId, concurrency - 1);
        } else {
          if (volumeId > REMOTE) {
            diskVolumeLoads.remove(volumeId);
          }
        }
      }
      lastAssignedVolumeId.remove(containerId);
    }

    /**
     *  volume of a host : 0 ~ n
     *  compressed task, amazon s3, unKnown volume : -1
     *  remote task : -2
     */
    public int getLowestVolumeId(){
      Map.Entry<Integer, Integer> volumeEntry = null;

      for (Map.Entry<Integer, Integer> entry : diskVolumeLoads.entrySet()) {
        if(volumeEntry == null) volumeEntry = entry;

        if (volumeEntry.getValue() >= entry.getValue()) {
          volumeEntry = entry;
        }
      }

      if(volumeEntry != null){
        return volumeEntry.getKey();
      } else {
        return REMOTE;
      }
    }

    public boolean isAssigned(TajoContainerId containerId){
      return lastAssignedVolumeId.containsKey(containerId);
    }

    public boolean isRemote(TajoContainerId containerId){
      Integer volumeId = lastAssignedVolumeId.get(containerId);
      if(volumeId == null || volumeId > REMOTE){
        return false;
      } else {
        return true;
      }
    }

    public int getRemoteConcurrency(){
      return getVolumeConcurrency(REMOTE);
    }

    public int getVolumeConcurrency(int volumeId){
      Integer size = diskVolumeLoads.get(volumeId);
      if(size == null) return 0;
      else return size;
    }

    public int getRemainingLocalTaskSize(){
      return remainTasksNum.get();
    }

    public String getHost() {

      return host;
    }

    public String getRack() {
      return rack;
    }
  }

  private class ScheduledRequests {
    // two list leafTasks and nonLeafTasks keep all tasks to be scheduled. Even though some task is included in
    // leafTaskHostMapping or leafTasksRackMapping, some task T will not be sent to a task runner
    // if the task is not included in leafTasks and nonLeafTasks.
    private final Set<TaskAttemptId> leafTasks = Collections.synchronizedSet(new HashSet<TaskAttemptId>());
    private final Set<TaskAttemptId> nonLeafTasks = Collections.synchronizedSet(new HashSet<TaskAttemptId>());
    private Map<String, HostVolumeMapping> leafTaskHostMapping = Maps.newConcurrentMap();
    private final Map<String, HashSet<TaskAttemptId>> leafTasksRackMapping = Maps.newConcurrentMap();

    private synchronized void addLeafTask(TaskAttemptToSchedulerEvent event) {
      TaskAttempt taskAttempt = event.getTaskAttempt();
      List<DataLocation> locations = taskAttempt.getTask().getDataLocations();

      for (DataLocation location : locations) {
        String host = location.getHost();

        HostVolumeMapping hostVolumeMapping = leafTaskHostMapping.get(host);
        if (hostVolumeMapping == null) {
          String rack = RackResolver.resolve(host).getNetworkLocation();
          hostVolumeMapping = new HostVolumeMapping(host, rack);
          leafTaskHostMapping.put(host, hostVolumeMapping);
        }
        hostVolumeMapping.addTaskAttempt(location.getVolumeId(), taskAttempt);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to host " + host);
        }

        HashSet<TaskAttemptId> list = leafTasksRackMapping.get(hostVolumeMapping.getRack());
        if (list == null) {
          list = new HashSet<TaskAttemptId>();
          leafTasksRackMapping.put(hostVolumeMapping.getRack(), list);
        }

        list.add(taskAttempt.getId());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to rack " + hostVolumeMapping.getRack());
        }
      }

      leafTasks.add(taskAttempt.getId());
    }

    private void addNonLeafTask(TaskAttemptToSchedulerEvent event) {
      nonLeafTasks.add(event.getTaskAttempt().getId());
    }

    public int leafTaskNum() {
      return leafTasks.size();
    }

    public int nonLeafTaskNum() {
      return nonLeafTasks.size();
    }

    public Set<TaskAttemptId> assignedRequest = new HashSet<TaskAttemptId>();

    private TaskAttemptId allocateLocalTask(String host, TajoContainerId containerId){
      HostVolumeMapping hostVolumeMapping = leafTaskHostMapping.get(host);

      if (hostVolumeMapping != null) { //tajo host is located in hadoop datanode
        for (int i = 0; i < hostVolumeMapping.getRemainingLocalTaskSize(); i++) {
          TaskAttemptId attemptId = hostVolumeMapping.getLocalTask(containerId);

          if(attemptId == null) break;
          //find remaining local task
          if (leafTasks.contains(attemptId)) {
            leafTasks.remove(attemptId);
            //LOG.info(attemptId + " Assigned based on host match " + hostName);
            hostLocalAssigned++;
            totalAssigned++;
            return attemptId;
          }
        }
      }
      return null;
    }

    private TaskAttemptId allocateRackTask(String host) {

      List<HostVolumeMapping> remainingTasks = Lists.newArrayList(leafTaskHostMapping.values());
      String rack = RackResolver.resolve(host).getNetworkLocation();
      TaskAttemptId attemptId = null;

      if (remainingTasks.size() > 0) {
        synchronized (scheduledRequests) {
          //find largest remaining task of other host in rack
          Collections.sort(remainingTasks, new Comparator<HostVolumeMapping>() {
            @Override
            public int compare(HostVolumeMapping v1, HostVolumeMapping v2) {
              // descending remaining tasks
              if (v2.remainTasksNum.get() > v1.remainTasksNum.get()) {
                return 1;
              } else if (v2.remainTasksNum.get() == v1.remainTasksNum.get()) {
                return 0;
              } else {
                return -1;
              }
            }
          });
        }

        for (HostVolumeMapping tasks : remainingTasks) {
          for (int i = 0; i < tasks.getRemainingLocalTaskSize(); i++) {
            TaskAttemptId tId = tasks.getTaskAttemptIdByRack(rack);

            if (tId == null) break;

            if (leafTasks.contains(tId)) {
              leafTasks.remove(tId);
              attemptId = tId;
              break;
            }
          }
          if(attemptId != null) break;
        }
      }

      //find task in rack
      if (attemptId == null) {
        HashSet<TaskAttemptId> list = leafTasksRackMapping.get(rack);
        if (list != null) {
          synchronized (list) {
            Iterator<TaskAttemptId> iterator = list.iterator();
            while (iterator.hasNext()) {
              TaskAttemptId tId = iterator.next();
              iterator.remove();
              if (leafTasks.contains(tId)) {
                leafTasks.remove(tId);
                attemptId = tId;
                break;
              }
            }
          }
        }
      }

      if (attemptId != null) {
        rackLocalAssigned++;
        totalAssigned++;

        LOG.info(String.format("Assigned Local/Rack/Total: (%d/%d/%d), Locality: %.2f%%, Rack host: %s",
            hostLocalAssigned, rackLocalAssigned, totalAssigned,
            ((double) hostLocalAssigned / (double) totalAssigned) * 100, host));

      }
      return attemptId;
    }

    public void assignToLeafTasks(LinkedList<TaskRequestEvent> taskRequests) {
      Collections.shuffle(taskRequests);
      LinkedList<TaskRequestEvent> remoteTaskRequests = new LinkedList<TaskRequestEvent>();

      TaskRequestEvent taskRequest;
      while (leafTasks.size() > 0 && (!taskRequests.isEmpty() || !remoteTaskRequests.isEmpty())) {
        taskRequest = taskRequests.pollFirst();
        if(taskRequest == null) { // if there are only remote task requests
          taskRequest = remoteTaskRequests.pollFirst();
        }

        // checking if this container is still alive.
        // If not, ignore the task request and stop the task runner
        ContainerProxy container = context.getMasterContext().getResourceAllocator()
            .getContainer(taskRequest.getContainerId());
        if(container == null) {
          taskRequest.getCallback().run(stopTaskRunnerReq);
          continue;
        }

        // getting the hostname of requested node
        WorkerConnectionInfo connectionInfo =
            context.getMasterContext().getResourceAllocator().getWorkerConnectionInfo(taskRequest.getWorkerId());
        String host = connectionInfo.getHost();

        // if there are no worker matched to the hostname a task request
        if(!leafTaskHostMapping.containsKey(host)){
          String normalizedHost = NetUtils.normalizeHost(host);

          if(!leafTaskHostMapping.containsKey(normalizedHost) && !taskRequests.isEmpty()){
            // this case means one of either cases:
            // * there are no blocks which reside in this node.
            // * all blocks which reside in this node are consumed, and this task runner requests a remote task.
            // In this case, we transfer the task request to the remote task request list, and skip the followings.
            remoteTaskRequests.add(taskRequest);
            continue;
          }
        }

        TajoContainerId containerId = taskRequest.getContainerId();
        LOG.debug("assignToLeafTasks: " + taskRequest.getExecutionBlockId() + "," +
            "containerId=" + containerId);

        //////////////////////////////////////////////////////////////////////
        // disk or host-local allocation
        //////////////////////////////////////////////////////////////////////
        TaskAttemptId attemptId = allocateLocalTask(host, containerId);

        if (attemptId == null) { // if a local task cannot be found
          HostVolumeMapping hostVolumeMapping = leafTaskHostMapping.get(host);

          if(hostVolumeMapping != null) {
            if(!hostVolumeMapping.isRemote(containerId)){
              // assign to remote volume
              hostVolumeMapping.decreaseConcurrency(containerId);
              hostVolumeMapping.increaseConcurrency(containerId, HostVolumeMapping.REMOTE);
            }
            // this part is remote concurrency management of a tail tasks
            int tailLimit = Math.max(remainingScheduledObjectNum() / (leafTaskHostMapping.size() * 2), 1);

            if(hostVolumeMapping.getRemoteConcurrency() > tailLimit){
              //release container
              hostVolumeMapping.decreaseConcurrency(containerId);
              taskRequest.getCallback().run(stopTaskRunnerReq);
              continue;
            }
          }

          //////////////////////////////////////////////////////////////////////
          // rack-local allocation
          //////////////////////////////////////////////////////////////////////
          attemptId = allocateRackTask(host);

          //////////////////////////////////////////////////////////////////////
          // random node allocation
          //////////////////////////////////////////////////////////////////////
          if (attemptId == null && leafTaskNum() > 0) {
            synchronized (leafTasks){
              attemptId = leafTasks.iterator().next();
              leafTasks.remove(attemptId);
              rackLocalAssigned++;
              totalAssigned++;
              LOG.info(String.format("Assigned Local/Remote/Total: (%d/%d/%d), Locality: %.2f%%,",
                  hostLocalAssigned, rackLocalAssigned, totalAssigned,
                  ((double) hostLocalAssigned / (double) totalAssigned) * 100));
            }
          }
        }

        if (attemptId != null) {
          Task task = stage.getTask(attemptId.getTaskId());
          TaskRequest taskAssign = new TaskRequestImpl(
              attemptId,
              new ArrayList<FragmentProto>(task.getAllFragments()),
              "",
              false,
              LogicalNodeSerializer.serialize(task.getLogicalPlan()),
              context.getMasterContext().getQueryContext(),
              stage.getDataChannel(), stage.getBlock().getEnforcer());
          if (checkIfInterQuery(stage.getMasterPlan(), stage.getBlock())) {
            taskAssign.setInterQuery();
          }

          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(), connectionInfo));
          assignedRequest.add(attemptId);

          scheduledObjectNum--;
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

    public void assignToNonLeafTasks(LinkedList<TaskRequestEvent> taskRequests) {
      Collections.shuffle(taskRequests);

      TaskRequestEvent taskRequest;
      while (!taskRequests.isEmpty()) {
        taskRequest = taskRequests.pollFirst();
        LOG.debug("assignToNonLeafTasks: " + taskRequest.getExecutionBlockId());

        TaskAttemptId attemptId;
        // random allocation
        if (nonLeafTasks.size() > 0) {
          synchronized (nonLeafTasks){
            attemptId = nonLeafTasks.iterator().next();
            nonLeafTasks.remove(attemptId);
          }
          LOG.debug("Assigned based on * match");

          Task task;
          task = stage.getTask(attemptId.getTaskId());
          TaskRequest taskAssign = new TaskRequestImpl(
              attemptId,
              Lists.newArrayList(task.getAllFragments()),
              "",
              false,
              LogicalNodeSerializer.serialize(task.getLogicalPlan()),
              context.getMasterContext().getQueryContext(),
              stage.getDataChannel(),
              stage.getBlock().getEnforcer());
          if (checkIfInterQuery(stage.getMasterPlan(), stage.getBlock())) {
            taskAssign.setInterQuery();
          }
          for(Map.Entry<String, Set<FetchImpl>> entry: task.getFetchMap().entrySet()) {
            Collection<FetchImpl> fetches = entry.getValue();
            if (fetches != null) {
              for (FetchImpl fetch : fetches) {
                taskAssign.addFetch(entry.getKey(), fetch);
              }
            }
          }

          WorkerConnectionInfo connectionInfo = context.getMasterContext().getResourceAllocator().
              getWorkerConnectionInfo(taskRequest.getWorkerId());
          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId,
              taskRequest.getContainerId(), connectionInfo));
          taskRequest.getCallback().run(taskAssign.getProto());
          totalAssigned++;
          scheduledObjectNum--;
        }
      }
    }
  }
}
