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
import com.google.common.collect.Sets;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.TaskRequest;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.QueryCoordinatorProtocolService;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.RpcParameterFactory;
import org.apache.tajo.util.TUtil;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ResourceProtos.*;

public class DefaultTaskScheduler extends AbstractTaskScheduler {
  private static final Log LOG = LogFactory.getLog(DefaultTaskScheduler.class);

  private final TaskSchedulerContext context;
  private Stage stage;
  private TajoConf tajoConf;
  private Properties rpcParams;

  private Thread schedulingThread;
  private volatile boolean isStopped;
  private AtomicBoolean needWakeup = new AtomicBoolean();

  private ScheduledRequests scheduledRequests;

  private int minTaskMemory;
  private int nextTaskId = 0;
  private int scheduledObjectNum = 0;
  private boolean isLeaf;
  private int schedulerDelay;
  private int maximumRequestContainer;

  // candidate workers for locality of high priority
  private Set<Integer> candidateWorkers = Sets.newHashSet();

  public DefaultTaskScheduler(TaskSchedulerContext context, Stage stage) {
    super(DefaultTaskScheduler.class.getName());
    this.context = context;
    this.stage = stage;
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    rpcParams = RpcParameterFactory.get(tajoConf);

    scheduledRequests = new ScheduledRequests();
    minTaskMemory = tajoConf.getIntVar(TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY);
    schedulerDelay= tajoConf.getIntVar(TajoConf.ConfVars.QUERYMASTER_TASK_SCHEDULER_DELAY);
    isLeaf = stage.getMasterPlan().isLeaf(stage.getBlock());

    this.schedulingThread = new Thread() {
      public void run() {

        while (!isStopped && !Thread.currentThread().isInterrupted()) {

          try {
            schedule();
          } catch (InterruptedException e) {
            if (isStopped) {
              break;
            } else {
              LOG.fatal(e.getMessage(), e);
              stage.abort(StageState.ERROR, e);
            }
          } catch (Throwable e) {
            LOG.fatal(e.getMessage(), e);
            stage.abort(StageState.ERROR, e);
            break;
          }
        }
        info(LOG, "TaskScheduler schedulingThread stopped");
      }
    };
    super.init(conf);
  }

  @Override
  public void start() {
    info(LOG, "Start TaskScheduler");
    maximumRequestContainer = Math.min(tajoConf.getIntVar(TajoConf.ConfVars.QUERYMASTER_TASK_SCHEDULER_REQUEST_MAX_NUM)
        , stage.getContext().getWorkerMap().size());

    if (isLeaf) {
      candidateWorkers.addAll(getWorkerIds(getLeafTaskHosts()));
    } else {
      //find assigned hosts for Non-Leaf locality in children executionBlock
      List<ExecutionBlock> executionBlockList = stage.getMasterPlan().getChilds(stage.getBlock());
      for (ExecutionBlock executionBlock : executionBlockList) {
        Stage childStage = stage.getContext().getStage(executionBlock.getId());
        candidateWorkers.addAll(childStage.getAssignedWorkerMap().keySet());
      }
    }

    this.schedulingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    isStopped = true;

    if (schedulingThread != null) {
      synchronized (schedulingThread) {
        schedulingThread.interrupt();
      }
    }
    candidateWorkers.clear();
    scheduledRequests.clear();
    info(LOG, "Task Scheduler stopped");
    super.stop();
  }

  protected void info(Log log, String message) {
    log.info(String.format("[%s] %s", stage.getId(), message));
  }

  protected void warn(Log log, String message) {
    log.warn(String.format("[%s] %s", stage.getId(), message));
  }

  private Fragment[] fragmentsForNonLeafTask;
  private Fragment[] broadcastFragmentsForNonLeafTask;

  public void schedule() throws Exception {
    try {
      final int incompleteTaskNum = scheduledRequests.leafTaskNum() + scheduledRequests.nonLeafTaskNum();
      if (incompleteTaskNum == 0) {
        needWakeup.set(true);
        // all task is done or tasks is not scheduled
        synchronized (schedulingThread) {
          schedulingThread.wait(1000);
        }
      } else {
        LinkedList<TaskRequestEvent> taskRequests = createTaskRequest(incompleteTaskNum);

        if (taskRequests.size() == 0) {
          synchronized (schedulingThread) {
            schedulingThread.wait(schedulerDelay);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Get " + taskRequests.size() + " taskRequestEvents ");
          }

          if (isLeaf) {
            scheduledRequests.assignToLeafTasks(taskRequests);
          } else {
            scheduledRequests.assignToNonLeafTasks(taskRequests);
          }
        }
      }
    } catch (TimeoutException e) {
      LOG.error(e.getMessage());
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
          fragmentsForNonLeafTask = new Fragment[2];
          fragmentsForNonLeafTask[0] = castEvent.getLeftFragment();
          if (castEvent.hasRightFragments()) {
            Collection<Fragment> var = castEvent.getRightFragments();
            Fragment[] rightFragments = var.toArray(new Fragment[var.size()]);
            fragmentsForNonLeafTask[1] = rightFragments[0];
            if (rightFragments.length > 1) {
              broadcastFragmentsForNonLeafTask = new Fragment[rightFragments.length - 1];
              System.arraycopy(rightFragments, 1, broadcastFragmentsForNonLeafTask, 0, broadcastFragmentsForNonLeafTask.length);
            } else {
              broadcastFragmentsForNonLeafTask = null;
            }
          }
        }
      } else if (event instanceof FetchScheduleEvent) {
        FetchScheduleEvent castEvent = (FetchScheduleEvent) event;
        Map<String, List<FetchProto>> fetches = castEvent.getFetches();
        TaskAttemptScheduleContext taskScheduleContext = new TaskAttemptScheduleContext();
        Task task = Stage.newEmptyTask(context, taskScheduleContext, stage, nextTaskId++);
        scheduledObjectNum++;
        for (Entry<String, List<FetchProto>> eachFetch : fetches.entrySet()) {
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

        if (needWakeup.getAndSet(false)) {
          //wake up scheduler thread after scheduled
          synchronized (schedulingThread) {
            schedulingThread.notifyAll();
          }
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

  private Set<Integer> getWorkerIds(Collection<String> hosts){
    Set<Integer> workerIds = Sets.newHashSet();
    if(hosts.isEmpty()) return workerIds;

    for (WorkerConnectionInfo worker : stage.getContext().getWorkerMap().values()) {
      if(hosts.contains(worker.getHost())){
        workerIds.add(worker.getId());
      }
    }
    return workerIds;
  }


  protected LinkedList<TaskRequestEvent> createTaskRequest(final int incompleteTaskNum) throws Exception {
    LinkedList<TaskRequestEvent> taskRequestEvents = new LinkedList<>();

    //If scheduled tasks is long-term task, cluster resource can be the worst load balance.
    //This part is to throttle the maximum required container per request
    int requestContainerNum = Math.min(incompleteTaskNum, maximumRequestContainer);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try to schedule task resources: " + requestContainerNum);
    }

    ServiceTracker serviceTracker =
        context.getMasterContext().getQueryMasterContext().getWorkerContext().getServiceTracker();
    NettyClientBase tmClient = RpcClientManager.getInstance().
        getClient(serviceTracker.getUmbilicalAddress(), QueryCoordinatorProtocol.class, true, rpcParams);
    QueryCoordinatorProtocolService masterClientService = tmClient.getStub();

    CallFuture<NodeResourceResponse> callBack = new CallFuture<>();
    NodeResourceRequest.Builder request = NodeResourceRequest.newBuilder();
    request.setCapacity(NodeResources.createResource(minTaskMemory, isLeaf ? 1 : 0).getProto())
        .setNumContainers(requestContainerNum)
        .setPriority(stage.getPriority())
        .setQueryId(context.getMasterContext().getQueryId().getProto())
        .setType(isLeaf ? ResourceType.LEAF : ResourceType.INTERMEDIATE)
        .setUserId(context.getMasterContext().getQueryContext().getUser())
        .setRunningTasks(stage.getTotalScheduledObjectsCount() - stage.getCompletedTaskCount())
        .addAllCandidateNodes(candidateWorkers)
        .setQueue(context.getMasterContext().getQueryContext().get("queue", "default")); //TODO set queue

    masterClientService.reserveNodeResources(callBack.getController(), request.build(), callBack);
    NodeResourceResponse response = callBack.get();

    for (AllocationResourceProto resource : response.getResourceList()) {
      taskRequestEvents.add(new TaskRequestEvent(resource.getWorkerId(), resource, context.getBlockId()));
    }

    return taskRequestEvents;
  }

  @Override
  public int remainingScheduledObjectNum() {
    return scheduledObjectNum;
  }

  public void releaseTaskAttempt(TaskAttempt taskAttempt) {
    if (taskAttempt != null && taskAttempt.isLeafTask() && taskAttempt.getWorkerConnectionInfo() != null) {

      HostVolumeMapping mapping =
          scheduledRequests.leafTaskHostMapping.get(taskAttempt.getWorkerConnectionInfo().getHost());
      if (mapping != null && mapping.lastAssignedVolumeId.containsKey(taskAttempt.getId())) {
        mapping.decreaseConcurrency(mapping.lastAssignedVolumeId.remove(taskAttempt.getId()));
      }
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
        Collections.synchronizedMap(new HashMap<>());
    /** A value is last assigned volume id for each task runner */
    private HashMap<TaskAttemptId, Integer> lastAssignedVolumeId = Maps.newHashMap();
    /**
     * A key is disk volume id, and a value is the load of this volume.
     * This load is measured by counting how many number of tasks are running.
     *
     * These disk volumes are kept in an order of ascending order of the volume id.
     * In other words, the head volume ids are likely to -1, meaning no given volume id.
     */
    private SortedMap<Integer, Integer> diskVolumeLoads = new TreeMap<>();
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
          list = new LinkedHashSet<>();
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
    public synchronized TaskAttemptId getLocalTask() {
      int volumeId = getLowestVolumeId();
      TaskAttemptId taskAttemptId = null;

      if (unassignedTaskForEachVolume.size() >  0) {
        int retry = diskVolumeLoads.size();
        do {
          //clean and get a remaining local task
          taskAttemptId = getAndRemove(volumeId);

          if (taskAttemptId == null) {
            //reassign next volume
            volumeId = getLowestVolumeId();
            retry--;
          } else {
            lastAssignedVolumeId.put(taskAttemptId, volumeId);
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
      if(!unassignedTaskForEachVolume.containsKey(volumeId)) {
        if (volumeId > REMOTE) {
          diskVolumeLoads.remove(volumeId);
        }
        return taskAttemptId;
      }

      LinkedHashSet<TaskAttempt> list = unassignedTaskForEachVolume.get(volumeId);
      if (list != null && !list.isEmpty()) {
        TaskAttempt taskAttempt;
        synchronized (unassignedTaskForEachVolume) {
          Iterator<TaskAttempt> iterator = list.iterator();
          taskAttempt = iterator.next();
          iterator.remove();
          remainTasksNum.decrementAndGet();
        }

        taskAttemptId = taskAttempt.getId();
        for (DataLocation location : taskAttempt.getTask().getDataLocations()) {
          HostVolumeMapping volumeMapping = scheduledRequests.leafTaskHostMapping.get(location.getHost());
          if (volumeMapping != null) {
            volumeMapping.removeTaskAttempt(location.getVolumeId(), taskAttempt);
          }
        }

        increaseConcurrency(volumeId);
      } else {
        unassignedTaskForEachVolume.remove(volumeId);
      }

      return taskAttemptId;
    }

    private synchronized void removeTaskAttempt(int volumeId, TaskAttempt taskAttempt){
      if(!unassignedTaskForEachVolume.containsKey(volumeId)) return;

      LinkedHashSet<TaskAttempt> tasks  = unassignedTaskForEachVolume.get(volumeId);
      if(tasks.remove(taskAttempt)) {
        remainTasksNum.getAndDecrement();
      }

      if(tasks.isEmpty()){
        unassignedTaskForEachVolume.remove(volumeId);
        if (volumeId > REMOTE) {
          diskVolumeLoads.remove(volumeId);
        }
      }
    }

    /**
     * Increase the count of running tasks and disk loads for a certain task runner.
     *
     * @param volumeId Volume identifier
     * @return the volume load (i.e., how many running tasks use this volume)
     */
    private synchronized int increaseConcurrency(int volumeId) {

      int concurrency = 1;
      if (diskVolumeLoads.containsKey(volumeId)) {
        concurrency = diskVolumeLoads.get(volumeId) + 1;
      }

      if (volumeId > -1) {
        info(LOG, "Assigned host : " + host + ", Volume : " + volumeId + ", Concurrency : " + concurrency);
      } else if (volumeId == -1) {
        // this case is disabled namenode block meta or compressed text file or amazon s3
        info(LOG, "Assigned host : " + host + ", Unknown Volume : " + volumeId + ", Concurrency : " + concurrency);
      } else if (volumeId == REMOTE) {
        // this case has processed all block on host and it will be assigned to remote
        info(LOG, "Assigned host : " + host + ", Remaining local tasks : " + getRemainingLocalTaskSize()
            + ", Remote Concurrency : " + concurrency + ", Unassigned volumes: " + unassignedTaskForEachVolume.size());
      }
      diskVolumeLoads.put(volumeId, concurrency);
      return concurrency;
    }

    /**
     * Decrease the count of running tasks of a certain task runner
     */
    private synchronized void decreaseConcurrency(int volumeId){
      if(diskVolumeLoads.containsKey(volumeId)){
        int concurrency = diskVolumeLoads.get(volumeId);
        if(concurrency > 0){
          diskVolumeLoads.put(volumeId, concurrency - 1);
        }
      }
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

        if (entry.getKey() != REMOTE && volumeEntry.getValue() >= entry.getValue()) {
          volumeEntry = entry;
        }
      }

      if(volumeEntry != null){
        return volumeEntry.getKey();
      } else {
        return REMOTE;
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

  protected void cancel(TaskAttempt taskAttempt) {

    TaskAttemptToSchedulerEvent schedulerEvent = new TaskAttemptToSchedulerEvent(
        EventType.T_SCHEDULE, taskAttempt.getTask().getId().getExecutionBlockId(),
        null, taskAttempt);

    if(taskAttempt.isLeafTask()) {
      releaseTaskAttempt(taskAttempt);

      scheduledRequests.addLeafTask(schedulerEvent);
    } else {
      scheduledRequests.addNonLeafTask(schedulerEvent);
    }

    context.getMasterContext().getEventHandler().handle(
        new TaskAttemptEvent(taskAttempt.getId(), TaskAttemptEventType.TA_ASSIGN_CANCEL));
  }

  protected int cancel(List<TaskAllocationProto> tasks) {
    int canceled = 0;
    for (TaskAllocationProto proto : tasks) {
      TaskAttemptId attemptId = new TaskAttemptId(proto.getTaskRequest().getId());
      cancel(stage.getTask(attemptId.getTaskId()).getAttempt(attemptId));
      canceled++;
    }
    return canceled;
  }

  private class ScheduledRequests {
    // two list leafTasks and nonLeafTasks keep all tasks to be scheduled. Even though some task is included in
    // leafTaskHostMapping or leafTasksRackMapping, some task T will not be sent to a task runner
    // if the task is not included in leafTasks and nonLeafTasks.
    private final Set<TaskAttemptId> leafTasks = Collections.synchronizedSet(new HashSet<>());
    private final Set<TaskAttemptId> nonLeafTasks = Collections.synchronizedSet(new HashSet<>());
    private Map<String, HostVolumeMapping> leafTaskHostMapping = Maps.newConcurrentMap();
    private final Map<String, HashSet<TaskAttemptId>> leafTasksRackMapping = Maps.newConcurrentMap();

    protected void clear() {
      leafTasks.clear();
      nonLeafTasks.clear();
      leafTaskHostMapping.clear();
      leafTasksRackMapping.clear();
    }

    private void addLeafTask(TaskAttemptToSchedulerEvent event) {
      TaskAttempt taskAttempt = event.getTaskAttempt();
      List<DataLocation> locations = taskAttempt.getTask().getDataLocations();

      for (DataLocation location : locations) {
        String host = location.getHost();
        leafTaskHosts.add(host);

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
          list = new HashSet<>();
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

    private TaskAttemptId allocateLocalTask(String host){
      HostVolumeMapping hostVolumeMapping = leafTaskHostMapping.get(host);

      if (hostVolumeMapping != null) { //tajo host is located in hadoop datanode
        for (int i = 0; i < hostVolumeMapping.getRemainingLocalTaskSize(); i++) {
          TaskAttemptId attemptId = hostVolumeMapping.getLocalTask();

          if(attemptId == null) break;
          //find remaining local task
          if (leafTasks.contains(attemptId)) {
            leafTasks.remove(attemptId);
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

      return attemptId;
    }

    public void assignToLeafTasks(LinkedList<TaskRequestEvent> taskRequests) {
      Collections.shuffle(taskRequests);
      LinkedList<TaskRequestEvent> remoteTaskRequests = new LinkedList<>();
      String queryMasterHostAndPort = context.getMasterContext().getQueryMasterContext().getWorkerContext().
          getConnectionInfo().getHostAndQMPort();

      TaskRequestEvent taskRequest;
      while (leafTasks.size() > 0 && (!taskRequests.isEmpty() || !remoteTaskRequests.isEmpty())) {
        int localAssign = 0;
        int rackAssign = 0;

        taskRequest = taskRequests.pollFirst();
        if(taskRequest == null) { // if there are only remote task requests
          taskRequest = remoteTaskRequests.pollFirst();
        }

        // checking if this container is still alive.
        // If not, ignore the task request and stop the task runner
        WorkerConnectionInfo connectionInfo = context.getMasterContext().getWorkerMap().get(taskRequest.getWorkerId());
        if(connectionInfo == null) continue;

        // getting the hostname of requested node
        String host = connectionInfo.getHost();

        // if there are no worker matched to the hostname a task request
        if (!leafTaskHostMapping.containsKey(host) && !taskRequests.isEmpty()) {
          String normalizedHost = NetUtils.normalizeHost(host);

          if (!leafTaskHostMapping.containsKey(normalizedHost)) {
            // this case means one of either cases:
            // * there are no blocks which reside in this node.
            // * all blocks which reside in this node are consumed, and this task runner requests a remote task.
            // In this case, we transfer the task request to the remote task request list, and skip the followings.
            remoteTaskRequests.add(taskRequest);
            continue;
          } else {
            host = normalizedHost;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("assignToLeafTasks: " + taskRequest.getExecutionBlockId() + "," +
              "worker=" + connectionInfo.getHostAndPeerRpcPort());
        }

        //////////////////////////////////////////////////////////////////////
        // disk or host-local allocation
        //////////////////////////////////////////////////////////////////////
        TaskAttemptId attemptId = allocateLocalTask(host);

        if (attemptId == null) { // if a local task cannot be found
          HostVolumeMapping hostVolumeMapping = leafTaskHostMapping.get(host);

          if(!taskRequests.isEmpty()) { //if other requests remains, move to remote list for better locality
            remoteTaskRequests.add(taskRequest);
            candidateWorkers.remove(connectionInfo.getId());
            continue;

          } else {
            if(hostVolumeMapping != null) {
              int nodes = context.getMasterContext().getWorkerMap().size();
              //this part is to control the assignment of tail and remote task balancing per node
              int tailLimit = 1;
              if (remainingScheduledObjectNum() > 0 && nodes > 0) {
                tailLimit = Math.max(remainingScheduledObjectNum() / nodes, 1);
              }

              //remote task throttling per node
              if (nodes > 1 && hostVolumeMapping.getRemoteConcurrency() >= tailLimit) {
                continue;
              } else {
                // assign to remote volume
                hostVolumeMapping.increaseConcurrency(HostVolumeMapping.REMOTE);
              }
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
            }
          }

          if (attemptId != null && hostVolumeMapping != null) {
            hostVolumeMapping.lastAssignedVolumeId.put(attemptId, HostVolumeMapping.REMOTE);
          }
          rackAssign++;
        } else {
          localAssign++;
        }

        if (attemptId != null) {
          Task task = stage.getTask(attemptId.getTaskId());
          TaskRequest taskAssign = new TaskRequestImpl(
              attemptId,
                  new ArrayList<>(task.getAllFragments()),
              "",
              false,
              LogicalNodeSerializer.serialize(task.getLogicalPlan()),
              context.getMasterContext().getQueryContext(),
              stage.getDataChannel(), stage.getBlock().getEnforcer(),
              queryMasterHostAndPort);

          if (checkIfInterQuery(stage.getMasterPlan(), stage.getBlock())) {
            taskAssign.setInterQuery();
          }

          //TODO send batch request
          BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
          requestProto.addTaskRequest(TaskAllocationProto.newBuilder()
              .setResource(taskRequest.getResponseProto().getResource())
              .setTaskRequest(taskAssign.getProto()).build());

          requestProto.setExecutionBlockId(attemptId.getTaskId().getExecutionBlockId().getProto());
          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId, connectionInfo));

          InetSocketAddress addr = stage.getAssignedWorkerMap().get(connectionInfo.getId());
          if (addr == null) addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());

          AsyncRpcClient tajoWorkerRpc = null;
          CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();
          totalAttempts++;
          try {
            tajoWorkerRpc = RpcClientManager.getInstance().getClient(addr, TajoWorkerProtocol.class, true,
                rpcParams);

            TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
            tajoWorkerRpcClient.allocateTasks(callFuture.getController(), requestProto.build(), callFuture);

            BatchAllocationResponse responseProto = callFuture.get();

            if (responseProto.getCancellationTaskCount() > 0) {
              cancellation += cancel(responseProto.getCancellationTaskList());
              info(LOG, "Canceled requests: " + responseProto.getCancellationTaskCount() + " from " +  addr);
              continue;
            }
          } catch (ExecutionException | ConnectException e) {
            cancellation += cancel(requestProto.getTaskRequestList());

            warn(LOG, "Canceled requests: " + requestProto.getTaskRequestCount()
                + " by " + ExceptionUtils.getFullStackTrace(e));
            continue;
          } catch (Exception e) {
            throw new TajoInternalError(e);
          }

          scheduledObjectNum--;
          totalAssigned++;
          hostLocalAssigned += localAssign;
          rackLocalAssigned += rackAssign;

          if (rackAssign > 0) {
            info(LOG, String.format("Assigned Local/Rack/Total: (%d/%d/%d), " +
                    "Attempted Cancel/Assign/Total: (%d/%d/%d), " +
                    "Locality: %.2f%%, Rack host: %s",
                hostLocalAssigned, rackLocalAssigned, totalAssigned,
                cancellation, totalAssigned, totalAttempts,
                ((double) hostLocalAssigned / (double) totalAssigned) * 100, host));
          }

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
      if (masterPlan.isRoot(parent) && parent.isUnionOnly()) {
        return false;
      }

      return true;
    }

    public void assignToNonLeafTasks(LinkedList<TaskRequestEvent> taskRequests) {
      Collections.shuffle(taskRequests);
      String queryMasterHostAndPort = context.getMasterContext().getQueryMasterContext().getWorkerContext().
          getConnectionInfo().getHostAndQMPort();

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
              stage.getBlock().getEnforcer(),
              queryMasterHostAndPort);

          if (checkIfInterQuery(stage.getMasterPlan(), stage.getBlock())) {
            taskAssign.setInterQuery();
          }
          for(Map.Entry<String, Set<FetchProto>> entry: task.getFetchMap().entrySet()) {
            Collection<FetchProto> fetches = entry.getValue();
            if (fetches != null) {
              for (FetchProto fetch : fetches) {
                taskAssign.addFetch(fetch);
              }
            }
          }

          WorkerConnectionInfo connectionInfo =
              context.getMasterContext().getWorkerMap().get(taskRequest.getWorkerId());

          //TODO send batch request
          BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
          requestProto.addTaskRequest(TaskAllocationProto.newBuilder()
              .setResource(taskRequest.getResponseProto().getResource())
              .setTaskRequest(taskAssign.getProto()).build());

          requestProto.setExecutionBlockId(attemptId.getTaskId().getExecutionBlockId().getProto());
          context.getMasterContext().getEventHandler().handle(new TaskAttemptAssignedEvent(attemptId, connectionInfo));

          CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();

          InetSocketAddress addr = stage.getAssignedWorkerMap().get(connectionInfo.getId());
          if (addr == null) addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());

          AsyncRpcClient tajoWorkerRpc;
          try {
            tajoWorkerRpc = RpcClientManager.getInstance().getClient(addr, TajoWorkerProtocol.class, true,
                rpcParams);

            TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
            tajoWorkerRpcClient.allocateTasks(callFuture.getController(), requestProto.build(), callFuture);

            BatchAllocationResponse responseProto = callFuture.get();

            if(responseProto.getCancellationTaskCount() > 0) {
              cancellation += cancel(responseProto.getCancellationTaskList());
              info(LOG, "Canceled requests: " + responseProto.getCancellationTaskCount() + " from " +  addr);
              continue;
            }

          } catch (ExecutionException | ConnectException e) {
            cancellation += cancel(requestProto.getTaskRequestList());
            warn(LOG, "Canceled requests: " + requestProto.getTaskRequestCount()
                + " by " + ExceptionUtils.getFullStackTrace(e));
            continue;
          } catch (Exception e) {
            throw new TajoInternalError(e);
          }

          totalAssigned++;
          scheduledObjectNum--;
        }
      }
    }
  }
}
