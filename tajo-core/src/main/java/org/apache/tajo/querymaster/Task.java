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

package org.apache.tajo.querymaster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TaskState;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.util.history.TaskHistory;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.ResourceProtos.*;
import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class Task implements EventHandler<TaskEvent> {
  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(Task.class);

  private final Configuration systemConf;
	private TaskId taskId;
  private EventHandler eventHandler;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	
	private Map<String, Set<FragmentProto>> fragMap;
	private Map<String, Set<FetchProto>> fetchMap;

  private int totalFragmentNum;

  private List<ShuffleFileOutput> shuffleFileOutputs;
	private TableStats stats;
  private final boolean isLeafTask;
  private List<IntermediateEntry> intermediateData;

  private Map<TaskAttemptId, TaskAttempt> attempts;
  private final int maxAttempts = 3;
  private Integer nextAttempt = -1;
  private TaskAttemptId lastAttemptId;

  private TaskAttemptId successfulAttempt;

  private WorkerConnectionInfo succeededWorker;

  private int failedAttempts;
  private int finishedAttempts; // finish are total of success, failed and killed

  private long launchTime;
  private long finishTime;

  private List<DataLocation> dataLocations = Lists.newArrayList();

  private static final AttemptKilledTransition ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();

  private TaskHistory finalTaskHistory;

  private final int maxUrlLength;

  protected static final StateMachineFactory
      <Task, TaskState, TaskEventType, TaskEvent> stateMachineFactory =
      new StateMachineFactory <Task, TaskState, TaskEventType, TaskEvent>(TaskState.NEW)

          // Transitions from NEW state
          .addTransition(TaskState.NEW, TaskState.SCHEDULED,
              TaskEventType.T_SCHEDULE,
              new InitialScheduleTransition())
          .addTransition(TaskState.NEW, TaskState.KILLED,
              TaskEventType.T_KILL,
              new KillNewTaskTransition())

          // Transitions from SCHEDULED state
          .addTransition(TaskState.SCHEDULED, TaskState.RUNNING,
              TaskEventType.T_ATTEMPT_LAUNCHED,
              new AttemptLaunchedTransition())
          .addTransition(TaskState.SCHEDULED, TaskState.KILL_WAIT,
              TaskEventType.T_KILL,
              new KillTaskTransition())

          // Transitions from RUNNING state
          .addTransition(TaskState.RUNNING, TaskState.RUNNING,
              TaskEventType.T_ATTEMPT_LAUNCHED)
          .addTransition(TaskState.RUNNING, TaskState.SUCCEEDED,
              TaskEventType.T_ATTEMPT_SUCCEEDED,
              new AttemptSucceededTransition())
          .addTransition(TaskState.RUNNING, TaskState.KILL_WAIT,
              TaskEventType.T_KILL,
              new KillTaskTransition())
          .addTransition(TaskState.RUNNING,
              EnumSet.of(TaskState.RUNNING, TaskState.FAILED),
              TaskEventType.T_ATTEMPT_FAILED,
              new AttemptFailedOrRetryTransition())

          // Transitions from KILL_WAIT state
          .addTransition(TaskState.KILL_WAIT, TaskState.KILLED,
              TaskEventType.T_ATTEMPT_KILLED,
              ATTEMPT_KILLED_TRANSITION)
          .addTransition(TaskState.KILL_WAIT, TaskState.KILL_WAIT,
              TaskEventType.T_ATTEMPT_LAUNCHED,
              new KillTaskTransition())
          .addTransition(TaskState.KILL_WAIT, TaskState.FAILED,
              TaskEventType.T_ATTEMPT_FAILED,
              new AttemptFailedTransition())
          .addTransition(TaskState.KILL_WAIT, TaskState.KILLED,
              TaskEventType.T_ATTEMPT_SUCCEEDED,
              ATTEMPT_KILLED_TRANSITION)
              // Ignore-able transitions.
          .addTransition(TaskState.KILL_WAIT, TaskState.KILL_WAIT,
              EnumSet.of(
                  TaskEventType.T_KILL,
                  TaskEventType.T_SCHEDULE))

          // Transitions from SUCCEEDED state
          // Ignore-able transitions
          .addTransition(TaskState.SUCCEEDED, TaskState.SUCCEEDED,
              EnumSet.of(TaskEventType.T_KILL,
                  TaskEventType.T_ATTEMPT_KILLED, TaskEventType.T_ATTEMPT_SUCCEEDED, TaskEventType.T_ATTEMPT_FAILED))

          // Transitions from FAILED state
          // Ignore-able transitions
          .addTransition(TaskState.FAILED, TaskState.FAILED,
              EnumSet.of(TaskEventType.T_KILL,
                  TaskEventType.T_ATTEMPT_KILLED, TaskEventType.T_ATTEMPT_SUCCEEDED, TaskEventType.T_ATTEMPT_FAILED))

          // Transitions from KILLED state
          .addTransition(TaskState.KILLED, TaskState.KILLED, TaskEventType.T_ATTEMPT_KILLED, new KillTaskTransition())
          // Ignore-able transitions
          .addTransition(TaskState.KILLED, TaskState.KILLED,
              EnumSet.of(
                  TaskEventType.T_KILL,
                  TaskEventType.T_SCHEDULE,
                  TaskEventType.T_ATTEMPT_LAUNCHED,
                  TaskEventType.T_ATTEMPT_SUCCEEDED,
                  TaskEventType.T_ATTEMPT_FAILED))

          .installTopology();

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;


  private final Lock readLock;
  private final Lock writeLock;
  private TaskAttemptScheduleContext scheduleContext;

	public Task(Configuration conf, TaskAttemptScheduleContext scheduleContext,
              TaskId id, boolean isLeafTask, EventHandler eventHandler) {
    this.systemConf = conf;
		this.taskId = id;
    this.eventHandler = eventHandler;
    this.isLeafTask = isLeafTask;
		scan = new ArrayList<>();
    fetchMap = Maps.newHashMap();
    fragMap = Maps.newHashMap();
    shuffleFileOutputs = new ArrayList<>();
    attempts = Collections.emptyMap();
    lastAttemptId = null;
    nextAttempt = -1;
    failedAttempts = 0;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.scheduleContext = scheduleContext;

    stateMachine = stateMachineFactory.make(this);
    totalFragmentNum = 0;
    maxUrlLength = conf.getInt(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.name(),
        ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH.defaultIntVal);
	}

  public boolean isLeafTask() {
    return this.isLeafTask;
  }

  public TaskState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public TaskAttemptState getLastAttemptStatus() {
    TaskAttempt lastAttempt = getLastAttempt();
    if (lastAttempt != null) {
      return lastAttempt.getState();
    } else {
      return TaskAttemptState.TA_ASSIGNED;
    }
  }

  public TaskHistory getTaskHistory() {
    if (finalTaskHistory != null) {
      if (finalTaskHistory.getFinishTime() == 0) {
        finalTaskHistory = makeTaskHistory();
      }
      return finalTaskHistory;
    } else {
      return makeTaskHistory();
    }
  }

  private TaskHistory makeTaskHistory() {
    TaskHistory taskHistory = new TaskHistory();

    TaskAttempt lastAttempt = getLastAttempt();
    if (lastAttempt != null) {
      taskHistory.setId(lastAttempt.getId().toString());
      taskHistory.setState(lastAttempt.getState().toString());
      taskHistory.setProgress(lastAttempt.getProgress());
    }

    if(getSucceededWorker() != null) {
      taskHistory.setHostAndPort(succeededWorker.getHostAndPeerRpcPort());
    }

    taskHistory.setRetryCount(this.getRetryCount());
    taskHistory.setLaunchTime(launchTime);
    taskHistory.setFinishTime(finishTime);

    taskHistory.setNumShuffles(getShuffleOutpuNum());
    if (!getShuffleFileOutputs().isEmpty()) {
      ShuffleFileOutput shuffleFileOutputs = getShuffleFileOutputs().get(0);
      if (taskHistory.getNumShuffles() > 0) {
        taskHistory.setShuffleKey("" + shuffleFileOutputs.getPartId());
        taskHistory.setShuffleFileName(shuffleFileOutputs.getFileName());
      }
    }

    List<String> fragmentList = new ArrayList<>();
    for (FragmentProto eachFragment : getAllFragments()) {
      try {
        Fragment fragment = FragmentConvertor.convert(systemConf, eachFragment);
        fragmentList.add(fragment.toString());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        fragmentList.add("ERROR: " + eachFragment.getKind() + "," + eachFragment.getId() + ": " + e.getMessage());
      }
    }
    taskHistory.setFragments(fragmentList.toArray(new String[fragmentList.size()]));

    List<String[]> fetchList = new ArrayList<>();
    for (Map.Entry<String, Set<FetchProto>> e : getFetchMap().entrySet()) {
      for (FetchProto f : e.getValue()) {
        for (URI uri : Repartitioner.createSimpleURIs(maxUrlLength, f)) {
          fetchList.add(new String[] {e.getKey(), uri.toString()});
        }
      }
    }

    taskHistory.setFetchs(fetchList.toArray(new String[][]{}));

    List<String> dataLocationList = new ArrayList<>();
    for(DataLocation eachLocation: getDataLocations()) {
      dataLocationList.add(eachLocation.toString());
    }

    taskHistory.setDataLocations(dataLocationList.toArray(new String[dataLocationList.size()]));
    return taskHistory;
  }

	public void setLogicalPlan(LogicalNode plan) {
	  this.plan = plan;

	  LogicalNode node = plan;
	  ArrayList<LogicalNode> s = new ArrayList<>();
	  s.add(node);
	  while (!s.isEmpty()) {
	    node = s.remove(s.size()-1);
	    if (node instanceof UnaryNode) {
	      UnaryNode unary = (UnaryNode) node;
	      s.add(s.size(), unary.getChild());
	    } else if (node instanceof BinaryNode) {
	      BinaryNode binary = (BinaryNode) node;
	      s.add(s.size(), binary.getLeftChild());
	      s.add(s.size(), binary.getRightChild());
	    } else if (node instanceof ScanNode) {
	      scan.add((ScanNode)node);
	    } else if (node instanceof TableSubQueryNode) {
        s.add(((TableSubQueryNode) node).getSubQuery());
      }
	  }
	}

  private void addDataLocation(Fragment fragment) {
    ImmutableList<String> hosts = fragment.getHostNames();
    Integer[] diskIds = null;
    if (fragment instanceof FileFragment) {
      diskIds = ((FileFragment)fragment).getDiskIds();
    }
    for (int i = 0; i < hosts.size(); i++) {
      dataLocations.add(new DataLocation(hosts.get(i), diskIds == null ? DataLocation.UNKNOWN_VOLUME_ID : diskIds[i]));
    }
  }

  public void addFragment(Fragment fragment, boolean useDataLocation) {
    Set<FragmentProto> fragmentProtos;
    if (fragMap.containsKey(fragment.getInputSourceId())) {
      fragmentProtos = fragMap.get(fragment.getInputSourceId());
    } else {
      fragmentProtos = new HashSet<>();
      fragMap.put(fragment.getInputSourceId(), fragmentProtos);
    }
    fragmentProtos.add(FragmentConvertor.toFragmentProto(systemConf, fragment));
    if (useDataLocation) {
      addDataLocation(fragment);
    }
    totalFragmentNum++;
  }

  public void addFragments(Collection<Fragment> fragments) {
    for (Fragment eachFragment: fragments) {
      addFragment(eachFragment, false);
    }
  }

  public List<DataLocation> getDataLocations() {
    return dataLocations;
  }

  public WorkerConnectionInfo getSucceededWorker() {
    return succeededWorker;
  }
	
	public void addFetches(String tableId, Collection<FetchProto> fetches) {
	  Set<FetchProto> fetchSet;
    if (fetchMap.containsKey(tableId)) {
      fetchSet = fetchMap.get(tableId);
    } else {
      fetchSet = Sets.newHashSet();
    }
    fetchSet.addAll(fetches);
    fetchMap.put(tableId, fetchSet);
	}
	
	public void setFetches(Map<String, Set<FetchProto>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
	}

  public Collection<FragmentProto> getAllFragments() {
    Set<FragmentProto> fragmentProtos = new HashSet<>();
    for (Set<FragmentProto> eachFragmentSet : fragMap.values()) {
      fragmentProtos.addAll(eachFragmentSet);
    }
    return fragmentProtos;
  }
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public TaskId getId() {
		return taskId;
	}

	public Collection<Set<FetchProto>> getFetches() {
	  return fetchMap.values();
	}

  public Map<String, Set<FetchProto>> getFetchMap() {
    return fetchMap;
  }

	@Override
	public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(plan.getType() + " \n");
		for (Entry<String, Set<FragmentProto>> e : fragMap.entrySet()) {
		  builder.append(e.getKey()).append(" : ");
      for (FragmentProto fragment : e.getValue()) {
        builder.append(fragment).append(", ");
      }
		}
		for (Entry<String, Set<FetchProto>> e : fetchMap.entrySet()) {
      builder.append(e.getKey()).append(" : ");
      for (FetchProto t : e.getValue()) {
        for (URI uri : Repartitioner.createFullURIs(maxUrlLength, t)){
          builder.append(uri).append(" ");
        }
      }
    }
		
		return builder.toString();
	}
	
	public void setStats(TableStats stats) {
	  this.stats = stats;
	}
	
	public void setShuffleFileOutputs(List<ShuffleFileOutput> partitions) {
	  this.shuffleFileOutputs = Collections.unmodifiableList(partitions);
	}
	
	public TableStats getStats() {
	  return this.stats;
	}
	
	public List<ShuffleFileOutput> getShuffleFileOutputs() {
	  return this.shuffleFileOutputs;
	}
	
	public int getShuffleOutpuNum() {
	  return this.shuffleFileOutputs.size();
	}

  public TaskAttempt newAttempt() {
    TaskAttempt attempt = new TaskAttempt(scheduleContext,
        QueryIdFactory.newTaskAttemptId(this.getId(), ++nextAttempt),
        this, eventHandler);
    lastAttemptId = attempt.getId();
    return attempt;
  }

  public TaskAttempt getAttempt(TaskAttemptId attemptId) {
    return attempts.get(attemptId);
  }

  public TaskAttempt getAttempt(int attempt) {
    return this.attempts.get(QueryIdFactory.newTaskAttemptId(this.getId(), attempt));
  }

  public TaskAttempt getLastAttempt() {
    return getAttempt(this.lastAttemptId);
  }

  public TaskAttempt getSuccessfulAttempt() {
    readLock.lock();
    try {
      if (null == successfulAttempt) {
        return null;
      }
      return attempts.get(successfulAttempt);
    } finally {
      readLock.unlock();
    }
  }

  public int getRetryCount () {
    return this.nextAttempt;
  }

  public int getTotalFragmentNum() {
    return totalFragmentNum;
  }

  private static class InitialScheduleTransition implements
    SingleArcTransition<Task, TaskEvent> {

    @Override
    public void transition(Task task, TaskEvent taskEvent) {
      task.addAndScheduleAttempt();
    }
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  @VisibleForTesting
  public void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }

  @VisibleForTesting
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public long getRunningTime() {
    if(finishTime > 0) {
      return finishTime - launchTime;
    } else {
      return System.currentTimeMillis() - launchTime;
    }
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt() {
    // Create new task attempt
    TaskAttempt attempt = newAttempt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getId());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getId(), attempt);
        break;

      case 1:
        Map<TaskAttemptId, TaskAttempt> newAttempts
            = new LinkedHashMap<>(3);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        attempts.put(attempt.getId(), attempt);
        break;

      default:
        attempts.put(attempt.getId(), attempt);
        break;
    }

    if (failedAttempts > 0) {
      eventHandler.handle(new TaskAttemptScheduleEvent(systemConf, attempt.getId(),
          TaskAttemptEventType.TA_RESCHEDULE));
    } else {
      eventHandler.handle(new TaskAttemptScheduleEvent(systemConf, attempt.getId(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
  }

  private void finishTask() {
    this.finishTime = System.currentTimeMillis();
    finalTaskHistory = makeTaskHistory();
  }

  private static class KillNewTaskTransition implements SingleArcTransition<Task, TaskEvent> {

    @Override
    public void transition(Task task, TaskEvent taskEvent) {
      task.eventHandler.handle(new StageTaskEvent(task.getId(), TaskState.KILLED));
    }
  }

  private static class KillTaskTransition implements SingleArcTransition<Task, TaskEvent> {

    @Override
    public void transition(Task task, TaskEvent taskEvent) {
      task.finishTask();
      task.eventHandler.handle(new TaskAttemptEvent(task.lastAttemptId, TaskAttemptEventType.TA_KILL));
    }
  }

  private static class AttemptKilledTransition implements SingleArcTransition<Task, TaskEvent>{

    @Override
    public void transition(Task task, TaskEvent event) {
      task.finishTask();
      task.eventHandler.handle(new StageTaskEvent(task.getId(), TaskState.KILLED));
    }
  }

  private static class AttemptSucceededTransition
      implements SingleArcTransition<Task, TaskEvent>{

    @Override
    public void transition(Task task,
                           TaskEvent event) {
      if (!(event instanceof TaskTAttemptEvent)) {
        throw new IllegalArgumentException("event should be a TaskTAttemptEvent type.");
      }
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      TaskAttempt attempt = task.attempts.get(attemptEvent.getTaskAttemptId());

      task.successfulAttempt = attemptEvent.getTaskAttemptId();
      task.succeededWorker = attempt.getWorkerConnectionInfo();

      task.finishTask();
      task.eventHandler.handle(new StageTaskEvent(event.getTaskId(), TaskState.SUCCEEDED));
    }
  }

  private static class AttemptLaunchedTransition implements SingleArcTransition<Task, TaskEvent> {
    @Override
    public void transition(Task task,
                           TaskEvent event) {
      if (!(event instanceof TaskTAttemptEvent)) {
        throw new IllegalArgumentException("event should be a TaskTAttemptEvent type.");
      }
      task.launchTime = System.currentTimeMillis();
    }
  }

  private static class AttemptFailedTransition implements SingleArcTransition<Task, TaskEvent> {
    @Override
    public void transition(Task task, TaskEvent event) {
      TaskTAttemptFailedEvent attemptEvent = TUtil.checkTypeAndGet(event, TaskTAttemptFailedEvent.class);

      LOG.info("=============================================================");
      LOG.info(">>> Task Failed: " + attemptEvent.getTaskAttemptId() + " <<<");
      LOG.info("=============================================================");
      task.failedAttempts++;
      task.finishedAttempts++;

      task.finishTask();
      task.eventHandler.handle(new StageTaskFailedEvent(task.getId(), attemptEvent.getException()));
    }
  }

  private static class AttemptFailedOrRetryTransition implements
    MultipleArcTransition<Task, TaskEvent, TaskState> {

    @Override
    public TaskState transition(Task task, TaskEvent taskEvent) {
      TaskTAttemptFailedEvent attemptEvent = TUtil.checkTypeAndGet(taskEvent, TaskTAttemptFailedEvent.class);

      task.failedAttempts++;
      task.finishedAttempts++;
      boolean retry = task.failedAttempts < task.maxAttempts;

      LOG.info("====================================================================================");
      LOG.info(">>> Task Failed: " + attemptEvent.getTaskAttemptId() + ", " +
          "retry:" + retry + ", attempts:" +  task.failedAttempts + " <<<");
      LOG.info("====================================================================================");

      if (retry) {
        if (task.successfulAttempt == null) {
          task.addAndScheduleAttempt();
        }
      } else {
        task.finishTask();
        task.eventHandler.handle(new StageTaskFailedEvent(task.getId(), attemptEvent.getException()));
        return TaskState.FAILED;
      }

      return task.getState();
    }
  }

  @Override
  public void handle(TaskEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskId() + " of type "
          + event.getType());
    }

    try {
      writeLock.lock();
      TaskState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", eventType:" + event.getType().name()
            + ", oldState:" + oldState.name()
            + ", nextState:" + getState().name()
            , e);
        eventHandler.handle(new QueryEvent(TajoIdUtils.parseQueryId(getId().toString()),
            QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
        if (oldState != getState()) {
          LOG.debug(taskId + " Task Transitioned from " + oldState + " to "
              + getState());
        }
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  public void setIntermediateData(Collection<IntermediateEntry> partitions) {
    this.intermediateData = new ArrayList<>(partitions);
  }

  public List<IntermediateEntry> getIntermediateData() {
    return this.intermediateData;
  }

  public static class PullHost implements Cloneable {
    String host;
    int port;
    int hashCode;

    public PullHost(String pullServerAddr, int pullServerPort){
      this.host = pullServerAddr;
      this.port = pullServerPort;
      this.hashCode = Objects.hashCode(host, port);
    }
    public String getHost() {
      return host;
    }

    public int getPort() {
      return this.port;
    }

    public String getPullAddress() {
      return host + ":" + port;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PullHost) {
        PullHost other = (PullHost) obj;
        return host.equals(other.host) && port == other.port;
      }

      return false;
    }

    @Override
    public PullHost clone() throws CloneNotSupportedException {
      PullHost newPullHost = (PullHost) super.clone();
      newPullHost.host = host;
      newPullHost.port = port;
      newPullHost.hashCode = Objects.hashCode(newPullHost.host, newPullHost.port);
      return newPullHost;
    }

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

  public static class IntermediateEntry {
    ExecutionBlockId ebId;
    int taskId;
    int attemptId;
    int partId;
    PullHost host;
    long volume;
    List<Pair<Long, Integer>> pages;
    List<Pair<Long, Pair<Integer, Integer>>> failureRowNums;

    public IntermediateEntry(IntermediateEntryProto proto) {
      this.ebId = new ExecutionBlockId(proto.getEbId());
      this.taskId = proto.getTaskId();
      this.attemptId = proto.getAttemptId();
      this.partId = proto.getPartId();

      String[] pullHost = proto.getHost().split(":");
      this.host = new PullHost(pullHost[0], Integer.parseInt(pullHost[1]));
      this.volume = proto.getVolume();

      failureRowNums = new ArrayList<>();
      for (FailureIntermediateProto eachFailure: proto.getFailuresList()) {

        failureRowNums.add(new Pair(eachFailure.getPagePos(),
            new Pair(eachFailure.getStartRowNum(), eachFailure.getEndRowNum())));
      }

      pages = new ArrayList<>();
      for (IntermediateEntryProto.PageProto eachPage: proto.getPagesList()) {
        pages.add(new Pair(eachPage.getPos(), eachPage.getLength()));
      }
    }

    public IntermediateEntry(int taskId, int attemptId, int partId, PullHost host) {
      this.taskId = taskId;
      this.attemptId = attemptId;
      this.partId = partId;
      this.host = host;
    }

    public IntermediateEntry(int taskId, int attemptId, int partId, PullHost host, long volume) {
      this.taskId = taskId;
      this.attemptId = attemptId;
      this.partId = partId;
      this.host = host;
      this.volume = volume;
    }

    public ExecutionBlockId getEbId() {
      return ebId;
    }

    public void setEbId(ExecutionBlockId ebId) {
      this.ebId = ebId;
    }

    public int getTaskId() {
      return this.taskId;
    }

    public int getAttemptId() {
      return this.attemptId;
    }

    public int getPartId() {
      return this.partId;
    }

    public PullHost getPullHost() {
      return this.host;
    }

    public long getVolume() {
      return this.volume;
    }

    public long setVolume(long volume) {
      return this.volume = volume;
    }

    public List<Pair<Long, Integer>> getPages() {
      return pages;
    }

    public void setPages(List<Pair<Long, Integer>> pages) {
      this.pages = pages;
    }

    public List<Pair<Long, Pair<Integer, Integer>>> getFailureRowNums() {
      return failureRowNums;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(ebId, taskId, partId, attemptId, host);
    }

    public List<Pair<Long, Long>> split(long firstSplitVolume, long splitVolume) {
      List<Pair<Long, Long>> splits = new ArrayList<>();

      if (pages == null || pages.isEmpty()) {
        return splits;
      }
      int pageSize = pages.size();

      long currentOffset = -1;
      long currentBytes = 0;

      long realSplitVolume = firstSplitVolume > 0 ? firstSplitVolume : splitVolume;
      for (Pair<Long, Integer> eachPage : pages) {
        if (currentOffset == -1) {
          currentOffset = eachPage.getFirst();
        }
        if (currentBytes > 0 && currentBytes + eachPage.getSecond() >= realSplitVolume) {
          splits.add(new Pair(currentOffset, currentBytes));
          currentOffset = eachPage.getFirst();
          currentBytes = 0;
          realSplitVolume = splitVolume;
        }

        currentBytes += eachPage.getSecond();
      }

      //add last
      if (currentBytes > 0) {
        splits.add(new Pair(currentOffset, currentBytes));
      }
      return splits;
    }
  }
}
