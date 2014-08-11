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

package org.apache.tajo.master.querymaster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
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
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.QueryUnitId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.FragmentPair;
import org.apache.tajo.master.TaskState;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.worker.FetchImpl;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleFileOutput;

public class QueryUnit implements EventHandler<TaskEvent> {
  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(QueryUnit.class);

  private final Configuration systemConf;
	private QueryUnitId taskId;
  private EventHandler eventHandler;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	
	private Map<String, Set<FragmentProto>> fragMap;
	private Map<String, Set<FetchImpl>> fetchMap;

  private int totalFragmentNum;

  private List<ShuffleFileOutput> shuffleFileOutputs;
	private TableStats stats;
  private final boolean isLeafTask;
  private List<IntermediateEntry> intermediateData;

  private Map<QueryUnitAttemptId, QueryUnitAttempt> attempts;
  private final int maxAttempts = 3;
  private Integer nextAttempt = -1;
  private QueryUnitAttemptId lastAttemptId;

  private QueryUnitAttemptId successfulAttempt;
  private String succeededHost;
  private int succeededPullServerPort;

  private int failedAttempts;
  private int finishedAttempts; // finish are total of success, failed and killed

  private long launchTime;
  private long finishTime;

  private List<DataLocation> dataLocations = Lists.newArrayList();

  private static final AttemptKilledTransition ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();

  protected static final StateMachineFactory
      <QueryUnit, TaskState, TaskEventType, TaskEvent> stateMachineFactory =
      new StateMachineFactory <QueryUnit, TaskState, TaskEventType, TaskEvent>(TaskState.NEW)

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
              EnumSet.of(TaskEventType.T_KILL, TaskEventType.T_ATTEMPT_KILLED, TaskEventType.T_ATTEMPT_SUCCEEDED))

          // Transitions from FAILED state
          // Ignore-able transitions
          .addTransition(TaskState.FAILED, TaskState.FAILED,
              EnumSet.of(TaskEventType.T_KILL, TaskEventType.T_ATTEMPT_KILLED, TaskEventType.T_ATTEMPT_SUCCEEDED))

          // Transitions from KILLED state
          .addTransition(TaskState.KILLED, TaskState.KILLED,
              TaskEventType.T_ATTEMPT_KILLED,
              new KillTaskTransition())

          .installTopology();

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;


  private final Lock readLock;
  private final Lock writeLock;
  private QueryUnitAttemptScheduleContext scheduleContext;

	public QueryUnit(Configuration conf, QueryUnitAttemptScheduleContext scheduleContext,
                   QueryUnitId id, boolean isLeafTask, EventHandler eventHandler) {
    this.systemConf = conf;
		this.taskId = id;
    this.eventHandler = eventHandler;
    this.isLeafTask = isLeafTask;
		scan = new ArrayList<ScanNode>();
    fetchMap = Maps.newHashMap();
    fragMap = Maps.newHashMap();
    shuffleFileOutputs = new ArrayList<ShuffleFileOutput>();
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

	public void setLogicalPlan(LogicalNode plan) {
	  this.plan = plan;

	  LogicalNode node = plan;
	  ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
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

  private void addDataLocation(FileFragment fragment) {
    String[] hosts = fragment.getHosts();
    int[] diskIds = fragment.getDiskIds();
    for (int i = 0; i < hosts.length; i++) {
      dataLocations.add(new DataLocation(hosts[i], diskIds[i]));
    }
  }

  public void addFragment(FileFragment fragment, boolean useDataLocation) {
    Set<FragmentProto> fragmentProtos;
    if (fragMap.containsKey(fragment.getTableName())) {
      fragmentProtos = fragMap.get(fragment.getTableName());
    } else {
      fragmentProtos = new HashSet<FragmentProto>();
      fragMap.put(fragment.getTableName(), fragmentProtos);
    }
    fragmentProtos.add(fragment.getProto());
    if (useDataLocation) {
      addDataLocation(fragment);
    }
    totalFragmentNum++;
  }

  public void addFragments(Collection<FileFragment> fragments) {
    for (FileFragment eachFragment: fragments) {
      addFragment(eachFragment, false);
    }
  }

  public void setFragment(FragmentPair[] fragmentPairs) {
    for (FragmentPair eachFragmentPair : fragmentPairs) {
      this.addFragment(eachFragmentPair.getLeftFragment(), true);
      if (eachFragmentPair.getRightFragment() != null) {
        this.addFragment(eachFragmentPair.getRightFragment(), true);
      }
    }
  }

  public List<DataLocation> getDataLocations() {
    return dataLocations;
  }

  public String getSucceededHost() {
    return succeededHost;
  }
	
	public void addFetches(String tableId, Collection<FetchImpl> fetches) {
	  Set<FetchImpl> fetchSet;
    if (fetchMap.containsKey(tableId)) {
      fetchSet = fetchMap.get(tableId);
    } else {
      fetchSet = Sets.newHashSet();
    }
    fetchSet.addAll(fetches);
    fetchMap.put(tableId, fetchSet);
	}
	
	public void setFetches(Map<String, Set<FetchImpl>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
	}

  public Collection<FragmentProto> getAllFragments() {
    Set<FragmentProto> fragmentProtos = new HashSet<FragmentProto>();
    for (Set<FragmentProto> eachFragmentSet : fragMap.values()) {
      fragmentProtos.addAll(eachFragmentSet);
    }
    return fragmentProtos;
  }
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public QueryUnitId getId() {
		return taskId;
	}
	
	public Collection<FetchImpl> getFetchHosts(String tableId) {
	  return fetchMap.get(tableId);
	}
	
	public Collection<Set<FetchImpl>> getFetches() {
	  return fetchMap.values();
	}

  public Map<String, Set<FetchImpl>> getFetchMap() {
    return fetchMap;
  }
	
	public Collection<FetchImpl> getFetch(ScanNode scan) {
	  return this.fetchMap.get(scan.getTableName());
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan.toArray(new ScanNode[scan.size()]);
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
		for (Entry<String, Set<FetchImpl>> e : fetchMap.entrySet()) {
      builder.append(e.getKey()).append(" : ");
      for (FetchImpl t : e.getValue()) {
        for (URI uri : t.getURIs()){
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

  public QueryUnitAttempt newAttempt() {
    QueryUnitAttempt attempt = new QueryUnitAttempt(scheduleContext,
        QueryIdFactory.newQueryUnitAttemptId(this.getId(), ++nextAttempt),
        this, eventHandler);
    lastAttemptId = attempt.getId();
    return attempt;
  }

  public QueryUnitAttempt getAttempt(QueryUnitAttemptId attemptId) {
    return attempts.get(attemptId);
  }

  public QueryUnitAttempt getAttempt(int attempt) {
    return this.attempts.get(QueryIdFactory.newQueryUnitAttemptId(this.getId(), attempt));
  }

  public QueryUnitAttempt getLastAttempt() {
    return getAttempt(this.lastAttemptId);
  }

  public QueryUnitAttempt getSuccessfulAttempt() {
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
    SingleArcTransition<QueryUnit, TaskEvent> {

    @Override
    public void transition(QueryUnit task, TaskEvent taskEvent) {
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
    QueryUnitAttempt attempt = newAttempt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getId());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getId(), attempt);
        break;

      case 1:
        Map<QueryUnitAttemptId, QueryUnitAttempt> newAttempts
            = new LinkedHashMap<QueryUnitAttemptId, QueryUnitAttempt>(3);
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
  }

  private static class KillNewTaskTransition implements SingleArcTransition<QueryUnit, TaskEvent> {

    @Override
    public void transition(QueryUnit task, TaskEvent taskEvent) {
      task.eventHandler.handle(new SubQueryTaskEvent(task.getId(), TaskState.KILLED));
    }
  }

  private static class KillTaskTransition implements SingleArcTransition<QueryUnit, TaskEvent> {

    @Override
    public void transition(QueryUnit task, TaskEvent taskEvent) {
      task.finishTask();
      task.eventHandler.handle(new TaskAttemptEvent(task.lastAttemptId, TaskAttemptEventType.TA_KILL));
    }
  }

  private static class AttemptKilledTransition implements SingleArcTransition<QueryUnit, TaskEvent>{

    @Override
    public void transition(QueryUnit task, TaskEvent event) {
      task.finishTask();
      task.eventHandler.handle(new SubQueryTaskEvent(task.getId(), TaskState.KILLED));
    }
  }

  private static class AttemptSucceededTransition
      implements SingleArcTransition<QueryUnit, TaskEvent>{

    @Override
    public void transition(QueryUnit task,
                           TaskEvent event) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      QueryUnitAttempt attempt = task.attempts.get(attemptEvent.getTaskAttemptId());

      task.successfulAttempt = attemptEvent.getTaskAttemptId();
      task.succeededHost = attempt.getHost();
      task.succeededPullServerPort = attempt.getPullServerPort();

      task.finishTask();
      task.eventHandler.handle(new SubQueryTaskEvent(event.getTaskId(), TaskState.SUCCEEDED));
    }
  }

  private static class AttemptLaunchedTransition implements SingleArcTransition<QueryUnit, TaskEvent> {
    @Override
    public void transition(QueryUnit task,
                           TaskEvent event) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      QueryUnitAttempt attempt = task.attempts.get(attemptEvent.getTaskAttemptId());
      task.launchTime = System.currentTimeMillis();
      task.succeededHost = attempt.getHost();
    }
  }

  private static class AttemptFailedTransition implements SingleArcTransition<QueryUnit, TaskEvent> {
    @Override
    public void transition(QueryUnit task, TaskEvent event) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      LOG.info("=============================================================");
      LOG.info(">>> Task Failed: " + attemptEvent.getTaskAttemptId() + " <<<");
      LOG.info("=============================================================");
      task.failedAttempts++;
      task.finishedAttempts++;

      task.finishTask();
      task.eventHandler.handle(new SubQueryTaskEvent(task.getId(), TaskState.FAILED));
    }
  }

  private static class AttemptFailedOrRetryTransition implements
    MultipleArcTransition<QueryUnit, TaskEvent, TaskState> {

    @Override
    public TaskState transition(QueryUnit task, TaskEvent taskEvent) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) taskEvent;
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
        task.eventHandler.handle(new SubQueryTaskEvent(task.getId(), TaskState.FAILED));
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
    this.intermediateData = new ArrayList<IntermediateEntry>(partitions);
  }

  public List<IntermediateEntry> getIntermediateData() {
    return this.intermediateData;
  }

  public static class PullHost implements Cloneable {
    String host;
    int port;
    public PullHost(String pullServerAddr, int pullServerPort){
      this.host = pullServerAddr;
      this.port = pullServerPort;
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
      return Objects.hashCode(host, port);
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
      return newPullHost;
    }
  }

  public static class IntermediateEntry {
    ExecutionBlockId ebId;
    int taskId;
    int attemptId;
    int partId;
    PullHost host;
    long volume;

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

    @Override
    public int hashCode() {
      return Objects.hashCode(ebId, taskId, partId, attemptId, host);
    }
  }
}
