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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.QueryUnitId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.ipc.TajoWorkerProtocol.Partition;
import org.apache.tajo.master.TaskState;
import org.apache.tajo.master.event.*;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.TajoIdUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class QueryUnit implements EventHandler<TaskEvent> {
  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(QueryUnit.class);

  private final Configuration systemConf;
	private QueryUnitId taskId;
  private EventHandler eventHandler;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	
	private Map<String, FragmentProto> fragMap;
	private Map<String, Set<URI>> fetchMap;
	
  private List<Partition> partitions;
	private TableStats stats;
  private List<DataLocation> dataLocations;
  private final boolean isLeafTask;
  private List<IntermediateEntry> intermediateData;

  private Map<QueryUnitAttemptId, QueryUnitAttempt> attempts;
  private final int maxAttempts = 3;
  private Integer lastAttemptId;

  private QueryUnitAttemptId successfulAttempt;
  private String succeededHost;
  private int succeededPullServerPort;

  private int failedAttempts;
  private int finishedAttempts; // finish are total of success, failed and killed

  private long launchTime;
  private long finishTime;

  protected static final StateMachineFactory
      <QueryUnit, TaskState, TaskEventType, TaskEvent> stateMachineFactory =
      new StateMachineFactory
          <QueryUnit, TaskState, TaskEventType, TaskEvent>(TaskState.NEW)

      .addTransition(TaskState.NEW, TaskState.SCHEDULED,
          TaskEventType.T_SCHEDULE, new InitialScheduleTransition())

       .addTransition(TaskState.SCHEDULED, TaskState.RUNNING,
           TaskEventType.T_ATTEMPT_LAUNCHED, new AttemptLaunchedTransition())

        .addTransition(TaskState.RUNNING, TaskState.RUNNING,
           TaskEventType.T_ATTEMPT_LAUNCHED)

       .addTransition(TaskState.RUNNING, TaskState.SUCCEEDED,
           TaskEventType.T_ATTEMPT_SUCCEEDED, new AttemptSucceededTransition())

       .addTransition(TaskState.RUNNING,
            EnumSet.of(TaskState.RUNNING, TaskState.FAILED),
            TaskEventType.T_ATTEMPT_FAILED, new AttemptFailedTransition())



      .installTopology();
  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;


  private final Lock readLock;
  private final Lock writeLock;

	public QueryUnit(Configuration conf, QueryUnitId id,
                   boolean isLeafTask, EventHandler eventHandler) {
    this.systemConf = conf;
		this.taskId = id;
    this.eventHandler = eventHandler;
    this.isLeafTask = isLeafTask;
		scan = new ArrayList<ScanNode>();
    fetchMap = Maps.newHashMap();
    fragMap = Maps.newHashMap();
    partitions = new ArrayList<Partition>();
    attempts = Collections.emptyMap();
    lastAttemptId = -1;
    failedAttempts = 0;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
	}

  public boolean isLeafTask() {
    return this.isLeafTask;
  }

  public void setDataLocations(FileFragment fragment) {
    String[] hosts = fragment.getHosts();
    int[] volumeIds = fragment.getDiskIds();
    this.dataLocations = new ArrayList<DataLocation>(hosts.length);

    for (int i = 0; i < hosts.length; i++) {
      this.dataLocations.add(new DataLocation(hosts[i], volumeIds[i]));
    }
  }

  public List<DataLocation> getDataLocations() {
    return this.dataLocations;
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

  @Deprecated
  public void setFragment(String tableId, FileFragment fragment) {
    this.fragMap.put(tableId, fragment.getProto());
    setDataLocations(fragment);
  }

  public void setFragment2(FileFragment fragment) {
    this.fragMap.put(fragment.getTableName(), fragment.getProto());
    setDataLocations(fragment);
  }

  public String getSucceededHost() {
    return succeededHost;
  }

	public void addFetch(String tableId, String uri) throws URISyntaxException {
	  this.addFetch(tableId, new URI(uri));
	}
	
	public void addFetch(String tableId, URI uri) {
	  Set<URI> uris;
	  if (fetchMap.containsKey(tableId)) {
	    uris = fetchMap.get(tableId);
	  } else {
	    uris = Sets.newHashSet();
	  }
	  uris.add(uri);
    fetchMap.put(tableId, uris);
	}
	
	public void addFetches(String tableId, Collection<URI> urilist) {
	  Set<URI> uris;
    if (fetchMap.containsKey(tableId)) {
      uris = fetchMap.get(tableId);
    } else {
      uris = Sets.newHashSet();
    }
    uris.addAll(urilist);
    fetchMap.put(tableId, uris);
	}
	
	public void setFetches(Map<String, Set<URI>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
	}

  public Collection<FragmentProto> getAllFragments() {
    return fragMap.values();
  }
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public QueryUnitId getId() {
		return taskId;
	}
	
	public Collection<URI> getFetchHosts(String tableId) {
	  return fetchMap.get(tableId);
	}
	
	public Collection<Set<URI>> getFetches() {
	  return fetchMap.values();
	}
	
	public Collection<URI> getFetch(ScanNode scan) {
	  return this.fetchMap.get(scan.getTableName());
	}

	public String getOutputName() {
		return this.store.getTableName();
	}
	
	public Schema getOutputSchema() {
	  return this.store.getOutSchema();
	}
	
	public StoreTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan.toArray(new ScanNode[scan.size()]);
	}
	
	@Override
	public String toString() {
		String str = new String(plan.getType() + " \n");
		for (Entry<String, FragmentProto> e : fragMap.entrySet()) {
		  str += e.getKey() + " : ";
      str += e.getValue() + " ";
		}
		for (Entry<String, Set<URI>> e : fetchMap.entrySet()) {
      str += e.getKey() + " : ";
      for (URI t : e.getValue()) {
        str += t + " ";
      }
    }
		
		return str;
	}
	
	public void setStats(TableStats stats) {
	  this.stats = stats;
	}
	
	public void setPartitions(List<Partition> partitions) {
	  this.partitions = Collections.unmodifiableList(partitions);
	}
	
	public TableStats getStats() {
	  return this.stats;
	}
	
	public List<Partition> getPartitions() {
	  return this.partitions;
	}
	
	public int getPartitionNum() {
	  return this.partitions.size();
	}

  public QueryUnitAttempt newAttempt() {
    QueryUnitAttempt attempt = new QueryUnitAttempt(QueryIdFactory.newQueryUnitAttemptId(
        this.getId(), ++lastAttemptId), this, eventHandler);
    return attempt;
  }

  public QueryUnitAttempt getAttempt(QueryUnitAttemptId attemptId) {
    return attempts.get(attemptId);
  }

  public QueryUnitAttempt getAttempt(int attempt) {
    return this.attempts.get(QueryIdFactory.newQueryUnitAttemptId(this.getId(), attempt));
  }

  public QueryUnitAttempt getLastAttempt() {
    return this.attempts.get(this.lastAttemptId);
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
    return this.lastAttemptId;
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

  private static class AttemptSucceededTransition
      implements SingleArcTransition<QueryUnit, TaskEvent>{

    @Override
    public void transition(QueryUnit task,
                           TaskEvent event) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      QueryUnitAttempt attempt = task.attempts.get(
          attemptEvent.getTaskAttemptId());

      task.successfulAttempt = attemptEvent.getTaskAttemptId();
      task.succeededHost = attempt.getHost();
      task.finishTime = System.currentTimeMillis();
      task.succeededPullServerPort = attempt.getPullServerPort();
      task.eventHandler.handle(new SubQueryTaskEvent(event.getTaskId(),
          SubQueryEventType.SQ_TASK_COMPLETED));
    }
  }

  private static class AttemptLaunchedTransition implements SingleArcTransition<QueryUnit, TaskEvent> {
    @Override
    public void transition(QueryUnit task,
                           TaskEvent event) {
      task.launchTime = System.currentTimeMillis();
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<QueryUnit, TaskEvent, TaskState> {

    @Override
    public TaskState transition(QueryUnit task, TaskEvent taskEvent) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) taskEvent;
      LOG.info("=============================================================");
      LOG.info(">>> Task Failed: " + attemptEvent.getTaskAttemptId() + " <<<");
      LOG.info("=============================================================");
      task.failedAttempts++;
      task.finishedAttempts++;

      if (task.failedAttempts < task.maxAttempts) {
        if (task.successfulAttempt == null) {
          task.addAndScheduleAttempt();
        }
      } else {
        task.eventHandler.handle(
            new SubQueryTaskEvent(task.getId(), SubQueryEventType.SQ_FAILED));
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
        LOG.error("Can't handle this event at current state", e);
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

  public static class IntermediateEntry {
    int taskId;
    int attemptId;
    int partitionId;
    String pullHost;
    int port;

    public IntermediateEntry(int taskId, int attemptId, int partitionId,
                             String pullServerAddr, int pullServerPort) {
      this.taskId = taskId;
      this.attemptId = attemptId;
      this.partitionId = partitionId;
      this.pullHost = pullServerAddr;
      this.port = pullServerPort;
    }

    public int getTaskId() {
      return this.taskId;
    }

    public int getAttemptId() {
      return this.attemptId;
    }

    public int getPartitionId() {
      return this.partitionId;
    }

    public String getPullHost() {
      return this.pullHost;
    }

    public int getPullPort() {
      return port;
    }

    public String getPullAddress() {
      return pullHost + ":" + port;
    }
  }
}
