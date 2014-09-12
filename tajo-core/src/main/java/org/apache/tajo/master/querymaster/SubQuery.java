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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty;
import org.apache.tajo.ipc.TajoWorkerProtocol.IntermediateEntryProto;
import org.apache.tajo.master.*;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.worker.FetchImpl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType;


/**
 * SubQuery plays a role in controlling an ExecutionBlock and is a finite state machine.
 */
public class SubQuery implements EventHandler<SubQueryEvent> {

  private static final Log LOG = LogFactory.getLog(SubQuery.class);

  private MasterPlan masterPlan;
  private ExecutionBlock block;
  private int priority;
  private Schema schema;
  private TableMeta meta;
  private TableStats resultStatistics;
  private TableStats inputStatistics;
  private EventHandler<Event> eventHandler;
  private final AbstractStorageManager sm;
  private AbstractTaskScheduler taskScheduler;
  private QueryMasterTask.QueryMasterTaskContext context;
  private final List<String> diagnostics = new ArrayList<String>();
  private SubQueryState subQueryState;

  private long startTime;
  private long finishTime;

  volatile Map<QueryUnitId, QueryUnit> tasks = new ConcurrentHashMap<QueryUnitId, QueryUnit>();
  volatile Map<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final ContainerLaunchTransition CONTAINER_LAUNCH_TRANSITION = new ContainerLaunchTransition();
  private static final TaskCompletedTransition TASK_COMPLETED_TRANSITION = new TaskCompletedTransition();
  private static final AllocatedContainersCancelTransition CONTAINERS_CANCEL_TRANSITION =
      new AllocatedContainersCancelTransition();
  private static final SubQueryCompleteTransition SUBQUERY_COMPLETED_TRANSITION =
      new SubQueryCompleteTransition();
  private StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent> stateMachine;

  protected static final StateMachineFactory<SubQuery, SubQueryState,
      SubQueryEventType, SubQueryEvent> stateMachineFactory =
      new StateMachineFactory <SubQuery, SubQueryState,
          SubQueryEventType, SubQueryEvent> (SubQueryState.NEW)

          // Transitions from NEW state
          .addTransition(SubQueryState.NEW,
              EnumSet.of(SubQueryState.INITED, SubQueryState.ERROR, SubQueryState.SUCCEEDED),
              SubQueryEventType.SQ_INIT,
              new InitAndRequestContainer())
          .addTransition(SubQueryState.NEW, SubQueryState.NEW,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.NEW, SubQueryState.KILLED,
              SubQueryEventType.SQ_KILL)
          .addTransition(SubQueryState.NEW, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(SubQueryState.INITED, SubQueryState.RUNNING,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINER_LAUNCH_TRANSITION)
          .addTransition(SubQueryState.INITED, SubQueryState.INITED,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.INITED, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_KILL)
          .addTransition(SubQueryState.INITED, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINER_LAUNCH_TRANSITION)
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_TASK_COMPLETED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.RUNNING,
              EnumSet.of(SubQueryState.SUCCEEDED, SubQueryState.FAILED),
              SubQueryEventType.SQ_SUBQUERY_COMPLETED,
              SUBQUERY_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_FAILED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.RUNNING, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_KILL,
              new KillTasksTransition())
          .addTransition(SubQueryState.RUNNING, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able Transition
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_START)

          // Transitions from KILL_WAIT state
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.KILL_WAIT,
              EnumSet.of(SubQueryEventType.SQ_KILL))
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_TASK_COMPLETED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.KILL_WAIT,
              EnumSet.of(SubQueryState.SUCCEEDED, SubQueryState.FAILED, SubQueryState.KILLED),
              SubQueryEventType.SQ_SUBQUERY_COMPLETED,
              SUBQUERY_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.KILL_WAIT,
              SubQueryEventType.SQ_FAILED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(SubQueryState.KILL_WAIT, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

              // Transitions from SUCCEEDED state
          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
              // Ignore-able events
          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.SUCCEEDED,
              EnumSet.of(
                  SubQueryEventType.SQ_START,
                  SubQueryEventType.SQ_KILL,
                  SubQueryEventType.SQ_CONTAINER_ALLOCATED))

          // Transitions from FAILED state
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(SubQueryState.FAILED, SubQueryState.ERROR,
              SubQueryEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              EnumSet.of(
                  SubQueryEventType.SQ_START,
                  SubQueryEventType.SQ_KILL,
                  SubQueryEventType.SQ_CONTAINER_ALLOCATED,
                  SubQueryEventType.SQ_FAILED))

          // Transitions from FAILED state
          .addTransition(SubQueryState.ERROR, SubQueryState.ERROR,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(SubQueryState.ERROR, SubQueryState.ERROR,
              SubQueryEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // Ignore-able transitions
          .addTransition(SubQueryState.ERROR, SubQueryState.ERROR,
              EnumSet.of(
                  SubQueryEventType.SQ_START,
                  SubQueryEventType.SQ_KILL,
                  SubQueryEventType.SQ_FAILED,
                  SubQueryEventType.SQ_INTERNAL_ERROR,
                  SubQueryEventType.SQ_SUBQUERY_COMPLETED))

          .installTopology();

  private final Lock readLock;
  private final Lock writeLock;

  private int totalScheduledObjectsCount;
  private int succeededObjectCount = 0;
  private int completedTaskCount = 0;
  private int succeededTaskCount = 0;
  private int killedObjectCount = 0;
  private int failedObjectCount = 0;
  private TaskSchedulerContext schedulerContext;
  private List<IntermediateEntry> hashShuffleIntermediateEntries = new ArrayList<IntermediateEntry>();
  private AtomicInteger completeReportReceived = new AtomicInteger(0);

  public SubQuery(QueryMasterTask.QueryMasterTaskContext context, MasterPlan masterPlan, ExecutionBlock block, AbstractStorageManager sm) {
    this.context = context;
    this.masterPlan = masterPlan;
    this.block = block;
    this.sm = sm;
    this.eventHandler = context.getEventHandler();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    subQueryState = stateMachine.getCurrentState();
  }

  public static boolean isRunningState(SubQueryState state) {
    return state == SubQueryState.INITED || state == SubQueryState.NEW || state == SubQueryState.RUNNING;
  }

  public QueryMasterTask.QueryMasterTaskContext getContext() {
    return context;
  }

  public MasterPlan getMasterPlan() {
    return masterPlan;
  }

  public DataChannel getDataChannel() {
    return masterPlan.getOutgoingChannels(getId()).iterator().next();
  }

  public EventHandler<Event> getEventHandler() {
    return eventHandler;
  }

  public AbstractTaskScheduler getTaskScheduler() {
    return taskScheduler;
  }

  public void setStartTime() {
    startTime = context.getClock().getTime();
  }

  @SuppressWarnings("UnusedDeclaration")
  public long getStartTime() {
    return this.startTime;
  }

  public void setFinishTime() {
    finishTime = context.getClock().getTime();
  }

  @SuppressWarnings("UnusedDeclaration")
  public long getFinishTime() {
    return this.finishTime;
  }

  public float getTaskProgress() {
    readLock.lock();
    try {
      if (getState() == SubQueryState.NEW) {
        return 0;
      } else {
        return (float)(succeededObjectCount) / (float)totalScheduledObjectsCount;
      }
    } finally {
      readLock.unlock();
    }
  }

  public float getProgress() {
    List<QueryUnit> tempTasks = null;
    readLock.lock();
    try {
      if (getState() == SubQueryState.NEW) {
        return 0.0f;
      } else {
        tempTasks = new ArrayList<QueryUnit>(tasks.values());
      }
    } finally {
      readLock.unlock();
    }

    float totalProgress = 0.0f;
    for (QueryUnit eachQueryUnit: tempTasks) {
      if (eachQueryUnit.getLastAttempt() != null) {
        totalProgress += eachQueryUnit.getLastAttempt().getProgress();
      }
    }

    if (totalProgress > 0.0f) {
      return (float) Math.floor((totalProgress / (float) tempTasks.size()) * 1000.0f) / 1000.0f;
    } else {
      return 0.0f;
    }
  }

  public int getSucceededObjectCount() {
    return succeededObjectCount;
  }

  public int getTotalScheduledObjectsCount() {
    return totalScheduledObjectsCount;
  }

  public ExecutionBlock getBlock() {
    return block;
  }

  public void addTask(QueryUnit task) {
    tasks.put(task.getId(), task);
  }

  /**
   * It finalizes this subquery. It is only invoked when the subquery is succeeded.
   */
  public void complete() {
    cleanup();
    finalizeStats();
    setFinishTime();
    eventHandler.handle(new SubQueryCompletedEvent(getId(), SubQueryState.SUCCEEDED));
  }

  /**
   * It finalizes this subquery. Unlike {@link SubQuery#complete()},
   * it is invoked when a subquery is abnormally finished.
   *
   * @param finalState The final subquery state
   */
  public void abort(SubQueryState finalState) {
    // TODO -
    // - committer.abortSubQuery(...)
    // - record SubQuery Finish Time
    // - CleanUp Tasks
    // - Record History
    cleanup();
    setFinishTime();
    eventHandler.handle(new SubQueryCompletedEvent(getId(), finalState));
  }

  public StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent> getStateMachine() {
    return this.stateMachine;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }


  public int getPriority() {
    return this.priority;
  }

  public AbstractStorageManager getStorageManager() {
    return sm;
  }
  
  public ExecutionBlockId getId() {
    return block.getId();
  }
  
  public QueryUnit[] getQueryUnits() {
    return tasks.values().toArray(new QueryUnit[tasks.size()]);
  }
  
  public QueryUnit getQueryUnit(QueryUnitId qid) {
    return tasks.get(qid);
  }

  public Schema getSchema() {
    return schema;
  }

  public TableMeta getTableMeta() {
    return meta;
  }

  public TableStats getResultStats() {
    return resultStatistics;
  }

  public TableStats getInputStats() {
    return inputStatistics;
  }

  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  protected void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getId());
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof SubQuery) {
      SubQuery other = (SubQuery)o;
      return getId().equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return getId().hashCode();
  }
  
  public int compareTo(SubQuery other) {
    return getId().compareTo(other.getId());
  }

  public SubQueryState getSynchronizedState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /* non-blocking call for client API */
  public SubQueryState getState() {
    return subQueryState;
  }

  public static TableStats[] computeStatFromUnionBlock(SubQuery subQuery) {
    TableStats[] stat = new TableStats[]{new TableStats(), new TableStats()};
    long[] avgRows = new long[]{0, 0};
    long[] numBytes = new long[]{0, 0};
    long[] readBytes = new long[]{0, 0};
    long[] numRows = new long[]{0, 0};
    int[] numBlocks = new int[]{0, 0};
    int[] numOutputs = new int[]{0, 0};

    List<ColumnStats> columnStatses = Lists.newArrayList();

    MasterPlan masterPlan = subQuery.getMasterPlan();
    Iterator<ExecutionBlock> it = masterPlan.getChilds(subQuery.getBlock()).iterator();
    while (it.hasNext()) {
      ExecutionBlock block = it.next();
      SubQuery childSubQuery = subQuery.context.getSubQuery(block.getId());
      TableStats[] childStatArray = new TableStats[]{
          childSubQuery.getInputStats(), childSubQuery.getResultStats()
      };
      for (int i = 0; i < 2; i++) {
        if (childStatArray[i] == null) {
          continue;
        }
        avgRows[i] += childStatArray[i].getAvgRows();
        numBlocks[i] += childStatArray[i].getNumBlocks();
        numBytes[i] += childStatArray[i].getNumBytes();
        readBytes[i] += childStatArray[i].getReadBytes();
        numOutputs[i] += childStatArray[i].getNumShuffleOutputs();
        numRows[i] += childStatArray[i].getNumRows();
      }
      columnStatses.addAll(childStatArray[1].getColumnStats());
    }

    for (int i = 0; i < 2; i++) {
      stat[i].setNumBlocks(numBlocks[i]);
      stat[i].setNumBytes(numBytes[i]);
      stat[i].setReadBytes(readBytes[i]);
      stat[i].setNumShuffleOutputs(numOutputs[i]);
      stat[i].setNumRows(numRows[i]);
      stat[i].setAvgRows(avgRows[i]);
    }
    stat[1].setColumnStats(columnStatses);

    return stat;
  }

  private TableStats[] computeStatFromTasks() {
    List<TableStats> inputStatsList = Lists.newArrayList();
    List<TableStats> resultStatsList = Lists.newArrayList();
    for (QueryUnit unit : getQueryUnits()) {
      resultStatsList.add(unit.getStats());
      if (unit.getLastAttempt().getInputStats() != null) {
        inputStatsList.add(unit.getLastAttempt().getInputStats());
      }
    }
    TableStats inputStats = StatisticsUtil.aggregateTableStat(inputStatsList);
    TableStats resultStats = StatisticsUtil.aggregateTableStat(resultStatsList);
    return new TableStats[]{inputStats, resultStats};
  }

  private void stopScheduler() {
    // If there are launched TaskRunners, send the 'shouldDie' message to all r
    // via received task requests.
    if (taskScheduler != null) {
      taskScheduler.stop();
    }
  }

  private void releaseContainers() {
    // If there are still live TaskRunners, try to kill the containers.
    eventHandler.handle(new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP, getId(), containers.values()));
  }

  public void releaseContainer(ContainerId containerId) {
    // try to kill the container.
    ArrayList<Container> list = new ArrayList<Container>();
    list.add(containers.get(containerId));
    eventHandler.handle(new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP, getId(), list));
  }

  /**
   * It computes all stats and sets the intermediate result.
   */
  private void finalizeStats() {
    TableStats[] statsArray;
    if (block.hasUnion()) {
      statsArray = computeStatFromUnionBlock(this);
    } else {
      statsArray = computeStatFromTasks();
    }

    DataChannel channel = masterPlan.getOutgoingChannels(getId()).get(0);
    // get default or store type
    CatalogProtos.StoreType storeType = CatalogProtos.StoreType.CSV; // default setting

    // if store plan (i.e., CREATE or INSERT OVERWRITE)
    StoreTableNode storeTableNode = PlannerUtil.findTopNode(getBlock().getPlan(), NodeType.STORE);
    if (storeTableNode != null) {
      storeType = storeTableNode.getStorageType();
    }
    schema = channel.getSchema();
    meta = CatalogUtil.newTableMeta(storeType, new KeyValueSet());
    inputStatistics = statsArray[0];
    resultStatistics = statsArray[1];
  }

  @Override
  public void handle(SubQueryEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getSubQueryId() + " of type " + event.getType() + ", preState=" + getSynchronizedState());
    }

    try {
      writeLock.lock();
      SubQueryState oldState = getSynchronizedState();
      try {
        getStateMachine().doTransition(event.getType(), event);
        subQueryState = getSynchronizedState();
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", eventType:" + event.getType().name()
            + ", oldState:" + oldState.name()
            + ", nextState:" + getSynchronizedState().name()
            , e);
        eventHandler.handle(new SubQueryEvent(getId(),
            SubQueryEventType.SQ_INTERNAL_ERROR));
      }

      // notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
        if (oldState != getSynchronizedState()) {
          LOG.debug(getId() + " SubQuery Transitioned from " + oldState + " to "
              + getSynchronizedState());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void handleTaskRequestEvent(TaskRequestEvent event) {
    taskScheduler.handleTaskRequestEvent(event);
  }

  private static class InitAndRequestContainer implements MultipleArcTransition<SubQuery,
      SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(final SubQuery subQuery, SubQueryEvent subQueryEvent) {
      subQuery.setStartTime();
      ExecutionBlock execBlock = subQuery.getBlock();
      SubQueryState state;

      try {
        // Union operator does not require actual query processing. It is performed logically.
        if (execBlock.hasUnion()) {
          subQuery.finalizeStats();
          state = SubQueryState.SUCCEEDED;
        } else {
          // execute pre-processing asyncronously
          subQuery.getContext().getQueryMasterContext().getEventExecutor()
              .submit(new Runnable() {
                        @Override
                        public void run() {
                          try {
                            ExecutionBlock parent = subQuery.getMasterPlan().getParent(subQuery.getBlock());
                            DataChannel channel = subQuery.getMasterPlan().getChannel(subQuery.getId(), parent.getId());
                            setShuffleIfNecessary(subQuery, channel);
                            initTaskScheduler(subQuery);
                            schedule(subQuery);
                            subQuery.totalScheduledObjectsCount = subQuery.getTaskScheduler().remainingScheduledObjectNum();
                            LOG.info(subQuery.totalScheduledObjectsCount + " objects are scheduled");

                            if (subQuery.getTaskScheduler().remainingScheduledObjectNum() == 0) { // if there is no tasks
                              subQuery.stopScheduler();
                              subQuery.finalizeStats();
                              subQuery.eventHandler.handle(new SubQueryCompletedEvent(subQuery.getId(), SubQueryState.SUCCEEDED));
                            } else {
                              subQuery.taskScheduler.start();
                              allocateContainers(subQuery);

                            }
                          } catch (Throwable e) {
                            LOG.error("SubQuery (" + subQuery.getId() + ") ERROR: ", e);
                            subQuery.setFinishTime();
                            subQuery.eventHandler.handle(new SubQueryDiagnosticsUpdateEvent(subQuery.getId(), e.getMessage()));
                            subQuery.eventHandler.handle(new SubQueryCompletedEvent(subQuery.getId(), SubQueryState.ERROR));
                          }
                        }
                      }
              );
          state = SubQueryState.INITED;
        }
      } catch (Throwable e) {
        LOG.error("SubQuery (" + subQuery.getId() + ") ERROR: ", e);
        subQuery.setFinishTime();
        subQuery.eventHandler.handle(new SubQueryDiagnosticsUpdateEvent(subQuery.getId(), e.getMessage()));
        subQuery.eventHandler.handle(new SubQueryCompletedEvent(subQuery.getId(), SubQueryState.ERROR));
        return SubQueryState.ERROR;
      }

      return state;
    }

    private void initTaskScheduler(SubQuery subQuery) throws IOException {
      TajoConf conf = subQuery.context.getConf();
      subQuery.schedulerContext = new TaskSchedulerContext(subQuery.context,
          subQuery.getMasterPlan().isLeaf(subQuery.getId()), subQuery.getId());
      subQuery.taskScheduler = TaskSchedulerFactory.get(conf, subQuery.schedulerContext, subQuery);
      subQuery.taskScheduler.init(conf);
      LOG.info(subQuery.taskScheduler.getName() + " is chosen for the task scheduling for " + subQuery.getId());
    }

    /**
     * If a parent block requires a repartition operation, the method sets proper repartition
     * methods and the number of partitions to a given subquery.
     */
    private static void setShuffleIfNecessary(SubQuery subQuery, DataChannel channel) {
      if (channel.getShuffleType() != ShuffleType.NONE_SHUFFLE) {
        int numTasks = calculateShuffleOutputNum(subQuery, channel);
        Repartitioner.setShuffleOutputNumForTwoPhase(subQuery, numTasks, channel);
      }
    }

    /**
     * Getting the total memory of cluster
     *
     * @param subQuery
     * @return mega bytes
     */
    private static int getClusterTotalMemory(SubQuery subQuery) {
      List<TajoMasterProtocol.WorkerResourceProto> workers =
          subQuery.context.getQueryMasterContext().getQueryMaster().getAllWorker();

      int totalMem = 0;
      for (TajoMasterProtocol.WorkerResourceProto worker : workers) {
        totalMem += worker.getMemoryMB();
      }
      return totalMem;
    }
    /**
     * Getting the desire number of partitions according to the volume of input data.
     * This method is only used to determine the partition key number of hash join or aggregation.
     *
     * @param subQuery
     * @return
     */
    public static int calculateShuffleOutputNum(SubQuery subQuery, DataChannel channel) {
      TajoConf conf = subQuery.context.getConf();
      MasterPlan masterPlan = subQuery.getMasterPlan();
      ExecutionBlock parent = masterPlan.getParent(subQuery.getBlock());

      LogicalNode grpNode = null;
      if (parent != null) {
        grpNode = PlannerUtil.findMostBottomNode(parent.getPlan(), NodeType.GROUP_BY);
        if (grpNode == null) {
          grpNode = PlannerUtil.findMostBottomNode(parent.getPlan(), NodeType.DISTINCT_GROUP_BY);
        }
      }

      // We assume this execution block the first stage of join if two or more tables are included in this block,
      if (parent != null && parent.getScanNodes().length >= 2) {
        List<ExecutionBlock> childs = masterPlan.getChilds(parent);

        // for outer
        ExecutionBlock outer = childs.get(0);
        long outerVolume = getInputVolume(subQuery.masterPlan, subQuery.context, outer);

        // for inner
        ExecutionBlock inner = childs.get(1);
        long innerVolume = getInputVolume(subQuery.masterPlan, subQuery.context, inner);
        LOG.info(subQuery.getId() + ", Outer volume: " + Math.ceil((double) outerVolume / 1048576) + "MB, "
            + "Inner volume: " + Math.ceil((double) innerVolume / 1048576) + "MB");

        long bigger = Math.max(outerVolume, innerVolume);

        int mb = (int) Math.ceil((double) bigger / 1048576);
        LOG.info(subQuery.getId() + ", Bigger Table's volume is approximately " + mb + " MB");

        int taskNum = (int) Math.ceil((double) mb /
            conf.getIntVar(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME));

        int totalMem = getClusterTotalMemory(subQuery);
        LOG.info(subQuery.getId() + ", Total memory of cluster is " + totalMem + " MB");
        int slots = Math.max(totalMem / conf.getIntVar(ConfVars.TASK_DEFAULT_MEMORY), 1);
        // determine the number of task
        taskNum = Math.min(taskNum, slots);

        if (conf.getIntVar(ConfVars.$TEST_MIN_TASK_NUM) > 0) {
          taskNum = conf.getIntVar(ConfVars.$TEST_MIN_TASK_NUM);
          LOG.warn("!!!!! TESTCASE MODE !!!!!");
        }

        // The shuffle output numbers of join may be inconsistent by execution block order.
        // Thus, we need to compare the number with DataChannel output numbers.
        // If the number is right, the number and DataChannel output numbers will be consistent.
        int outerShuffleOutputNum = 0, innerShuffleOutputNum = 0;
        for (DataChannel eachChannel : masterPlan.getOutgoingChannels(outer.getId())) {
          outerShuffleOutputNum = Math.max(outerShuffleOutputNum, eachChannel.getShuffleOutputNum());
        }
        for (DataChannel eachChannel : masterPlan.getOutgoingChannels(inner.getId())) {
          innerShuffleOutputNum = Math.max(innerShuffleOutputNum, eachChannel.getShuffleOutputNum());
        }
        if (outerShuffleOutputNum != innerShuffleOutputNum
            && taskNum != outerShuffleOutputNum
            && taskNum != innerShuffleOutputNum) {
          LOG.info(subQuery.getId() + ", Change determined number of join partitions cause difference of outputNum" +
                  ", originTaskNum=" + taskNum + ", changedTaskNum=" + Math.max(outerShuffleOutputNum, innerShuffleOutputNum) +
                  ", outerShuffleOutptNum=" + outerShuffleOutputNum +
                  ", innerShuffleOutputNum=" + innerShuffleOutputNum);
          taskNum = Math.max(outerShuffleOutputNum, innerShuffleOutputNum);
        }

        LOG.info(subQuery.getId() + ", The determined number of join partitions is " + taskNum);

        return taskNum;
        // Is this subquery the first step of group-by?
      } else if (grpNode != null) {
        boolean hasGroupColumns = true;
        if (grpNode.getType() == NodeType.GROUP_BY) {
          hasGroupColumns = ((GroupbyNode)grpNode).getGroupingColumns().length > 0;
        } else if (grpNode.getType() == NodeType.DISTINCT_GROUP_BY) {
          // Find current distinct stage node.
          DistinctGroupbyNode distinctNode = PlannerUtil.findMostBottomNode(subQuery.getBlock().getPlan(), NodeType.DISTINCT_GROUP_BY);
          if (distinctNode == null) {
            LOG.warn(subQuery.getId() + ", Can't find current DistinctGroupbyNode");
            distinctNode = (DistinctGroupbyNode)grpNode;
          }
          hasGroupColumns = distinctNode.getGroupingColumns().length > 0;

          Enforcer enforcer = subQuery.getBlock().getEnforcer();
          if (enforcer == null) {
            LOG.warn(subQuery.getId() + ", DistinctGroupbyNode's enforcer is null.");
          }
          EnforceProperty property = PhysicalPlannerImpl.getAlgorithmEnforceProperty(enforcer, distinctNode);
          if (property != null) {
            if (property.getDistinct().getIsMultipleAggregation()) {
              MultipleAggregationStage stage = property.getDistinct().getMultipleAggregationStage();
              if (stage != MultipleAggregationStage.THRID_STAGE) {
                hasGroupColumns = true;
              }
            }
          }
        }
        if (!hasGroupColumns) {
          LOG.info(subQuery.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
          return 1;
        } else {
          long volume = getInputVolume(subQuery.masterPlan, subQuery.context, subQuery.block);

          int mb = (int) Math.ceil((double) volume / 1048576);
          LOG.info(subQuery.getId() + ", Table's volume is approximately " + mb + " MB");
          // determine the number of task
          int taskNumBySize = (int) Math.ceil((double) mb /
              conf.getIntVar(ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME));

          int totalMem = getClusterTotalMemory(subQuery);

          LOG.info(subQuery.getId() + ", Total memory of cluster is " + totalMem + " MB");
          int slots = Math.max(totalMem / conf.getIntVar(ConfVars.TASK_DEFAULT_MEMORY), 1);
          int taskNum = Math.min(taskNumBySize, slots); //Maximum partitions
          LOG.info(subQuery.getId() + ", The determined number of aggregation partitions is " + taskNum);
          return taskNum;
        }
      } else {
        LOG.info("============>>>>> Unexpected Case! <<<<<================");
        long volume = getInputVolume(subQuery.masterPlan, subQuery.context, subQuery.block);

        int mb = (int) Math.ceil((double)volume / 1048576);
        LOG.info(subQuery.getId() + ", Table's volume is approximately " + mb + " MB");
        // determine the number of task per 128MB
        int taskNum = (int) Math.ceil((double)mb / 128);
        LOG.info(subQuery.getId() + ", The determined number of partitions is " + taskNum);
        return taskNum;
      }
    }

    private static void schedule(SubQuery subQuery) throws IOException {
      MasterPlan masterPlan = subQuery.getMasterPlan();
      ExecutionBlock execBlock = subQuery.getBlock();
      if (subQuery.getMasterPlan().isLeaf(execBlock.getId()) && execBlock.getScanNodes().length == 1) { // Case 1: Just Scan
        scheduleFragmentsForLeafQuery(subQuery);
      } else if (execBlock.getScanNodes().length > 1) { // Case 2: Join
        Repartitioner.scheduleFragmentsForJoinQuery(subQuery.schedulerContext, subQuery);
      } else { // Case 3: Others (Sort or Aggregation)
        int numTasks = getNonLeafTaskNum(subQuery);
        Repartitioner.scheduleFragmentsForNonLeafTasks(subQuery.schedulerContext, masterPlan, subQuery, numTasks);
      }
    }

    /**
     * Getting the desire number of tasks according to the volume of input data
     *
     * @param subQuery
     * @return
     */
    public static int getNonLeafTaskNum(SubQuery subQuery) {
      // Getting intermediate data size
      long volume = getInputVolume(subQuery.getMasterPlan(), subQuery.context, subQuery.getBlock());

      int mb = (int) Math.ceil((double)volume / 1048576);
      LOG.info(subQuery.getId() + ", Table's volume is approximately " + mb + " MB");
      // determine the number of task per 64MB
      int maxTaskNum = Math.max(1, (int) Math.ceil((double)mb / 64));
      LOG.info(subQuery.getId() + ", The determined number of non-leaf tasks is " + maxTaskNum);
      return maxTaskNum;
    }

    public static long getInputVolume(MasterPlan masterPlan, QueryMasterTask.QueryMasterTaskContext context,
                                      ExecutionBlock execBlock) {
      Map<String, TableDesc> tableMap = context.getTableDescMap();
      if (masterPlan.isLeaf(execBlock)) {
        ScanNode[] outerScans = execBlock.getScanNodes();
        long maxVolume = 0;
        for (ScanNode eachScanNode: outerScans) {
          TableStats stat = tableMap.get(eachScanNode.getCanonicalName()).getStats();
          if (stat.getNumBytes() > maxVolume) {
            maxVolume = stat.getNumBytes();
          }
        }
        return maxVolume;
      } else {
        long aggregatedVolume = 0;
        for (ExecutionBlock childBlock : masterPlan.getChilds(execBlock)) {
          SubQuery subquery = context.getSubQuery(childBlock.getId());
          if (subquery == null || subquery.getSynchronizedState() != SubQueryState.SUCCEEDED) {
            aggregatedVolume += getInputVolume(masterPlan, context, childBlock);
          } else {
            aggregatedVolume += subquery.getResultStats().getNumBytes();
          }
        }

        return aggregatedVolume;
      }
    }

    public static void allocateContainers(SubQuery subQuery) {
      ExecutionBlock execBlock = subQuery.getBlock();

      //TODO consider disk slot
      int requiredMemoryMBPerTask = 512;

      int numRequest = subQuery.getContext().getResourceAllocator().calculateNumRequestContainers(
          subQuery.getContext().getQueryMasterContext().getWorkerContext(),
          subQuery.schedulerContext.getEstimatedTaskNum(),
          requiredMemoryMBPerTask
      );

      final Resource resource = Records.newRecord(Resource.class);

      resource.setMemory(requiredMemoryMBPerTask);

      LOG.info("Request Container for " + subQuery.getId() + " containers=" + numRequest);

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(subQuery.getPriority());
      ContainerAllocationEvent event =
          new ContainerAllocationEvent(ContainerAllocatorEventType.CONTAINER_REQ,
              subQuery.getId(), priority, resource, numRequest,
              subQuery.masterPlan.isLeaf(execBlock), 0.0f);
      subQuery.eventHandler.handle(event);
    }

    private static void scheduleFragmentsForLeafQuery(SubQuery subQuery) throws IOException {
      ExecutionBlock execBlock = subQuery.getBlock();
      ScanNode[] scans = execBlock.getScanNodes();
      Preconditions.checkArgument(scans.length == 1, "Must be Scan Query");
      ScanNode scan = scans[0];
      TableDesc table = subQuery.context.getTableDescMap().get(scan.getCanonicalName());

      Collection<FileFragment> fragments;
      TableMeta meta = table.getMeta();

      // Depending on scanner node's type, it creates fragments. If scan is for
      // a partitioned table, It will creates lots fragments for all partitions.
      // Otherwise, it creates at least one fragments for a table, which may
      // span a number of blocks or possibly consists of a number of files.
      if (scan.getType() == NodeType.PARTITIONS_SCAN) {
        // After calling this method, partition paths are removed from the physical plan.
        fragments = Repartitioner.getFragmentsFromPartitionedTable(subQuery.getStorageManager(), scan, table);
      } else {
        Path inputPath = table.getPath();
        fragments = subQuery.getStorageManager().getSplits(scan.getCanonicalName(), meta, table.getSchema(), inputPath);
      }

      SubQuery.scheduleFragments(subQuery, fragments);
      if (subQuery.getTaskScheduler() instanceof DefaultTaskScheduler) {
        //Leaf task of DefaultTaskScheduler should be fragment size
        // EstimatedTaskNum determined number of initial container
        subQuery.schedulerContext.setEstimatedTaskNum(fragments.size());
      } else {
        TajoConf conf = subQuery.context.getConf();
        subQuery.schedulerContext.setTaskSize(conf.getIntVar(ConfVars.TASK_DEFAULT_SIZE) * 1024 * 1024);
        int estimatedTaskNum = (int) Math.ceil((double) table.getStats().getNumBytes() /
            (double) subQuery.schedulerContext.getTaskSize());
        subQuery.schedulerContext.setEstimatedTaskNum(estimatedTaskNum);
      }
    }
  }

  public static void scheduleFragment(SubQuery subQuery, FileFragment fragment) {
    subQuery.taskScheduler.handle(new FragmentScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        subQuery.getId(), fragment));
  }


  public static void scheduleFragments(SubQuery subQuery, Collection<FileFragment> fragments) {
    for (FileFragment eachFragment : fragments) {
      scheduleFragment(subQuery, eachFragment);
    }
  }

  public static void scheduleFragments(SubQuery subQuery, Collection<FileFragment> leftFragments,
                                       Collection<FileFragment> broadcastFragments) {
    for (FileFragment eachLeafFragment : leftFragments) {
      scheduleFragment(subQuery, eachLeafFragment, broadcastFragments);
    }
  }

  public static void scheduleFragment(SubQuery subQuery,
                                      FileFragment leftFragment, Collection<FileFragment> rightFragments) {
    subQuery.taskScheduler.handle(new FragmentScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        subQuery.getId(), leftFragment, rightFragments));
  }

  public static void scheduleFetches(SubQuery subQuery, Map<String, List<FetchImpl>> fetches) {
    subQuery.taskScheduler.handle(new FetchScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        subQuery.getId(), fetches));
  }

  public static QueryUnit newEmptyQueryUnit(TaskSchedulerContext schedulerContext,
                                            QueryUnitAttemptScheduleContext queryUnitContext,
                                            SubQuery subQuery, int taskId) {
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryUnit unit = new QueryUnit(schedulerContext.getMasterContext().getConf(),
        queryUnitContext,
        QueryIdFactory.newQueryUnitId(schedulerContext.getBlockId(), taskId),
        schedulerContext.isLeafQuery(), subQuery.eventHandler);
    unit.setLogicalPlan(execBlock.getPlan());
    subQuery.addTask(unit);
    return unit;
  }

  private static class ContainerLaunchTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery, SubQueryEvent event) {
      try {
        SubQueryContainerAllocationEvent allocationEvent =
            (SubQueryContainerAllocationEvent) event;
        for (Container container : allocationEvent.getAllocatedContainer()) {
          ContainerId cId = container.getId();
          if (subQuery.containers.containsKey(cId)) {
            subQuery.eventHandler.handle(new SubQueryDiagnosticsUpdateEvent(subQuery.getId(),
                "Duplicated containers are allocated: " + cId.toString()));
            subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_INTERNAL_ERROR));
          }
          subQuery.containers.put(cId, container);
        }
        LOG.info("SubQuery (" + subQuery.getId() + ") has " + subQuery.containers.size() + " containers!");
        subQuery.eventHandler.handle(
            new LaunchTaskRunnersEvent(subQuery.getId(), allocationEvent.getAllocatedContainer(),
                subQuery.getContext().getQueryContext(),
                CoreGsonHelper.toJson(subQuery.getBlock().getPlan(), LogicalNode.class))
        );

        subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_START));
      } catch (Throwable t) {
        subQuery.eventHandler.handle(new SubQueryDiagnosticsUpdateEvent(subQuery.getId(),
            ExceptionUtils.getStackTrace(t)));
        subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_INTERNAL_ERROR));
      }
    }
  }

  /**
   * It is used in KILL_WAIT state against Contained Allocated event.
   * It just returns allocated containers to resource manager.
   */
  private static class AllocatedContainersCancelTransition implements SingleArcTransition<SubQuery, SubQueryEvent> {
    @Override
    public void transition(SubQuery subQuery, SubQueryEvent event) {
      try {
        SubQueryContainerAllocationEvent allocationEvent =
            (SubQueryContainerAllocationEvent) event;
        subQuery.eventHandler.handle(
            new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP,
                subQuery.getId(), allocationEvent.getAllocatedContainer()));
        LOG.info(String.format("[%s] %d allocated containers are canceled",
            subQuery.getId().toString(),
            allocationEvent.getAllocatedContainer().size()));
      } catch (Throwable t) {
        subQuery.eventHandler.handle(new SubQueryDiagnosticsUpdateEvent(subQuery.getId(),
            ExceptionUtils.getStackTrace(t)));
        subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_INTERNAL_ERROR));
      }
    }
  }

  private static class TaskCompletedTransition implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                           SubQueryEvent event) {
      SubQueryTaskEvent taskEvent = (SubQueryTaskEvent) event;
      QueryUnit task = subQuery.getQueryUnit(taskEvent.getTaskId());

      if (task == null) { // task failed
        LOG.error(String.format("Task %s is absent", taskEvent.getTaskId()));
        subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_FAILED));
      } else {
        subQuery.completedTaskCount++;

        if (taskEvent.getState() == TaskState.SUCCEEDED) {
          subQuery.succeededObjectCount++;
        } else if (task.getState() == TaskState.KILLED) {
          subQuery.killedObjectCount++;
        } else if (task.getState() == TaskState.FAILED) {
          subQuery.failedObjectCount++;
          // if at least one task is failed, try to kill all tasks.
          subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_KILL));
        }

        LOG.info(String.format("[%s] Task Completion Event (Total: %d, Success: %d, Killed: %d, Failed: %d)",
            subQuery.getId(),
            subQuery.getTotalScheduledObjectsCount(),
            subQuery.succeededObjectCount,
            subQuery.killedObjectCount,
            subQuery.failedObjectCount));

        if (subQuery.totalScheduledObjectsCount ==
            subQuery.succeededObjectCount + subQuery.killedObjectCount + subQuery.failedObjectCount) {
          subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_SUBQUERY_COMPLETED));
        }
      }
    }
  }

  private static class KillTasksTransition implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      subQuery.getTaskScheduler().stop();
      for (QueryUnit queryUnit : subQuery.getQueryUnits()) {
        subQuery.eventHandler.handle(new TaskEvent(queryUnit.getId(), TaskEventType.T_KILL));
      }
    }
  }

  private void cleanup() {
    stopScheduler();
    releaseContainers();

    if (!getContext().getQueryContext().getBool(SessionVars.DEBUG_ENABLED)) {
      List<ExecutionBlock> childs = getMasterPlan().getChilds(getId());
      List<TajoIdProtos.ExecutionBlockIdProto> ebIds = Lists.newArrayList();

      for (ExecutionBlock executionBlock : childs) {
        ebIds.add(executionBlock.getId().getProto());
      }

      getContext().getQueryMasterContext().getQueryMaster().cleanupExecutionBlock(ebIds);
    }
  }

  public List<IntermediateEntry> getHashShuffleIntermediateEntries() {
    return hashShuffleIntermediateEntries;
  }

  protected void waitingIntermediateReport() {
    LOG.info(getId() + ", waiting IntermediateReport: expectedTaskNum=" + completeReportReceived.get());
    synchronized(completeReportReceived) {
      long startTime = System.currentTimeMillis();
      while (true) {
        if (completeReportReceived.get() >= tasks.size()) {
          LOG.info(getId() + ", completed waiting IntermediateReport");
          return;
        } else {
          try {
            completeReportReceived.wait(10 * 1000);
          } catch (InterruptedException e) {
          }
          long elapsedTime = System.currentTimeMillis() - startTime;
          if (elapsedTime >= 120 * 1000) {
            LOG.error(getId() + ", Timeout while receiving intermediate reports: " + elapsedTime + " ms");
            abort(SubQueryState.FAILED);
            return;
          }
        }
      }
    }
  }

  public void receiveExecutionBlockReport(TajoWorkerProtocol.ExecutionBlockReport report) {
    LOG.info(getId() + ", receiveExecutionBlockReport:" +  report.getSucceededTasks());
    if (!report.getReportSuccess()) {
      LOG.error(getId() + ", ExecutionBlock final report fail cause:" + report.getReportErrorMessage());
      abort(SubQueryState.FAILED);
      return;
    }
    if (report.getIntermediateEntriesCount() > 0) {
      synchronized (hashShuffleIntermediateEntries) {
        for (IntermediateEntryProto eachInterm: report.getIntermediateEntriesList()) {
          hashShuffleIntermediateEntries.add(new IntermediateEntry(eachInterm));
        }
      }
    }
    synchronized(completeReportReceived) {
      completeReportReceived.addAndGet(report.getSucceededTasks());
      completeReportReceived.notifyAll();
    }
  }

  private static class SubQueryCompleteTransition
      implements MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      // TODO - Commit subQuery
      // TODO - records succeeded, failed, killed completed task
      // TODO - records metrics
      try {
        LOG.info(String.format("subQuery completed - %s (total=%d, success=%d, killed=%d)",
            subQuery.getId().toString(),
            subQuery.getTotalScheduledObjectsCount(),
            subQuery.getSucceededObjectCount(),
            subQuery.killedObjectCount));

        if (subQuery.killedObjectCount > 0 || subQuery.failedObjectCount > 0) {
          if (subQuery.failedObjectCount > 0) {
            subQuery.abort(SubQueryState.FAILED);
            return SubQueryState.FAILED;
          } else if (subQuery.killedObjectCount > 0) {
            subQuery.abort(SubQueryState.KILLED);
            return SubQueryState.KILLED;
          } else {
            LOG.error("Invalid State " + subQuery.getSynchronizedState() + " State");
            subQuery.abort(SubQueryState.ERROR);
            return SubQueryState.ERROR;
          }
        } else {
          subQuery.complete();
          return SubQueryState.SUCCEEDED;
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        subQuery.abort(SubQueryState.ERROR);
        return SubQueryState.ERROR;
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<SubQuery, SubQueryEvent> {
    @Override
    public void transition(SubQuery subQuery, SubQueryEvent event) {
      subQuery.addDiagnostic(((SubQueryDiagnosticsUpdateEvent) event).getDiagnosticUpdate());
    }
  }

  private static class InternalErrorTransition implements SingleArcTransition<SubQuery, SubQueryEvent> {
    @Override
    public void transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      subQuery.abort(SubQueryState.ERROR);
    }
  }
}
