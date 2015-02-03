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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty;
import org.apache.tajo.ipc.TajoWorkerProtocol.IntermediateEntryProto;
import org.apache.tajo.master.LaunchTaskRunnersEvent;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.TaskState;
import org.apache.tajo.master.container.TajoContainer;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.querymaster.Task.IntermediateEntry;
import org.apache.tajo.storage.FileStorageManager;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.history.StageHistory;
import org.apache.tajo.util.history.TaskHistory;
import org.apache.tajo.worker.FetchImpl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;


/**
 * Stage plays a role in controlling an ExecutionBlock and is a finite state machine.
 */
public class Stage implements EventHandler<StageEvent> {

  private static final Log LOG = LogFactory.getLog(Stage.class);

  private MasterPlan masterPlan;
  private ExecutionBlock block;
  private int priority;
  private Schema schema;
  private TableMeta meta;
  private TableStats resultStatistics;
  private TableStats inputStatistics;
  private EventHandler<Event> eventHandler;
  private AbstractTaskScheduler taskScheduler;
  private QueryMasterTask.QueryMasterTaskContext context;
  private final List<String> diagnostics = new ArrayList<String>();
  private StageState stageState;

  private long startTime;
  private long finishTime;
  private volatile long lastContactTime;
  private Thread timeoutChecker;

  volatile Map<TaskId, Task> tasks = new ConcurrentHashMap<TaskId, Task>();
  volatile Map<TajoContainerId, TajoContainer> containers = new ConcurrentHashMap<TajoContainerId,
    TajoContainer>();

  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final ContainerLaunchTransition CONTAINER_LAUNCH_TRANSITION = new ContainerLaunchTransition();
  private static final TaskCompletedTransition TASK_COMPLETED_TRANSITION = new TaskCompletedTransition();
  private static final AllocatedContainersCancelTransition CONTAINERS_CANCEL_TRANSITION =
      new AllocatedContainersCancelTransition();
  private static final StageCompleteTransition STAGE_COMPLETED_TRANSITION = new StageCompleteTransition();
  private static final StageFinalizeTransition STAGE_FINALIZE_TRANSITION = new StageFinalizeTransition();
  private StateMachine<StageState, StageEventType, StageEvent> stateMachine;

  protected static final StateMachineFactory<Stage, StageState,
      StageEventType, StageEvent> stateMachineFactory =
      new StateMachineFactory<Stage, StageState,
          StageEventType, StageEvent>(StageState.NEW)

          // Transitions from NEW state
          .addTransition(StageState.NEW,
              EnumSet.of(StageState.INITED, StageState.ERROR, StageState.SUCCEEDED),
              StageEventType.SQ_INIT,
              new InitAndRequestContainer())
          .addTransition(StageState.NEW, StageState.NEW,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.NEW, StageState.KILLED,
              StageEventType.SQ_KILL)
          .addTransition(StageState.NEW, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(StageState.INITED, StageState.RUNNING,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINER_LAUNCH_TRANSITION)
          .addTransition(StageState.INITED, StageState.INITED,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.INITED, StageState.KILL_WAIT,
              StageEventType.SQ_KILL, new KillTasksTransition())
          .addTransition(StageState.INITED, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(StageState.RUNNING, StageState.RUNNING,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINER_LAUNCH_TRANSITION)
          .addTransition(StageState.RUNNING, StageState.RUNNING,
              StageEventType.SQ_TASK_COMPLETED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(StageState.RUNNING, StageState.FINALIZING,
              StageEventType.SQ_SHUFFLE_REPORT,
              STAGE_FINALIZE_TRANSITION)
          .addTransition(StageState.RUNNING,
              EnumSet.of(StageState.SUCCEEDED, StageState.FAILED),
              StageEventType.SQ_STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(StageState.RUNNING, StageState.RUNNING,
              StageEventType.SQ_FAILED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(StageState.RUNNING, StageState.RUNNING,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.RUNNING, StageState.KILL_WAIT,
              StageEventType.SQ_KILL,
              new KillTasksTransition())
          .addTransition(StageState.RUNNING, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able Transition
          .addTransition(StageState.RUNNING, StageState.RUNNING,
              StageEventType.SQ_START)

          // Transitions from KILL_WAIT state
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              EnumSet.of(StageEventType.SQ_START, StageEventType.SQ_CONTAINER_ALLOCATED),
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              EnumSet.of(StageEventType.SQ_KILL), new KillTasksTransition())
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              StageEventType.SQ_TASK_COMPLETED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(StageState.KILL_WAIT,
              EnumSet.of(StageState.SUCCEEDED, StageState.FAILED, StageState.KILLED),
              StageEventType.SQ_STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              StageEventType.SQ_FAILED,
              TASK_COMPLETED_TRANSITION)
          .addTransition(StageState.KILL_WAIT, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

              // Transitions from FINALIZING state
          .addTransition(StageState.FINALIZING, StageState.FINALIZING,
              StageEventType.SQ_SHUFFLE_REPORT,
              STAGE_FINALIZE_TRANSITION)
          .addTransition(StageState.FINALIZING,
              EnumSet.of(StageState.SUCCEEDED, StageState.FAILED),
              StageEventType.SQ_STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(StageState.FINALIZING, StageState.FINALIZING,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.FINALIZING, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
              // Ignore-able Transition
          .addTransition(StageState.FINALIZING, StageState.KILLED,
              StageEventType.SQ_KILL)

              // Transitions from SUCCEEDED state
          .addTransition(StageState.SUCCEEDED, StageState.SUCCEEDED,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(StageState.SUCCEEDED, StageState.SUCCEEDED,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.SUCCEEDED, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
              // Ignore-able events
          .addTransition(StageState.SUCCEEDED, StageState.SUCCEEDED,
              EnumSet.of(
                  StageEventType.SQ_START,
                  StageEventType.SQ_KILL,
                  StageEventType.SQ_CONTAINER_ALLOCATED))

          // Transitions from KILLED state
          .addTransition(StageState.KILLED, StageState.KILLED,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(StageState.KILLED, StageState.KILLED,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.KILLED, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
              // Ignore-able transitions
          .addTransition(StageState.KILLED, StageState.KILLED,
              EnumSet.of(
                  StageEventType.SQ_START,
                  StageEventType.SQ_KILL,
                  StageEventType.SQ_CONTAINER_ALLOCATED,
                  StageEventType.SQ_SHUFFLE_REPORT,
                  StageEventType.SQ_STAGE_COMPLETED,
                  StageEventType.SQ_FAILED))

          // Transitions from FAILED state
          .addTransition(StageState.FAILED, StageState.FAILED,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(StageState.FAILED, StageState.FAILED,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.FAILED, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(StageState.FAILED, StageState.FAILED,
              EnumSet.of(
                  StageEventType.SQ_START,
                  StageEventType.SQ_KILL,
                  StageEventType.SQ_CONTAINER_ALLOCATED,
                  StageEventType.SQ_FAILED))

          // Transitions from ERROR state
          .addTransition(StageState.ERROR, StageState.ERROR,
              StageEventType.SQ_CONTAINER_ALLOCATED,
              CONTAINERS_CANCEL_TRANSITION)
          .addTransition(StageState.ERROR, StageState.ERROR,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // Ignore-able transitions
          .addTransition(StageState.ERROR, StageState.ERROR,
              EnumSet.of(
                  StageEventType.SQ_START,
                  StageEventType.SQ_KILL,
                  StageEventType.SQ_FAILED,
                  StageEventType.SQ_INTERNAL_ERROR,
                  StageEventType.SQ_STAGE_COMPLETED))

          .installTopology();

  private final Lock readLock;
  private final Lock writeLock;

  private int totalScheduledObjectsCount;
  private int completedTaskCount = 0;
  private int succeededObjectCount = 0;
  private int killedObjectCount = 0;
  private int failedObjectCount = 0;
  private TaskSchedulerContext schedulerContext;
  private List<IntermediateEntry> hashShuffleIntermediateEntries = Lists.newArrayList();
  private AtomicInteger completedShuffleTasks = new AtomicInteger(0);
  private AtomicBoolean stopShuffleReceiver = new AtomicBoolean();
  private StageHistory finalStageHistory;

  public Stage(QueryMasterTask.QueryMasterTaskContext context, MasterPlan masterPlan, ExecutionBlock block) {
    this.context = context;
    this.masterPlan = masterPlan;
    this.block = block;
    this.eventHandler = context.getEventHandler();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    stageState = stateMachine.getCurrentState();
  }

  public static boolean isRunningState(StageState state) {
    return state == StageState.INITED || state == StageState.NEW || state == StageState.RUNNING;
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
      if (getState() == StageState.NEW) {
        return 0;
      } else {
        return (float)(succeededObjectCount) / (float)totalScheduledObjectsCount;
      }
    } finally {
      readLock.unlock();
    }
  }

  public float getProgress() {
    List<Task> tempTasks = null;
    readLock.lock();
    try {
      if (getState() == StageState.NEW) {
        return 0.0f;
      } else {
        tempTasks = new ArrayList<Task>(tasks.values());
      }
    } finally {
      readLock.unlock();
    }

    float totalProgress = 0.0f;
    for (Task eachTask : tempTasks) {
      if (eachTask.getLastAttempt() != null) {
        totalProgress += eachTask.getLastAttempt().getProgress();
      }
    }

    if (totalProgress > 0.0f) {
      return (float) Math.floor((totalProgress / (float) Math.max(tempTasks.size(), 1)) * 1000.0f) / 1000.0f;
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

  public int getKilledObjectCount() {
    return killedObjectCount;
  }

  public int getFailedObjectCount() {
    return failedObjectCount;
  }

  public int getCompletedTaskCount() {
    return completedTaskCount;
  }

  public ExecutionBlock getBlock() {
    return block;
  }

  public void addTask(Task task) {
    tasks.put(task.getId(), task);
  }

  public StageHistory getStageHistory() {
    if (finalStageHistory != null) {
      if (finalStageHistory.getFinishTime() == 0) {
        finalStageHistory = makeStageHistory();
        finalStageHistory.setTasks(makeTaskHistories());
      }
      return finalStageHistory;
    } else {
      return makeStageHistory();
    }
  }

  private List<TaskHistory> makeTaskHistories() {
    List<TaskHistory> taskHistories = new ArrayList<TaskHistory>();

    for(Task eachTask : getTasks()) {
      taskHistories.add(eachTask.getTaskHistory());
    }

    return taskHistories;
  }

  private StageHistory makeStageHistory() {
    StageHistory stageHistory = new StageHistory();

    stageHistory.setExecutionBlockId(getId().toString());
    stageHistory.setPlan(PlannerUtil.buildExplainString(block.getPlan()));
    stageHistory.setState(getState().toString());
    stageHistory.setStartTime(startTime);
    stageHistory.setFinishTime(finishTime);
    stageHistory.setSucceededObjectCount(succeededObjectCount);
    stageHistory.setKilledObjectCount(killedObjectCount);
    stageHistory.setFailedObjectCount(failedObjectCount);
    stageHistory.setTotalScheduledObjectsCount(totalScheduledObjectsCount);
    stageHistory.setHostLocalAssigned(getTaskScheduler().getHostLocalAssigned());
    stageHistory.setRackLocalAssigned(getTaskScheduler().getRackLocalAssigned());

    long totalInputBytes = 0;
    long totalReadBytes = 0;
    long totalReadRows = 0;
    long totalWriteBytes = 0;
    long totalWriteRows = 0;
    int numShuffles = 0;
    for(Task eachTask : getTasks()) {
      numShuffles = eachTask.getShuffleOutpuNum();
      if (eachTask.getLastAttempt() != null) {
        TableStats inputStats = eachTask.getLastAttempt().getInputStats();
        if (inputStats != null) {
          totalInputBytes += inputStats.getNumBytes();
          totalReadBytes += inputStats.getReadBytes();
          totalReadRows += inputStats.getNumRows();
        }
        TableStats outputStats = eachTask.getLastAttempt().getResultStats();
        if (outputStats != null) {
          totalWriteBytes += outputStats.getNumBytes();
          totalWriteRows += outputStats.getNumRows();
        }
      }
    }

    stageHistory.setTotalInputBytes(totalInputBytes);
    stageHistory.setTotalReadBytes(totalReadBytes);
    stageHistory.setTotalReadRows(totalReadRows);
    stageHistory.setTotalWriteBytes(totalWriteBytes);
    stageHistory.setTotalWriteRows(totalWriteRows);
    stageHistory.setNumShuffles(numShuffles);
    stageHistory.setProgress(getProgress());
    return stageHistory;
  }

  /**
   * It finalizes this stage. It is only invoked when the stage is finalizing.
   */
  public void finalizeStage() {
    cleanup();
  }

  /**
   * It complete this stage. It is only invoked when the stage is succeeded.
   */
  public void complete() {
    finalizeStats();
    setFinishTime();
    eventHandler.handle(new StageCompletedEvent(getId(), StageState.SUCCEEDED));
  }

  /**
   * It finalizes this stage. Unlike {@link Stage#complete()},
   * it is invoked when a stage is abnormally finished.
   *
   * @param finalState The final stage state
   */
  public void abort(StageState finalState) {
    // TODO -
    // - committer.abortStage(...)
    // - record Stage Finish Time
    // - CleanUp Tasks
    // - Record History
    cleanup();
    setFinishTime();
    eventHandler.handle(new StageCompletedEvent(getId(), finalState));
  }

  public StateMachine<StageState, StageEventType, StageEvent> getStateMachine() {
    return this.stateMachine;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }


  public int getPriority() {
    return this.priority;
  }

  public ExecutionBlockId getId() {
    return block.getId();
  }
  
  public Task[] getTasks() {
    return tasks.values().toArray(new Task[tasks.size()]);
  }
  
  public Task getTask(TaskId qid) {
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
    if (o instanceof Stage) {
      Stage other = (Stage)o;
      return getId().equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return getId().hashCode();
  }
  
  public int compareTo(Stage other) {
    return getId().compareTo(other.getId());
  }

  public StageState getSynchronizedState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /* non-blocking call for client API */
  public StageState getState() {
    return stageState;
  }

  public static TableStats[] computeStatFromUnionBlock(Stage stage) {
    TableStats[] stat = new TableStats[]{new TableStats(), new TableStats()};
    long[] avgRows = new long[]{0, 0};
    long[] numBytes = new long[]{0, 0};
    long[] readBytes = new long[]{0, 0};
    long[] numRows = new long[]{0, 0};
    int[] numBlocks = new int[]{0, 0};
    int[] numOutputs = new int[]{0, 0};

    List<ColumnStats> columnStatses = Lists.newArrayList();

    MasterPlan masterPlan = stage.getMasterPlan();
    Iterator<ExecutionBlock> it = masterPlan.getChilds(stage.getBlock()).iterator();
    while (it.hasNext()) {
      ExecutionBlock block = it.next();
      Stage childStage = stage.context.getStage(block.getId());
      TableStats[] childStatArray = new TableStats[]{
          childStage.getInputStats(), childStage.getResultStats()
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
    for (Task unit : getTasks()) {
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
    // If there are still live TaskRunners, try to kill the containers. and send the shuffle report request
    eventHandler.handle(new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP, getId(), containers.values()));
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

    // if store plan (i.e., CREATE or INSERT OVERWRITE)
    StoreType storeType = PlannerUtil.getStoreType(masterPlan.getLogicalPlan());
    if (storeType == null) {
      // get default or store type
      storeType = StoreType.CSV;
    }

    schema = channel.getSchema();
    meta = CatalogUtil.newTableMeta(storeType, new KeyValueSet());
    inputStatistics = statsArray[0];
    resultStatistics = statsArray[1];
  }

  @Override
  public void handle(StageEvent event) {
    lastContactTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getStageId() + " of type " + event.getType() + ", preState="
          + getSynchronizedState());
    }

    try {
      writeLock.lock();
      StageState oldState = getSynchronizedState();
      try {
        getStateMachine().doTransition(event.getType(), event);
        stageState = getSynchronizedState();
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", eventType:" + event.getType().name()
            + ", oldState:" + oldState.name()
            + ", nextState:" + getSynchronizedState().name()
            , e);
        eventHandler.handle(new StageEvent(getId(),
            StageEventType.SQ_INTERNAL_ERROR));
      }

      // notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
        if (oldState != getSynchronizedState()) {
          LOG.debug(getId() + " Stage Transitioned from " + oldState + " to "
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

  private static class InitAndRequestContainer implements MultipleArcTransition<Stage,
      StageEvent, StageState> {

    @Override
    public StageState transition(final Stage stage, StageEvent stageEvent) {
      stage.setStartTime();
      ExecutionBlock execBlock = stage.getBlock();
      StageState state;

      try {
        // Union operator does not require actual query processing. It is performed logically.
        if (execBlock.hasUnion()) {
          stage.finalizeStats();
          state = StageState.SUCCEEDED;
        } else {
          ExecutionBlock parent = stage.getMasterPlan().getParent(stage.getBlock());
          DataChannel channel = stage.getMasterPlan().getChannel(stage.getId(), parent.getId());
          setShuffleIfNecessary(stage, channel);
          initTaskScheduler(stage);
          // execute pre-processing asyncronously
          stage.getContext().getQueryMasterContext().getEventExecutor()
              .submit(new Runnable() {
                        @Override
                        public void run() {
                          try {
                            schedule(stage);
                            stage.totalScheduledObjectsCount = stage.getTaskScheduler().remainingScheduledObjectNum();
                            LOG.info(stage.totalScheduledObjectsCount + " objects are scheduled");

                            if (stage.getTaskScheduler().remainingScheduledObjectNum() == 0) { // if there is no tasks
                              stage.finalizeStage();
                              stage.complete();
                            } else {
                              if(stage.getSynchronizedState() == StageState.INITED) {
                                stage.taskScheduler.start();
                                allocateContainers(stage);
                              } else {
                                /* all tasks are killed before stage are inited */
                                if (stage.getTotalScheduledObjectsCount() == stage.getCompletedTaskCount()) {
                                  stage.eventHandler.handle(
                                      new StageEvent(stage.getId(), StageEventType.SQ_STAGE_COMPLETED));
                                } else {
                                  stage.eventHandler.handle(
                                      new StageEvent(stage.getId(), StageEventType.SQ_KILL));
                                }
                              }
                            }
                          } catch (Throwable e) {
                            LOG.error("Stage (" + stage.getId() + ") ERROR: ", e);
                            stage.setFinishTime();
                            stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(), e.getMessage()));
                            stage.eventHandler.handle(new StageCompletedEvent(stage.getId(), StageState.ERROR));
                          }
                        }
                      }
              );
          state = StageState.INITED;
        }
      } catch (Throwable e) {
        LOG.error("Stage (" + stage.getId() + ") ERROR: ", e);
        stage.setFinishTime();
        stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(), e.getMessage()));
        stage.eventHandler.handle(new StageCompletedEvent(stage.getId(), StageState.ERROR));
        return StageState.ERROR;
      }

      return state;
    }

    private void initTaskScheduler(Stage stage) throws IOException {
      TajoConf conf = stage.context.getConf();
      stage.schedulerContext = new TaskSchedulerContext(stage.context,
          stage.getMasterPlan().isLeaf(stage.getId()), stage.getId());
      stage.taskScheduler = TaskSchedulerFactory.get(conf, stage.schedulerContext, stage);
      stage.taskScheduler.init(conf);
      LOG.info(stage.taskScheduler.getName() + " is chosen for the task scheduling for " + stage.getId());
    }

    /**
     * If a parent block requires a repartition operation, the method sets proper repartition
     * methods and the number of partitions to a given Stage.
     */
    private static void setShuffleIfNecessary(Stage stage, DataChannel channel) {
      if (channel.getShuffleType() != ShuffleType.NONE_SHUFFLE) {
        int numTasks = calculateShuffleOutputNum(stage, channel);
        Repartitioner.setShuffleOutputNumForTwoPhase(stage, numTasks, channel);
      }
    }

    /**
     * Getting the desire number of partitions according to the volume of input data.
     * This method is only used to determine the partition key number of hash join or aggregation.
     *
     * @param stage
     * @return
     */
    public static int calculateShuffleOutputNum(Stage stage, DataChannel channel) {
      TajoConf conf = stage.context.getConf();
      MasterPlan masterPlan = stage.getMasterPlan();
      ExecutionBlock parent = masterPlan.getParent(stage.getBlock());

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
        long outerVolume = getInputVolume(stage.masterPlan, stage.context, outer);

        // for inner
        ExecutionBlock inner = childs.get(1);
        long innerVolume = getInputVolume(stage.masterPlan, stage.context, inner);
        LOG.info(stage.getId() + ", Outer volume: " + Math.ceil((double) outerVolume / 1048576) + "MB, "
            + "Inner volume: " + Math.ceil((double) innerVolume / 1048576) + "MB");

        long bigger = Math.max(outerVolume, innerVolume);

        int mb = (int) Math.ceil((double) bigger / 1048576);
        LOG.info(stage.getId() + ", Bigger Table's volume is approximately " + mb + " MB");

        int taskNum = (int) Math.ceil((double) mb / masterPlan.getContext().getInt(SessionVars.JOIN_PER_SHUFFLE_SIZE));

        if (masterPlan.getContext().containsKey(SessionVars.TEST_MIN_TASK_NUM)) {
          taskNum = masterPlan.getContext().getInt(SessionVars.TEST_MIN_TASK_NUM);
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
          LOG.info(stage.getId() + ", Change determined number of join partitions cause difference of outputNum" +
                  ", originTaskNum=" + taskNum + ", changedTaskNum=" + Math.max(outerShuffleOutputNum, innerShuffleOutputNum) +
                  ", outerShuffleOutptNum=" + outerShuffleOutputNum +
                  ", innerShuffleOutputNum=" + innerShuffleOutputNum);
          taskNum = Math.max(outerShuffleOutputNum, innerShuffleOutputNum);
        }

        LOG.info(stage.getId() + ", The determined number of join partitions is " + taskNum);

        return taskNum;
        // Is this stage the first step of group-by?
      } else if (grpNode != null) {
        boolean hasGroupColumns = true;
        if (grpNode.getType() == NodeType.GROUP_BY) {
          hasGroupColumns = ((GroupbyNode)grpNode).getGroupingColumns().length > 0;
        } else if (grpNode.getType() == NodeType.DISTINCT_GROUP_BY) {
          // Find current distinct stage node.
          DistinctGroupbyNode distinctNode = PlannerUtil.findMostBottomNode(stage.getBlock().getPlan(), NodeType.DISTINCT_GROUP_BY);
          if (distinctNode == null) {
            LOG.warn(stage.getId() + ", Can't find current DistinctGroupbyNode");
            distinctNode = (DistinctGroupbyNode)grpNode;
          }
          hasGroupColumns = distinctNode.getGroupingColumns().length > 0;

          Enforcer enforcer = stage.getBlock().getEnforcer();
          if (enforcer == null) {
            LOG.warn(stage.getId() + ", DistinctGroupbyNode's enforcer is null.");
          }
          EnforceProperty property = PhysicalPlannerImpl.getAlgorithmEnforceProperty(enforcer, distinctNode);
          if (property != null) {
            if (property.getDistinct().getIsMultipleAggregation()) {
              MultipleAggregationStage multiAggStage = property.getDistinct().getMultipleAggregationStage();
              if (multiAggStage != MultipleAggregationStage.THRID_STAGE) {
                hasGroupColumns = true;
              }
            }
          }
        }
        if (!hasGroupColumns) {
          LOG.info(stage.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
          return 1;
        } else {
          long volume = getInputVolume(stage.masterPlan, stage.context, stage.block);

          int volumeByMB = (int) Math.ceil((double) volume / StorageUnit.MB);
          LOG.info(stage.getId() + ", Table's volume is approximately " + volumeByMB + " MB");
          // determine the number of task
          int taskNum = (int) Math.ceil((double) volumeByMB /
              masterPlan.getContext().getInt(SessionVars.GROUPBY_PER_SHUFFLE_SIZE));
          LOG.info(stage.getId() + ", The determined number of aggregation partitions is " + taskNum);
          return taskNum;
        }
      } else {
        LOG.info("============>>>>> Unexpected Case! <<<<<================");
        long volume = getInputVolume(stage.masterPlan, stage.context, stage.block);

        int mb = (int) Math.ceil((double)volume / 1048576);
        LOG.info(stage.getId() + ", Table's volume is approximately " + mb + " MB");
        // determine the number of task per 128MB
        int taskNum = (int) Math.ceil((double)mb / 128);
        LOG.info(stage.getId() + ", The determined number of partitions is " + taskNum);
        return taskNum;
      }
    }

    private static void schedule(Stage stage) throws IOException {
      MasterPlan masterPlan = stage.getMasterPlan();
      ExecutionBlock execBlock = stage.getBlock();
      if (stage.getMasterPlan().isLeaf(execBlock.getId()) && execBlock.getScanNodes().length == 1) { // Case 1: Just Scan
        scheduleFragmentsForLeafQuery(stage);
      } else if (execBlock.getScanNodes().length > 1) { // Case 2: Join
        Repartitioner.scheduleFragmentsForJoinQuery(stage.schedulerContext, stage);
      } else { // Case 3: Others (Sort or Aggregation)
        int numTasks = getNonLeafTaskNum(stage);
        Repartitioner.scheduleFragmentsForNonLeafTasks(stage.schedulerContext, masterPlan, stage, numTasks);
      }
    }

    /**
     * Getting the desire number of tasks according to the volume of input data
     *
     * @param stage
     * @return
     */
    public static int getNonLeafTaskNum(Stage stage) {
      // Getting intermediate data size
      long volume = getInputVolume(stage.getMasterPlan(), stage.context, stage.getBlock());

      int mb = (int) Math.ceil((double)volume / 1048576);
      LOG.info(stage.getId() + ", Table's volume is approximately " + mb + " MB");
      // determine the number of task per 64MB
      int minTaskNum = Math.max(1, stage.getContext().getQueryMasterContext().getConf().getInt(ConfVars.$TEST_MIN_TASK_NUM.varname, 1));
      int maxTaskNum = Math.max(minTaskNum, (int) Math.ceil((double)mb / 64));
      LOG.info(stage.getId() + ", The determined number of non-leaf tasks is " + maxTaskNum);
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
          Stage stage = context.getStage(childBlock.getId());
          if (stage == null || stage.getSynchronizedState() != StageState.SUCCEEDED) {
            aggregatedVolume += getInputVolume(masterPlan, context, childBlock);
          } else {
            aggregatedVolume += stage.getResultStats().getNumBytes();
          }
        }

        return aggregatedVolume;
      }
    }

    public static void allocateContainers(Stage stage) {
      ExecutionBlock execBlock = stage.getBlock();

      //TODO consider disk slot
      int requiredMemoryMBPerTask = 512;

      int numRequest = stage.getContext().getResourceAllocator().calculateNumRequestContainers(
          stage.getContext().getQueryMasterContext().getWorkerContext(),
          stage.schedulerContext.getEstimatedTaskNum(),
          requiredMemoryMBPerTask
      );

      final Resource resource = Records.newRecord(Resource.class);

      resource.setMemory(requiredMemoryMBPerTask);

      LOG.info("Request Container for " + stage.getId() + " containers=" + numRequest);

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(stage.getPriority());
      ContainerAllocationEvent event =
          new ContainerAllocationEvent(ContainerAllocatorEventType.CONTAINER_REQ,
              stage.getId(), priority, resource, numRequest,
              stage.masterPlan.isLeaf(execBlock), 0.0f);
      stage.eventHandler.handle(event);
    }

    private static void scheduleFragmentsForLeafQuery(Stage stage) throws IOException {
      ExecutionBlock execBlock = stage.getBlock();
      ScanNode[] scans = execBlock.getScanNodes();
      Preconditions.checkArgument(scans.length == 1, "Must be Scan Query");
      ScanNode scan = scans[0];
      TableDesc table = stage.context.getTableDescMap().get(scan.getCanonicalName());

      Collection<Fragment> fragments;
      TableMeta meta = table.getMeta();

      // Depending on scanner node's type, it creates fragments. If scan is for
      // a partitioned table, It will creates lots fragments for all partitions.
      // Otherwise, it creates at least one fragments for a table, which may
      // span a number of blocks or possibly consists of a number of files.
      if (scan.getType() == NodeType.PARTITIONS_SCAN) {
        // After calling this method, partition paths are removed from the physical plan.
        FileStorageManager storageManager =
            (FileStorageManager)StorageManager.getFileStorageManager(stage.getContext().getConf());
        fragments = Repartitioner.getFragmentsFromPartitionedTable(storageManager, scan, table);
      } else {
        StorageManager storageManager =
            StorageManager.getStorageManager(stage.getContext().getConf(), meta.getStoreType());
        fragments = storageManager.getSplits(scan.getCanonicalName(), table, scan);
      }

      Stage.scheduleFragments(stage, fragments);
      if (stage.getTaskScheduler() instanceof DefaultTaskScheduler) {
        //Leaf task of DefaultTaskScheduler should be fragment size
        // EstimatedTaskNum determined number of initial container
        stage.schedulerContext.setEstimatedTaskNum(fragments.size());
      } else {
        TajoConf conf = stage.context.getConf();
        stage.schedulerContext.setTaskSize(conf.getIntVar(ConfVars.TASK_DEFAULT_SIZE) * 1024 * 1024);
        int estimatedTaskNum = (int) Math.ceil((double) table.getStats().getNumBytes() /
            (double) stage.schedulerContext.getTaskSize());
        stage.schedulerContext.setEstimatedTaskNum(estimatedTaskNum);
      }
    }
  }

  public static void scheduleFragment(Stage stage, Fragment fragment) {
    stage.taskScheduler.handle(new FragmentScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        stage.getId(), fragment));
  }


  public static void scheduleFragments(Stage stage, Collection<Fragment> fragments) {
    for (Fragment eachFragment : fragments) {
      scheduleFragment(stage, eachFragment);
    }
  }

  public static void scheduleFragments(Stage stage, Collection<Fragment> leftFragments,
                                       Collection<Fragment> broadcastFragments) {
    for (Fragment eachLeafFragment : leftFragments) {
      scheduleFragment(stage, eachLeafFragment, broadcastFragments);
    }
  }

  public static void scheduleFragment(Stage stage,
                                      Fragment leftFragment, Collection<Fragment> rightFragments) {
    stage.taskScheduler.handle(new FragmentScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        stage.getId(), leftFragment, rightFragments));
  }

  public static void scheduleFetches(Stage stage, Map<String, List<FetchImpl>> fetches) {
    stage.taskScheduler.handle(new FetchScheduleEvent(TaskSchedulerEvent.EventType.T_SCHEDULE,
        stage.getId(), fetches));
  }

  public static Task newEmptyTask(TaskSchedulerContext schedulerContext,
                                  TaskAttemptScheduleContext taskContext,
                                  Stage stage, int taskId) {
    ExecutionBlock execBlock = stage.getBlock();
    Task unit = new Task(schedulerContext.getMasterContext().getConf(),
        taskContext,
        QueryIdFactory.newTaskId(schedulerContext.getBlockId(), taskId),
        schedulerContext.isLeafQuery(), stage.eventHandler);
    unit.setLogicalPlan(execBlock.getPlan());
    stage.addTask(unit);
    return unit;
  }

  private static class ContainerLaunchTransition
      implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(Stage stage, StageEvent event) {
      try {
        StageContainerAllocationEvent allocationEvent =
            (StageContainerAllocationEvent) event;
        for (TajoContainer container : allocationEvent.getAllocatedContainer()) {
          TajoContainerId cId = container.getId();
          if (stage.containers.containsKey(cId)) {
            stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(),
                "Duplicated containers are allocated: " + cId.toString()));
            stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_INTERNAL_ERROR));
          }
          stage.containers.put(cId, container);
        }
        LOG.info("Stage (" + stage.getId() + ") has " + stage.containers.size() + " containers!");
        stage.eventHandler.handle(
            new LaunchTaskRunnersEvent(stage.getId(), allocationEvent.getAllocatedContainer(),
                stage.getContext().getQueryContext(),
                CoreGsonHelper.toJson(stage.getBlock().getPlan(), LogicalNode.class))
        );

        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_START));
      } catch (Throwable t) {
        stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(),
            ExceptionUtils.getStackTrace(t)));
        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_INTERNAL_ERROR));
      }
    }
  }

  /**
   * It is used in KILL_WAIT state against Contained Allocated event.
   * It just returns allocated containers to resource manager.
   */
  private static class AllocatedContainersCancelTransition implements SingleArcTransition<Stage, StageEvent> {
    @Override
    public void transition(Stage stage, StageEvent event) {
      try {
        StageContainerAllocationEvent allocationEvent =
            (StageContainerAllocationEvent) event;
        stage.eventHandler.handle(
            new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP,
                stage.getId(), allocationEvent.getAllocatedContainer()));
        LOG.info(String.format("[%s] %d allocated containers are canceled",
            stage.getId().toString(),
            allocationEvent.getAllocatedContainer().size()));
      } catch (Throwable t) {
        stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(),
            ExceptionUtils.getStackTrace(t)));
        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_INTERNAL_ERROR));
      }
    }
  }

  private static class TaskCompletedTransition implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(Stage stage,
                           StageEvent event) {
      StageTaskEvent taskEvent = (StageTaskEvent) event;
      Task task = stage.getTask(taskEvent.getTaskId());

      if (task == null) { // task failed
        LOG.error(String.format("Task %s is absent", taskEvent.getTaskId()));
        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_FAILED));
      } else {
        stage.completedTaskCount++;

        if (taskEvent.getState() == TaskState.SUCCEEDED) {
          stage.succeededObjectCount++;
        } else if (task.getState() == TaskState.KILLED) {
          stage.killedObjectCount++;
        } else if (task.getState() == TaskState.FAILED) {
          stage.failedObjectCount++;
          // if at least one task is failed, try to kill all tasks.
          stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_KILL));
        }

        if (stage.totalScheduledObjectsCount == stage.completedTaskCount) {
          if (stage.succeededObjectCount == stage.completedTaskCount) {
            stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_SHUFFLE_REPORT));
          } else {
            stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_STAGE_COMPLETED));
          }
        } else {
          LOG.info(String.format("[%s] Task Completion Event (Total: %d, Success: %d, Killed: %d, Failed: %d)",
              stage.getId(),
              stage.totalScheduledObjectsCount,
              stage.succeededObjectCount,
              stage.killedObjectCount,
              stage.failedObjectCount));
        }
      }
    }
  }

  private static class KillTasksTransition implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(Stage stage, StageEvent stageEvent) {
      if(stage.getTaskScheduler() != null){
        stage.getTaskScheduler().stop();
      }

      for (Task task : stage.getTasks()) {
        stage.eventHandler.handle(new TaskEvent(task.getId(), TaskEventType.T_KILL));
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

    this.finalStageHistory = makeStageHistory();
    this.finalStageHistory.setTasks(makeTaskHistories());
  }

  public List<IntermediateEntry> getHashShuffleIntermediateEntries() {
    return hashShuffleIntermediateEntries;
  }

  protected void stopFinalization() {
    stopShuffleReceiver.set(true);
  }

  private static class StageFinalizeTransition implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(final Stage stage, StageEvent event) {
      //If a shuffle report are failed, remaining reports will ignore
      if (stage.stopShuffleReceiver.get()) {
        return;
      }

      stage.lastContactTime = System.currentTimeMillis();
      try {
        if (event instanceof StageShuffleReportEvent) {

          StageShuffleReportEvent finalizeEvent = (StageShuffleReportEvent) event;
          TajoWorkerProtocol.ExecutionBlockReport report = finalizeEvent.getReport();

          if (!report.getReportSuccess()) {
            stage.stopFinalization();
            LOG.error(stage.getId() + ", Shuffle report are failed. Caused by:" + report.getReportErrorMessage());
            stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_FAILED));
          }

          stage.completedShuffleTasks.addAndGet(finalizeEvent.getReport().getSucceededTasks());
          if (report.getIntermediateEntriesCount() > 0) {
            for (IntermediateEntryProto eachInterm : report.getIntermediateEntriesList()) {
              stage.hashShuffleIntermediateEntries.add(new IntermediateEntry(eachInterm));
            }
          }

          if (stage.completedShuffleTasks.get() >= stage.succeededObjectCount) {
            LOG.info(stage.getId() + ", Finalized shuffle reports: " + stage.completedShuffleTasks.get());
            stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_STAGE_COMPLETED));
            if (stage.timeoutChecker != null) {
              stage.stopFinalization();
              synchronized (stage.timeoutChecker){
                stage.timeoutChecker.notifyAll();
              }
            }
          } else {
            LOG.info(stage.getId() + ", Received shuffle report: " +
                stage.completedShuffleTasks.get() + "/" + stage.succeededObjectCount);
          }

        } else {
          LOG.info(String.format("Stage finalize - %s (total=%d, success=%d, killed=%d)",
              stage.getId().toString(),
              stage.totalScheduledObjectsCount,
              stage.succeededObjectCount,
              stage.killedObjectCount));
          stage.finalizeStage();
          LOG.info(stage.getId() + ", waiting for shuffle reports. expected Tasks:" + stage.succeededObjectCount);

          /* FIXME implement timeout handler of stage and task */
          if (stage.timeoutChecker != null) {
            stage.timeoutChecker = new Thread(new Runnable() {
              @Override
              public void run() {
                while (stage.getSynchronizedState() == StageState.FINALIZING && !Thread.interrupted()) {
                  long elapsedTime = System.currentTimeMillis() - stage.lastContactTime;
                  if (elapsedTime > 120 * 1000) {
                    stage.stopFinalization();
                    LOG.error(stage.getId() + ": Timed out while receiving intermediate reports: " + elapsedTime
                        + " ms, report:" + stage.completedShuffleTasks.get() + "/" + stage.succeededObjectCount);
                    stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_FAILED));
                  }
                  synchronized (this) {
                    try {
                      this.wait(1 * 1000);
                    } catch (InterruptedException e) {
                    }
                  }
                }
              }
            });
            stage.timeoutChecker.start();
          }
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        stage.stopFinalization();
        stage.eventHandler.handle(new StageDiagnosticsUpdateEvent(stage.getId(), t.getMessage()));
        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_INTERNAL_ERROR));
      }
    }
  }

  private static class StageCompleteTransition implements MultipleArcTransition<Stage, StageEvent, StageState> {

    @Override
    public StageState transition(Stage stage, StageEvent stageEvent) {
      // TODO - Commit Stage
      // TODO - records succeeded, failed, killed completed task
      // TODO - records metrics
      try {
        LOG.info(String.format("Stage completed - %s (total=%d, success=%d, killed=%d)",
            stage.getId().toString(),
            stage.getTotalScheduledObjectsCount(),
            stage.getSucceededObjectCount(),
            stage.killedObjectCount));

        if (stage.killedObjectCount > 0 || stage.failedObjectCount > 0) {
          if (stage.failedObjectCount > 0) {
            stage.abort(StageState.FAILED);
            return StageState.FAILED;
          } else if (stage.killedObjectCount > 0) {
            stage.abort(StageState.KILLED);
            return StageState.KILLED;
          } else {
            LOG.error("Invalid State " + stage.getSynchronizedState() + " State");
            stage.abort(StageState.ERROR);
            return StageState.ERROR;
          }
        } else {
          stage.complete();
          return StageState.SUCCEEDED;
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        stage.abort(StageState.ERROR);
        return StageState.ERROR;
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<Stage, StageEvent> {
    @Override
    public void transition(Stage stage, StageEvent event) {
      stage.addDiagnostic(((StageDiagnosticsUpdateEvent) event).getDiagnosticUpdate());
    }
  }

  private static class InternalErrorTransition implements SingleArcTransition<Stage, StageEvent> {
    @Override
    public void transition(Stage stage, StageEvent stageEvent) {
      stage.abort(StageState.ERROR);
    }
  }
}
