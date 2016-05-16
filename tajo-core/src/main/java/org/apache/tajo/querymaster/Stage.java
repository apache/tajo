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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.MasterPlan.ShuffleContext;
import org.apache.tajo.error.Errors.SerializedException;
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TaskState;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.serder.PlanProto.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.plan.serder.PlanProto.EnforceProperty;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.querymaster.Task.IntermediateEntry;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.RpcParameterFactory;
import org.apache.tajo.util.SplitUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.StageHistory;
import org.apache.tajo.util.history.TaskHistory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.ResourceProtos.*;
import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;


/**
 * Stage plays a role in controlling an ExecutionBlock and is a finite state machine.
 */
public class Stage implements EventHandler<StageEvent> {

  private static final Log LOG = LogFactory.getLog(Stage.class);

  private final Properties rpcParams;

  private MasterPlan masterPlan;
  private ExecutionBlock block;
  private int priority;
  private Schema outSchema;
  private TableMeta meta;
  private TableStats resultStatistics;
  private TableStats inputStatistics;
  private EventHandler<Event> eventHandler;
  private AbstractTaskScheduler taskScheduler;
  private QueryMasterTask.QueryMasterTaskContext context;
  private final List<String> diagnostics = new ArrayList<>();
  private StageState stageState;

  private long startTime;
  private long finishTime;
  private volatile long lastContactTime;
  private Thread timeoutChecker;

  private final Map<TaskId, Task> tasks = Maps.newConcurrentMap();
  private final Map<Integer, InetSocketAddress> workerMap = Maps.newConcurrentMap();

  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final TaskCompletedTransition TASK_COMPLETED_TRANSITION = new TaskCompletedTransition();
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
              StageEventType.SQ_START)
          .addTransition(StageState.INITED, StageState.INITED,
              StageEventType.SQ_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(StageState.INITED,
              EnumSet.of(StageState.SUCCEEDED, StageState.FAILED),
              StageEventType.SQ_STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(StageState.INITED, StageState.KILL_WAIT,
              StageEventType.SQ_KILL, new KillTasksTransition())
          .addTransition(StageState.INITED, StageState.ERROR,
              StageEventType.SQ_INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

              // Transitions from RUNNING state
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
              EnumSet.of(StageEventType.SQ_KILL),
              new KillTasksTransition())
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
              // Ignore-able events
          .addTransition(StageState.KILL_WAIT, StageState.KILL_WAIT,
              EnumSet.of(StageEventType.SQ_START))

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
                  StageEventType.SQ_SHUFFLE_REPORT))

          // Transitions from KILLED state
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
                  StageEventType.SQ_SHUFFLE_REPORT,
                  StageEventType.SQ_STAGE_COMPLETED,
                  StageEventType.SQ_FAILED))

          // Transitions from FAILED state
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
                  StageEventType.SQ_FAILED))

          // Transitions from ERROR state
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
                  StageEventType.SQ_STAGE_COMPLETED,
                  StageEventType.SQ_SHUFFLE_REPORT))

          .installTopology();

  private final Lock readLock;
  private final Lock writeLock;

  private volatile int totalScheduledObjectsCount;
  private volatile int completedTaskCount = 0;
  private volatile int succeededObjectCount = 0;
  private volatile int killedObjectCount = 0;
  private volatile int failedObjectCount = 0;
  private volatile SerializedException failureReason;
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

    this.rpcParams = RpcParameterFactory.get(context.getConf());

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
        tempTasks = new ArrayList<>(tasks.values());
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

  public SerializedException getFailureReason() {
    return failureReason;
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
    List<TaskHistory> taskHistories = new ArrayList<>();

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

    AbstractTaskScheduler scheduler = getTaskScheduler();
    if (scheduler != null) {
      stageHistory.setHostLocalAssigned(scheduler.getHostLocalAssigned());
      stageHistory.setRackLocalAssigned(scheduler.getRackLocalAssigned());
    }

    long totalInputBytes = 0;
    long totalReadBytes = 0;
    long totalReadRows = 0;
    long totalWriteBytes = 0;
    long totalWriteRows = 0;

    for(Task eachTask : getTasks()) {
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

    Set<Integer> partitions = Sets.newHashSet();
    for (IntermediateEntry entry : getHashShuffleIntermediateEntries()) {
       partitions.add(entry.getPartId());
    }

    stageHistory.setTotalInputBytes(totalInputBytes);
    stageHistory.setTotalReadBytes(totalReadBytes);
    stageHistory.setTotalReadRows(totalReadRows);
    stageHistory.setTotalWriteBytes(totalWriteBytes);
    stageHistory.setTotalWriteRows(totalWriteRows);
    stageHistory.setNumShuffles(partitions.size());
    stageHistory.setProgress(getProgress());
    return stageHistory;
  }

  public Set<PartitionDescProto> getPartitions() {
    Set<PartitionDescProto> partitions = new HashSet<>();
    for(Task eachTask : getTasks()) {
      if (eachTask.getLastAttempt() != null && !eachTask.getLastAttempt().getPartitions().isEmpty()) {
        partitions.addAll(eachTask.getLastAttempt().getPartitions());
      }
    }

    return partitions;
  }

  public void clearPartitions() {
    for(Task eachTask : getTasks()) {
      if (eachTask.getLastAttempt() != null && !eachTask.getLastAttempt().getPartitions().isEmpty()) {
        eachTask.getLastAttempt().getPartitions().clear();
      }
    }
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

  public void abort(StageState finalState) {
    abort(finalState, null);
  }

  /**
   * It finalizes this stage. Unlike {@link Stage#complete()},
   * it is invoked when a stage is abnormally finished.
   *
   * @param finalState The final stage state
   * @param reason The failure reason, if exist
   */
  public void abort(StageState finalState, Throwable reason) {
    // TODO -
    // - committer.abortStage(...)
    // - record Stage Finish Time
    // - CleanUp Tasks
    // - Record History
    if(reason != null)
      failureReason = ErrorUtil.convertException(reason);

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

  public Schema getOutSchema() {
    return outSchema;
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
    List<ColumnStats> columnStatses = StatisticsUtil.emptyColumnStats(stage.getDataChannel().getSchema());

    MasterPlan masterPlan = stage.getMasterPlan();
    for (ExecutionBlock block : masterPlan.getChilds(stage.getBlock())) {
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
      if (childStatArray[1].getColumnStats() != null && childStatArray[1].getColumnStats().size() > 0) {
        columnStatses = StatisticsUtil.aggregateColumnStats(columnStatses, childStatArray[1].getColumnStats());
      }
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
    if (taskScheduler != null) {
      taskScheduler.stop();
    }
  }

  /**
   * Get the launched worker address
   */
  protected Map<Integer, InetSocketAddress> getAssignedWorkerMap() {
    return workerMap;
  }

  private void sendStopExecutionBlockEvent(final StopExecutionBlockRequest requestProto) {

    for (final InetSocketAddress worker : getAssignedWorkerMap().values()) {
      getContext().getQueryMasterContext().getEventExecutor().submit(new Runnable() {
        @Override
        public void run() {
          try {
            AsyncRpcClient tajoWorkerRpc =
                RpcClientManager.getInstance().getClient(worker, TajoWorkerProtocol.class, true, rpcParams);
            TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
            tajoWorkerRpcClient.stopExecutionBlock(null,
                requestProto, NullCallback.get(PrimitiveProtos.BoolProto.class));
          } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
          }
        }
      });
    }
  }

  /**
   * Sends stopping request to all worker
   */
  protected void stopExecutionBlock() {
    // If there are still live tasks, try to kill the tasks. and send the shuffle report request

    List<TajoIdProtos.ExecutionBlockIdProto> ebIds = Lists.newArrayList();
    if (!getContext().getQueryContext().getBool(SessionVars.DEBUG_ENABLED)) {
      List<ExecutionBlock> childs = getMasterPlan().getChilds(getId());

      for (ExecutionBlock executionBlock : childs) {
        ebIds.add(executionBlock.getId().getProto());
      }
    }

    StopExecutionBlockRequest.Builder stopRequest = StopExecutionBlockRequest.newBuilder();
    ExecutionBlockListProto.Builder cleanupList = ExecutionBlockListProto.newBuilder();

    cleanupList.addAllExecutionBlockId(Lists.newArrayList(ebIds));
    stopRequest.setCleanupList(cleanupList.build());
    stopRequest.setExecutionBlockId(getId().getProto());
    sendStopExecutionBlockEvent(stopRequest.build());
  }

  /**
   * It computes all stats and sets the intermediate result.
   */
  private void finalizeStats() {
    TableStats[] statsArray;
    if (block.isUnionOnly()) {
      statsArray = computeStatFromUnionBlock(this);
    } else {
      statsArray = computeStatFromTasks();
    }

    DataChannel channel = masterPlan.getOutgoingChannels(getId()).get(0);

    // if store plan (i.e., CREATE or INSERT OVERWRITE)
    String dataFormat = PlannerUtil.getDataFormat(masterPlan.getLogicalPlan());
    if (dataFormat == null) {
      // get final output store type (i.e., SELECT)
      dataFormat = channel.getDataFormat();
    }

    outSchema = channel.getSchema();
    meta = CatalogUtil.newTableMeta(dataFormat, new KeyValueSet());
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

  private static class InitAndRequestContainer implements MultipleArcTransition<Stage,
      StageEvent, StageState> {

    @Override
    public StageState transition(final Stage stage, StageEvent stageEvent) {
      stage.setStartTime();
      ExecutionBlock execBlock = stage.getBlock();
      StageState state;

      try {
        // Union operator does not require actual query processing. It is performed logically.
        if (execBlock.isUnionOnly()) {
          // Though union operator does not be processed at all, but it should handle the completion event.
          stage.complete();
          state = StageState.SUCCEEDED;
        } else {
          ExecutionBlock parent = stage.getMasterPlan().getParent(stage.getBlock());
          DataChannel channel = stage.getMasterPlan().getChannel(stage.getId(), parent.getId());
          setShuffleIfNecessary(stage, channel);
          // TODO: verify changed shuffle plan
          initTaskScheduler(stage);
          // execute pre-processing asyncronously
          stage.getContext().getQueryMasterContext().getSingleEventExecutor()
              .submit(new Runnable() {
                        @Override
                        public void run() {
                          try {
                            schedule(stage);
                            stage.totalScheduledObjectsCount = stage.getTaskScheduler().remainingScheduledObjectNum();
                            LOG.info(stage.totalScheduledObjectsCount + " objects are scheduled");

                            if (stage.getTaskScheduler().remainingScheduledObjectNum() == 0) { // if there is no tasks
                              stage.eventHandler.handle(
                                  new StageEvent(stage.getId(), StageEventType.SQ_STAGE_COMPLETED));
                            } else {
                              if(stage.getSynchronizedState() == StageState.INITED) {
                                stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_START));
                                stage.taskScheduler.start();
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
      if (channel.isHashShuffle()) {
        int numTasks = calculateShuffleOutputNum(stage);
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
    public static int calculateShuffleOutputNum(Stage stage) {
      MasterPlan masterPlan = stage.getMasterPlan();

      // For test
      if (masterPlan.getContext().containsKey(SessionVars.TEST_MIN_TASK_NUM)) {
        int partitionNum = masterPlan.getContext().getInt(SessionVars.TEST_MIN_TASK_NUM);
        LOG.info(stage.getId() + ", The determined number of partitions is " + partitionNum + " for test");
        return partitionNum;
      }

      Optional<ShuffleContext> optional = masterPlan.getShuffleInfo(stage.getId());
      if (optional.isPresent()) {
        LOG.info("# of partitions is determined as " + optional.get().getPartitionNum() +
            "to match with sibling eb's partition number");
        return optional.get().getPartitionNum();

      } else {
        ExecutionBlock parent = masterPlan.getParent(stage.getBlock());
        int partitionNum;

        if (parent != null) {
          // We assume this execution block the first stage of join if two or more tables are included in this block,
          if (parent.hasJoin()) {
            if (parent.getNonBroadcastRelNum() > 1) {
              // repartition join
              partitionNum = calculatePartitionNumForRepartitionJoin(parent, stage);
              LOG.info(stage.getId() + ", The determined number of partitions for repartition join is " + partitionNum);
            } else {
              // broadcast join
              // partition number is calculated using the volume of the large table
              partitionNum = calculatePartitionNumDefault(parent, stage);
              LOG.info(stage.getId() + ", The determined number of partitions for broadcast join is " + partitionNum);
            }

          } else {
            // Is this stage the first step of group-by?
            if (parent.hasAgg()) {
              LogicalNode grpNode = PlannerUtil.findMostBottomNode(parent.getPlan(), NodeType.GROUP_BY,
                  NodeType.DISTINCT_GROUP_BY, NodeType.WINDOW_AGG);
              if (grpNode == null) {
                throw new TajoInternalError("Cannot find aggregation plan for " + stage.getId());
              }

              if (!hasGroupKeys(stage, grpNode)) {
                LOG.info(stage.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
                partitionNum = 1;
              } else {
                partitionNum = calculatePartitionNumForAgg(parent, stage);
                LOG.info(stage.getId() + ", The determined number of aggregation partitions is " + partitionNum);
              }

            } else {
              // NOTE: the below code might be executed during sort, but the partition number is not used anymore for sort.
              LOG.info("============>>>>> Unexpected Case! <<<<<================");
              partitionNum = calculatePartitionNumDefault(parent, stage);
              LOG.info(stage.getId() + ", The determined number of partitions is " + partitionNum);
            }

          }
        } else {
          // This case means that the parent eb does not exist even though data shuffle is required after the current eb.
          throw new TajoInternalError("Cannot find parent execution block of " + stage.block.getId());
        }

        // Record the partition number for sibling execution blocks
        masterPlan.addShuffleInfo(stage.getId(), partitionNum);
        return partitionNum;
      }
    }

    private static int calculatePartitionNumForRepartitionJoin(ExecutionBlock parent, Stage currentStage) {
      List<ExecutionBlock> childs = currentStage.masterPlan.getChilds(parent);

      // for outer
      ExecutionBlock outer = childs.get(0);
      long outerVolume = getInputVolume(currentStage.masterPlan, currentStage.context, outer);

      // for inner
      ExecutionBlock inner = childs.get(1);
      long innerVolume = getInputVolume(currentStage.masterPlan, currentStage.context, inner);
      LOG.info(currentStage.getId() + ", Outer volume: " + Math.ceil((double) outerVolume / 1048576) + "MB, "
          + "Inner volume: " + Math.ceil((double) innerVolume / 1048576) + "MB");

      long bigger = Math.max(outerVolume, innerVolume);

      int mb = (int) Math.ceil((double) bigger / 1048576);
      LOG.info(currentStage.getId() + ", Bigger Table's volume is approximately " + mb + " MB");

      return (int) Math.ceil((double) mb /
          currentStage.masterPlan.getContext().getInt(SessionVars.JOIN_PER_SHUFFLE_SIZE));
    }

    private static int calculatePartitionNumForAgg(ExecutionBlock parent, Stage stage) {
      int volumeByMB = getInputVolumeMB(parent, stage);
      LOG.info(stage.getId() + ", Table's volume is approximately " + volumeByMB + " MB");
      // determine the number of task
      return (int) Math.ceil((double) volumeByMB /
          stage.masterPlan.getContext().getInt(SessionVars.GROUPBY_PER_SHUFFLE_SIZE));

    }

    private static boolean hasGroupKeys(Stage currentStage, LogicalNode aggNode) {
      if (aggNode.getType() == NodeType.GROUP_BY) {
        return ((GroupbyNode)aggNode).getGroupingColumns().length > 0;
      } else if (aggNode.getType() == NodeType.DISTINCT_GROUP_BY) {
        // Find current distinct stage node.
        DistinctGroupbyNode distinctNode = PlannerUtil.findMostBottomNode(currentStage.getBlock().getPlan(),
            NodeType.DISTINCT_GROUP_BY);
        if (distinctNode == null) {
          LOG.warn(currentStage.getId() + ", Can't find current DistinctGroupbyNode");
          distinctNode = (DistinctGroupbyNode)aggNode;
        }
        boolean hasGroupColumns = distinctNode.getGroupingColumns().length > 0;

        Enforcer enforcer = currentStage.getBlock().getEnforcer();
        if (enforcer == null) {
          LOG.warn(currentStage.getId() + ", DistinctGroupbyNode's enforcer is null.");
        } else {
          EnforceProperty property = PhysicalPlannerImpl.getAlgorithmEnforceProperty(enforcer, distinctNode);
          if (property != null) {
            if (property.getDistinct().getIsMultipleAggregation()) {
              MultipleAggregationStage multiAggStage = property.getDistinct().getMultipleAggregationStage();
              hasGroupColumns = multiAggStage != MultipleAggregationStage.THRID_STAGE;
            }
          }
        }
        return hasGroupColumns;
      } else {
        return ((WindowAggNode) aggNode).hasPartitionKeys();
      }
    }

    private static int calculatePartitionNumDefault(ExecutionBlock parent, Stage currentStage) {
      int mb = getInputVolumeMB(parent, currentStage);
      LOG.info(currentStage.getId() + ", Table's volume is approximately " + mb + " MB");
      // determine the number of task per 128 MB
      return (int) Math.ceil((double)mb / 128);
    }

    private static int getInputVolumeMB(ExecutionBlock parent, Stage currentStage) {
      // NOTE: Get input volume from the parent EB.
      // If the parent EB contains an UNION query, the volume of the whole input for the UNION is returned.
      // Otherwise, only the input volume of the current EB is returned.
      long volume = getInputVolume(currentStage.masterPlan, currentStage.context, parent);

      return (int) Math.ceil((double)volume / StorageUnit.MB);
    }

    private static void schedule(Stage stage) throws IOException, TajoException {
      MasterPlan masterPlan = stage.getMasterPlan();
      ExecutionBlock execBlock = stage.getBlock();
      if (stage.getMasterPlan().isLeaf(execBlock.getId()) && execBlock.getScanNodes().length == 1) { // Case 1: Just Scan
        // Some execution blocks can have broadcast table even though they don't have any join nodes
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
      // This method is assumed to be called only for aggregation or sort.
      LogicalNode plan = stage.getBlock().getPlan();
      LogicalNode sortNode = PlannerUtil.findTopNode(plan, NodeType.SORT);
      LogicalNode groupbyNode = PlannerUtil.findTopNode(plan, NodeType.GROUP_BY);

      // Task volume is assumed to be 64 MB by default.
      long taskVolume = 64;

      if (groupbyNode != null && sortNode == null) {
        // aggregation plan
        taskVolume = stage.getContext().getQueryContext().getLong(SessionVars.GROUPBY_TASK_INPUT_SIZE);
      } else if (sortNode != null && groupbyNode == null) {
        // sort plan
        taskVolume = stage.getContext().getQueryContext().getLong(SessionVars.SORT_TASK_INPUT_SIZE);
      } else if (sortNode != null /* && groupbyNode != null */) {
        // NOTE: when the plan includes both aggregation and sort, usually aggregation is executed first.
        // If not, we need to check the query plan is valid.
        LogicalNode aggChildOfSort = PlannerUtil.findTopNode(sortNode, NodeType.GROUP_BY);
        boolean aggFirst = aggChildOfSort != null && aggChildOfSort.equals(groupbyNode);
        // Set task volume according to the operator which will be executed first.
        if (aggFirst) {
          // choose aggregation task volume
          taskVolume = stage.getContext().getQueryContext().getLong(SessionVars.GROUPBY_TASK_INPUT_SIZE);
        } else {
          // choose sort task volume
          LOG.warn("Sort is executed before aggregation.");
          taskVolume = stage.getContext().getQueryContext().getLong(SessionVars.SORT_TASK_INPUT_SIZE);
        }
      } else {
        LOG.warn("Task volume is chosen as " + taskVolume + " in unexpected case.");
      }

      // Getting intermediate data size
      long volume = getInputVolume(stage.getMasterPlan(), stage.context, stage.getBlock());

      int mb = (int) Math.ceil((double)volume / (double)StorageUnit.MB);
      LOG.info(stage.getId() + ", Table's volume is approximately " + mb + " MB");
      // determine the number of task
      int minTaskNum = Math.max(1, stage.getContext().getQueryMasterContext().getConf().
          getInt(ConfVars.$TEST_MIN_TASK_NUM.varname, 1));
      int maxTaskNum = Math.max(minTaskNum, (int) Math.ceil((double)mb / taskVolume));
      LOG.info(stage.getId() + ", The determined number of non-leaf tasks is " + maxTaskNum);
      return maxTaskNum;
    }

    public static long getInputVolume(MasterPlan masterPlan, QueryMasterTask.QueryMasterTaskContext context,
                                      ExecutionBlock execBlock) {
      if (masterPlan.isLeaf(execBlock)) {
        ScanNode[] outerScans = execBlock.getScanNodes();
        long maxVolume = 0;
        for (ScanNode eachScanNode: outerScans) {
          TableStats stat = context.getTableDesc(eachScanNode).getStats();
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

    private static void scheduleFragmentsForLeafQuery(Stage stage) throws IOException, TajoException {
      ExecutionBlock execBlock = stage.getBlock();
      ScanNode[] scans = execBlock.getScanNodes();
      Preconditions.checkArgument(scans.length == 1, "Must be Scan Query");
      ScanNode scan = scans[0];
      TableDesc table = stage.context.getTableDesc(scan);

      Collection<Fragment> fragments = SplitUtil.getSplits(
          TablespaceManager.get(scan.getTableDesc().getUri()), scan, table, false);
      SplitUtil.preparePartitionScanPlanForSchedule(scan);
      Stage.scheduleFragments(stage, fragments);

      // The number of leaf tasks should be the number of fragments.
      stage.schedulerContext.setEstimatedTaskNum(fragments.size());
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

  public static void scheduleFetches(Stage stage, Map<String, List<FetchProto>> fetches) {
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

  private static class TaskCompletedTransition implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(Stage stage,
                           StageEvent event) {
      if (!(event instanceof StageTaskEvent)) {
        throw new IllegalArgumentException("event should be a StageTaskEvent type.");
      }
      StageTaskEvent taskEvent = (StageTaskEvent) event;
      Task task = stage.getTask(taskEvent.getTaskId());

      if (task == null) { // task failed
        LOG.error(String.format("Task %s is absent", taskEvent.getTaskId()));
        stage.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_FAILED));
      } else {
        stage.completedTaskCount++;
        stage.getTaskScheduler().releaseTaskAttempt(task.getLastAttempt());

        if (taskEvent.getState() == TaskState.SUCCEEDED) {
          stage.succeededObjectCount++;
        } else if (task.getState() == TaskState.KILLED) {
          stage.killedObjectCount++;
        } else if (task.getState() == TaskState.FAILED) {
          StageTaskFailedEvent failedEvent = TUtil.checkTypeAndGet(event, StageTaskFailedEvent.class);
          stage.failedObjectCount++;
          stage.failureReason = failedEvent.getException();
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
    stopExecutionBlock();
    this.finalStageHistory = makeStageHistory();
    this.finalStageHistory.setTasks(makeTaskHistories());
  }

  public List<IntermediateEntry> getHashShuffleIntermediateEntries() {
    return hashShuffleIntermediateEntries;
  }

  protected void stopFinalization() {
    stopShuffleReceiver.set(true);
  }

  private void finalizeShuffleReport(StageShuffleReportEvent event, ShuffleType type) {
    if(!checkIfNeedFinalizing(type)) return;

    ExecutionBlockReport report = event.getReport();

    if (!report.getReportSuccess()) {
      stopFinalization();
      LOG.error(getId() + ", " + type + " report are failed. Caused by:" + report.getReportErrorMessage());
      getEventHandler().handle(new StageEvent(getId(), StageEventType.SQ_FAILED));
    }

    completedShuffleTasks.addAndGet(report.getSucceededTasks());
    if (report.getIntermediateEntriesCount() > 0) {
      for (IntermediateEntryProto eachInterm : report.getIntermediateEntriesList()) {
        hashShuffleIntermediateEntries.add(new IntermediateEntry(eachInterm));
      }
    }

    if (completedShuffleTasks.get() >= succeededObjectCount) {
      LOG.info(getId() + ", Finalized " + type + " reports: " + completedShuffleTasks.get());
      getEventHandler().handle(new StageEvent(getId(), StageEventType.SQ_STAGE_COMPLETED));
      if (timeoutChecker != null) {
        stopFinalization();
        synchronized (timeoutChecker){
          timeoutChecker.notifyAll();
        }
      }
    } else {
      LOG.info(getId() + ", Received " + type + " reports " +
          completedShuffleTasks.get() + "/" + succeededObjectCount);
    }
  }

  /**
   * HASH_SHUFFLE, SCATTERED_HASH_SHUFFLE should get report from worker nodes when ExecutionBlock is stopping.
   * RANGE_SHUFFLE report is sent from task reporter when a task finished in worker node.
   */
  public static boolean checkIfNeedFinalizing(ShuffleType type) {
    switch (type) {
      case HASH_SHUFFLE:
      case SCATTERED_HASH_SHUFFLE:
        return true;
      default:
        return false;
    }
  }

  private static class StageFinalizeTransition implements SingleArcTransition<Stage, StageEvent> {

    @Override
    public void transition(final Stage stage, StageEvent event) {
      //If a shuffle report are failed, remaining reports will ignore
      if (stage.stopShuffleReceiver.get()) {
        return;
      }

      stage.lastContactTime = System.currentTimeMillis();
      ShuffleType shuffleType = stage.getDataChannel().getShuffleType();
      try {
        if (event instanceof StageShuffleReportEvent) {
          stage.finalizeShuffleReport((StageShuffleReportEvent) event, shuffleType);
        } else {
          LOG.info(String.format("Stage - %s finalize %s (total=%d, success=%d, killed=%d)",
              stage.getId().toString(),
              shuffleType,
              stage.totalScheduledObjectsCount,
              stage.succeededObjectCount,
              stage.killedObjectCount));
          stage.finalizeStage();

          if (checkIfNeedFinalizing(shuffleType)) {
            /* wait for StageShuffleReportEvent from worker nodes */

            LOG.info(stage.getId() + ", wait for " + shuffleType + " reports. expected Tasks:"
                + stage.succeededObjectCount);
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
                      stage.getEventHandler().handle(new StageEvent(stage.getId(), StageEventType.SQ_FAILED));
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
          } else {
            stage.getEventHandler().handle(new StageEvent(stage.getId(), StageEventType.SQ_STAGE_COMPLETED));
          }
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        stage.stopFinalization();
        stage.getEventHandler().handle(new StageDiagnosticsUpdateEvent(stage.getId(), t.getMessage()));
        stage.getEventHandler().handle(new StageEvent(stage.getId(), StageEventType.SQ_INTERNAL_ERROR));
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

        // If the current stage are failed, next stages receives SQ_KILL event
        if (stage.killedObjectCount + stage.failedObjectCount > 0) {
          if (stage.failedObjectCount > 0) {
            stage.abort(StageState.FAILED);
            return StageState.FAILED;
          } else {
            stage.abort(StageState.KILLED);
            return StageState.KILLED;
          }
        } else {
          stage.complete();
          return StageState.SUCCEEDED;
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        stage.abort(StageState.ERROR, t);
        return StageState.ERROR;
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<Stage, StageEvent> {
    @Override
    public void transition(Stage stage, StageEvent event) {
      if (!(event instanceof StageDiagnosticsUpdateEvent)) {
        throw new IllegalArgumentException("event should be a StageDiagnosticsUpdateEvent type.");
      }
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
