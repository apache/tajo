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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.ResourceProtos.TaskCompletionReport;
import org.apache.tajo.ResourceProtos.ShuffleFileOutput;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskAttemptToSchedulerEvent.TaskAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.querymaster.Task.IntermediateEntry;
import org.apache.tajo.querymaster.Task.PullHost;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TaskAttempt implements EventHandler<TaskAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(TaskAttempt.class);

  private final static int EXPIRE_TIME = 15000;

  private final TaskAttemptId id;
  private final Task task;
  final EventHandler eventHandler;

  private WorkerConnectionInfo workerConnectionInfo;
  private int expire;

  private final Lock readLock;
  private final Lock writeLock;

  private final List<String> diagnostics = new ArrayList<>();

  private final TaskAttemptScheduleContext scheduleContext;

  private float progress;
  private CatalogProtos.TableStatsProto inputStats;
  private CatalogProtos.TableStatsProto resultStats;

  private Set<PartitionDescProto> partitions;

  protected static final StateMachineFactory
      <TaskAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory = new StateMachineFactory
      <TaskAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptState.TA_NEW)

      // Transitions from TA_NEW state
      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_SCHEDULE, new TaskAttemptScheduleTransition())
      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_RESCHEDULE, new TaskAttemptScheduleTransition())
      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_KILL,
          new TaskKilledCompleteTransition())

      // Transitions from TA_UNASSIGNED state
      .addTransition(TaskAttemptState.TA_UNASSIGNED, TaskAttemptState.TA_ASSIGNED,
          TaskAttemptEventType.TA_ASSIGNED,
          new LaunchTransition())
      .addTransition(TaskAttemptState.TA_UNASSIGNED, TaskAttemptState.TA_KILL_WAIT,
          TaskAttemptEventType.TA_KILL,
          new KillUnassignedTaskTransition())

      // Transitions from TA_ASSIGNED state
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_ASSIGNED,
          TaskAttemptEventType.TA_ASSIGNED, new AlreadyAssignedTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_KILL_WAIT,
          TaskAttemptEventType.TA_KILL,
          new KillTaskTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_KILL,
          new KillTaskTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED,
          EnumSet.of(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_KILLED),
          TaskAttemptEventType.TA_UPDATE, new StatusUpdateTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new SucceededTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_ASSIGN_CANCEL, new CancelTransition())

      // Transitions from TA_RUNNING state
      .addTransition(TaskAttemptState.TA_RUNNING,
          EnumSet.of(TaskAttemptState.TA_RUNNING),
          TaskAttemptEventType.TA_UPDATE, new StatusUpdateTransition())
      .addTransition(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_KILL_WAIT,
          TaskAttemptEventType.TA_KILL,
          new KillTaskTransition())
      .addTransition(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new SucceededTransition())
      .addTransition(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())

      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_LOCAL_KILLED,
          new TaskKilledCompleteTransition())
      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_KILL_WAIT,
          TaskAttemptEventType.TA_ASSIGNED,
          new KillTaskTransition())
      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_SCHEDULE_CANCELED,
          new TaskKilledCompleteTransition())
      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_DONE,
          new TaskKilledCompleteTransition())
      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR)
      .addTransition(TaskAttemptState.TA_KILL_WAIT, TaskAttemptState.TA_KILL_WAIT,
          EnumSet.of(
              TaskAttemptEventType.TA_KILL,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              TaskAttemptEventType.TA_UPDATE))

      // Transitions from TA_SUCCEEDED state
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_UPDATE)
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new AlreadyDoneTransition())
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())
       // Ignore-able transitions
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_KILL)

      // Transitions from TA_KILLED state
      .addTransition(TaskAttemptState.TA_KILLED, TaskAttemptState.TA_KILLED,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE)
      // Ignore-able transitions
      .addTransition(TaskAttemptState.TA_KILLED, TaskAttemptState.TA_KILLED,
          EnumSet.of(
              TaskAttemptEventType.TA_UPDATE))
      .addTransition(TaskAttemptState.TA_KILLED, TaskAttemptState.TA_KILLED,
          EnumSet.of(
              TaskAttemptEventType.TA_LOCAL_KILLED,
              TaskAttemptEventType.TA_KILL,
              TaskAttemptEventType.TA_ASSIGNED,
              TaskAttemptEventType.TA_DONE),
          new TaskKilledCompleteTransition())

          // Transitions from TA_FAILED state
      .addTransition(TaskAttemptState.TA_FAILED, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_KILL)
      .installTopology();

  private final StateMachine<TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;


  public TaskAttempt(final TaskAttemptScheduleContext scheduleContext,
                     final TaskAttemptId id, final Task task,
                     final EventHandler eventHandler) {
    this.scheduleContext = scheduleContext;
    this.id = id;
    this.expire = TaskAttempt.EXPIRE_TIME;
    this.task = task;
    this.eventHandler = eventHandler;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
    this.partitions = new HashSet<>();
  }

  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public TaskAttemptId getId() {
    return this.id;
  }

  public boolean isLeafTask() {
    return this.task.isLeafTask();
  }

  public Task getTask() {
    return this.task;
  }

  public WorkerConnectionInfo getWorkerConnectionInfo() {
    return this.workerConnectionInfo;
  }

  public synchronized void setExpireTime(int expire) {
    this.expire = expire;
  }

  public synchronized void updateExpireTime(int period) {
    this.setExpireTime(this.expire - period);
  }

  public synchronized void resetExpireTime() {
    this.setExpireTime(TaskAttempt.EXPIRE_TIME);
  }

  public int getLeftTime() {
    return this.expire;
  }

  public float getProgress() {
    return progress;
  }

  public TableStats getInputStats() {
    if (inputStats == null) {
      return null;
    }

    return new TableStats(inputStats);
  }

  public TableStats getResultStats() {
    if (resultStats == null) {
      return null;
    }
    return new TableStats(resultStats);
  }

  public Set<PartitionDescProto> getPartitions() {
    return partitions;
  }

  public void addPartitions(List<PartitionDescProto> partitions) {
    this.partitions.addAll(partitions);
  }

  private void fillTaskStatistics(TaskCompletionReport report) {
    this.progress = 1.0f;

    List<IntermediateEntry> partitions = new ArrayList<>();

    if (report.getShuffleFileOutputsCount() > 0) {
      this.getTask().setShuffleFileOutputs(report.getShuffleFileOutputsList());

      PullHost host = new PullHost(getWorkerConnectionInfo().getHost(), getWorkerConnectionInfo().getPullServerPort());
      for (ShuffleFileOutput p : report.getShuffleFileOutputsList()) {
        IntermediateEntry entry = new IntermediateEntry(getId().getTaskId().getId(),
            getId().getId(), p.getPartId(), host, p.getVolume());
        partitions.add(entry);
      }
    }
    this.getTask().setIntermediateData(partitions);

    if (report.hasInputStats()) {
      this.inputStats = report.getInputStats();
    }
    if (report.hasResultStats()) {
      this.resultStats = report.getResultStats();
      this.getTask().setStats(new TableStats(resultStats));
    }
  }

  private static class TaskAttemptScheduleTransition implements
      SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt, TaskAttemptEvent taskAttemptEvent) {
      taskAttempt.eventHandler.handle(new TaskAttemptToSchedulerEvent(
          EventType.T_SCHEDULE, taskAttempt.getTask().getId().getExecutionBlockId(),
          taskAttempt.scheduleContext, taskAttempt));
    }
  }

  private static class KillUnassignedTaskTransition implements
      SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt, TaskAttemptEvent taskAttemptEvent) {
      taskAttempt.eventHandler.handle(new TaskAttemptToSchedulerEvent(
          EventType.T_SCHEDULE_CANCEL, taskAttempt.getTask().getId().getExecutionBlockId(),
          taskAttempt.scheduleContext, taskAttempt));
    }
  }

  private static class LaunchTransition
      implements SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent event) {
      if (!(event instanceof TaskAttemptAssignedEvent)) {
        throw new IllegalArgumentException("event should be a TaskAttemptAssignedEvent type.");
      }
      TaskAttemptAssignedEvent castEvent = (TaskAttemptAssignedEvent) event;
      taskAttempt.workerConnectionInfo = castEvent.getWorkerConnectionInfo();
      taskAttempt.eventHandler.handle(
          new TaskTAttemptEvent(taskAttempt.getId(),
              TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }

  private static class CancelTransition
      implements SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent event) {

      taskAttempt.workerConnectionInfo = null;
    }
  }

  private static class TaskKilledCompleteTransition implements SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent event) {
      taskAttempt.getTask().handle(new TaskEvent(taskAttempt.getId().getTaskId(),
          TaskEventType.T_ATTEMPT_KILLED));
      LOG.info(taskAttempt.getId() + " Received TA_KILLED Status from LocalTask");
    }
  }

  private static class StatusUpdateTransition
      implements MultipleArcTransition<TaskAttempt, TaskAttemptEvent, TaskAttemptState> {

    @Override
    public TaskAttemptState transition(TaskAttempt taskAttempt,
                                       TaskAttemptEvent event) {
      if (!(event instanceof TaskAttemptStatusUpdateEvent)) {
        throw new IllegalArgumentException("event should be a TaskAttemptStatusUpdateEvent type.");
      }
      TaskAttemptStatusUpdateEvent updateEvent = (TaskAttemptStatusUpdateEvent) event;

      taskAttempt.progress = updateEvent.getStatus().getProgress();
      taskAttempt.inputStats = updateEvent.getStatus().getInputStats();
      taskAttempt.resultStats = updateEvent.getStatus().getResultStats();

      return TaskAttemptState.TA_RUNNING;
    }
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  private static class AlreadyAssignedTransition
      implements SingleArcTransition<TaskAttempt, TaskAttemptEvent>{

    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
    }
  }

  private static class AlreadyDoneTransition
      implements SingleArcTransition<TaskAttempt, TaskAttemptEvent>{

    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
    }
  }

  private static class SucceededTransition implements SingleArcTransition<TaskAttempt, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttempt taskAttempt,
                           TaskAttemptEvent event) {
      if (!(event instanceof TaskCompletionEvent)) {
        throw new IllegalArgumentException("event should be a TaskCompletionEvent type.");
      }
      TaskCompletionReport report = ((TaskCompletionEvent)event).getReport();

      try {
        if (report.getPartitionsCount() > 0) {
          taskAttempt.addPartitions(report.getPartitionsList());
        }

        taskAttempt.fillTaskStatistics(report);
        taskAttempt.eventHandler.handle(new TaskTAttemptEvent(taskAttempt.getId(), TaskEventType.T_ATTEMPT_SUCCEEDED));
      } catch (Throwable t) {
        taskAttempt.eventHandler.handle(new TaskFatalErrorEvent(taskAttempt.getId(), t));
        taskAttempt.addDiagnosticInfo(ExceptionUtils.getStackTrace(t));
      }
    }
  }

  private static class KillTaskTransition implements SingleArcTransition<TaskAttempt, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttempt taskAttempt, TaskAttemptEvent event) {
      taskAttempt.eventHandler.handle(new LocalTaskEvent(taskAttempt.getId(),
          taskAttempt.getWorkerConnectionInfo().getId(),
          LocalTaskEventType.KILL));
    }
  }

  private static class FailedTransition implements SingleArcTransition<TaskAttempt, TaskAttemptEvent>{
    @Override
    public void transition(TaskAttempt taskAttempt, TaskAttemptEvent event) {
      if (!(event instanceof TaskFatalErrorEvent)) {
        throw new IllegalArgumentException("event should be a TaskFatalErrorEvent type.");
      }
      TaskFatalErrorEvent errorEvent = (TaskFatalErrorEvent) event;
      taskAttempt.eventHandler.handle(new TaskTAttemptFailedEvent(taskAttempt.getId(), errorEvent.getError()));
      taskAttempt.addDiagnosticInfo(errorEvent.getError().getMessage());
      LOG.error(taskAttempt.getId() + " FROM " + taskAttempt.getWorkerConnectionInfo().getHost()
          + " >> " + errorEvent.getError().getMessage());
    }
  }

  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptId() + " of type " + event.getType());
    }
    try {
      writeLock.lock();
      TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state of " + event.getTaskAttemptId() + ")"
            + ", eventType:" + event.getType().name()
            + ", oldState:" + oldState.name()
            + ", nextState:" + getState().name()
            , e);
        eventHandler.handle(
            new StageDiagnosticsUpdateEvent(event.getTaskAttemptId().getTaskId().getExecutionBlockId(),
                "Can't handle this event at current state of " + event.getTaskAttemptId() + ")"));
        eventHandler.handle(
            new StageEvent(event.getTaskAttemptId().getTaskId().getExecutionBlockId(),
                StageEventType.SQ_INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
       if (oldState != getState()) {
          LOG.debug(id + " TaskAttempt Transitioned from " + oldState + " to "
              + getState());
        }
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
