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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.ipc.TajoWorkerProtocol.TaskCompletionReport;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.QueryUnitAttemptScheduleEvent.QueryUnitAttemptScheduleContext;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.master.querymaster.QueryUnit.PullHost;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleFileOutput;

public class QueryUnitAttempt implements EventHandler<TaskAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(QueryUnitAttempt.class);

  private final static int EXPIRE_TIME = 15000;

  private final QueryUnitAttemptId id;
  private final QueryUnit queryUnit;
  final EventHandler eventHandler;

  private ContainerId containerId;
  private String hostName;
  private int port;
  private int expire;

  private final Lock readLock;
  private final Lock writeLock;

  private final List<String> diagnostics = new ArrayList<String>();

  private final QueryUnitAttemptScheduleContext scheduleContext;

  private float progress;
  private CatalogProtos.TableStatsProto inputStats;
  private CatalogProtos.TableStatsProto resultStats;

  protected static final StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory = new StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
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
      .installTopology();

  private final StateMachine<TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;


  public QueryUnitAttempt(final QueryUnitAttemptScheduleContext scheduleContext,
                          final QueryUnitAttemptId id, final QueryUnit queryUnit,
                          final EventHandler eventHandler) {
    this.scheduleContext = scheduleContext;
    this.id = id;
    this.expire = QueryUnitAttempt.EXPIRE_TIME;
    this.queryUnit = queryUnit;
    this.eventHandler = eventHandler;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public QueryUnitAttemptId getId() {
    return this.id;
  }

  public boolean isLeafTask() {
    return this.queryUnit.isLeafTask();
  }

  public QueryUnit getQueryUnit() {
    return this.queryUnit;
  }

  public String getHost() {
    return this.hostName;
  }

  public int getPort() {
    return this.port;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  public void setHost(String host) {
    this.hostName = host;
  }

  public void setPullServerPort(int port) {
    this.port = port;
  }

  public int getPullServerPort() {
    return port;
  }

  public synchronized void setExpireTime(int expire) {
    this.expire = expire;
  }

  public synchronized void updateExpireTime(int period) {
    this.setExpireTime(this.expire - period);
  }

  public synchronized void resetExpireTime() {
    this.setExpireTime(QueryUnitAttempt.EXPIRE_TIME);
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

  private void fillTaskStatistics(TaskCompletionReport report) {
    this.progress = 1.0f;

    List<IntermediateEntry> partitions = new ArrayList<IntermediateEntry>();

    if (report.getShuffleFileOutputsCount() > 0) {
      this.getQueryUnit().setShuffleFileOutputs(report.getShuffleFileOutputsList());

      PullHost host = new PullHost(getHost(), getPullServerPort());
      for (ShuffleFileOutput p : report.getShuffleFileOutputsList()) {
        IntermediateEntry entry = new IntermediateEntry(getId().getQueryUnitId().getId(),
            getId().getId(), p.getPartId(), host, p.getVolume());
        partitions.add(entry);
      }
    }
    this.getQueryUnit().setIntermediateData(partitions);

    if (report.hasInputStats()) {
      this.inputStats = report.getInputStats();
    }
    if (report.hasResultStats()) {
      this.resultStats = report.getResultStats();
      this.getQueryUnit().setStats(new TableStats(resultStats));
    }
  }

  private static class TaskAttemptScheduleTransition implements
      SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt, TaskAttemptEvent taskAttemptEvent) {
      taskAttempt.eventHandler.handle(new QueryUnitAttemptScheduleEvent(
          EventType.T_SCHEDULE, taskAttempt.getQueryUnit().getId().getExecutionBlockId(),
          taskAttempt.scheduleContext, taskAttempt));
    }
  }

  private static class KillUnassignedTaskTransition implements
      SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt, TaskAttemptEvent taskAttemptEvent) {
      taskAttempt.eventHandler.handle(new QueryUnitAttemptScheduleEvent(
          EventType.T_SCHEDULE_CANCEL, taskAttempt.getQueryUnit().getId().getExecutionBlockId(),
          taskAttempt.scheduleContext, taskAttempt));
    }
  }

  private static class LaunchTransition
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent event) {
      TaskAttemptAssignedEvent castEvent = (TaskAttemptAssignedEvent) event;
      taskAttempt.containerId = castEvent.getContainerId();
      taskAttempt.setHost(castEvent.getHostName());
      taskAttempt.setPullServerPort(castEvent.getPullServerPort());
      taskAttempt.eventHandler.handle(
          new TaskTAttemptEvent(taskAttempt.getId(),
              TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }

  private static class TaskKilledCompleteTransition implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent event) {
      taskAttempt.getQueryUnit().handle(new TaskEvent(taskAttempt.getId().getQueryUnitId(),
          TaskEventType.T_ATTEMPT_KILLED));
      LOG.info(taskAttempt.getId() + " Received TA_KILLED Status from LocalTask");
    }
  }

  private static class StatusUpdateTransition
      implements MultipleArcTransition<QueryUnitAttempt, TaskAttemptEvent, TaskAttemptState> {

    @Override
    public TaskAttemptState transition(QueryUnitAttempt taskAttempt,
                                       TaskAttemptEvent event) {
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
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent>{

    @Override
    public void transition(QueryUnitAttempt queryUnitAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
    }
  }

  private static class AlreadyDoneTransition
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent>{

    @Override
    public void transition(QueryUnitAttempt queryUnitAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
    }
  }

  private static class SucceededTransition implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {
    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent event) {
      TaskCompletionReport report = ((TaskCompletionEvent)event).getReport();

      try {
        taskAttempt.fillTaskStatistics(report);
        taskAttempt.eventHandler.handle(new TaskTAttemptEvent(taskAttempt.getId(), TaskEventType.T_ATTEMPT_SUCCEEDED));
      } catch (Throwable t) {
        taskAttempt.eventHandler.handle(new TaskFatalErrorEvent(taskAttempt.getId(), t.getMessage()));
        taskAttempt.addDiagnosticInfo(ExceptionUtils.getStackTrace(t));
      }
    }
  }

  private static class KillTaskTransition implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt, TaskAttemptEvent event) {
      taskAttempt.eventHandler.handle(new LocalTaskEvent(taskAttempt.getId(), taskAttempt.containerId,
          LocalTaskEventType.KILL));
    }
  }

  private static class FailedTransition implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent>{
    @Override
    public void transition(QueryUnitAttempt taskAttempt, TaskAttemptEvent event) {
      TaskFatalErrorEvent errorEvent = (TaskFatalErrorEvent) event;
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(taskAttempt.getId(), TaskEventType.T_ATTEMPT_FAILED));
      taskAttempt.addDiagnosticInfo(errorEvent.errorMessage());
      LOG.error(taskAttempt.getId() + " FROM " + taskAttempt.getHost() + " >> " + errorEvent.errorMessage());
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
            new SubQueryDiagnosticsUpdateEvent(event.getTaskAttemptId().getQueryUnitId().getExecutionBlockId(),
                "Can't handle this event at current state of " + event.getTaskAttemptId() + ")"));
        eventHandler.handle(
            new SubQueryEvent(event.getTaskAttemptId().getQueryUnitId().getExecutionBlockId(),
                SubQueryEventType.SQ_INTERNAL_ERROR));
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
