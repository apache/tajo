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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.ipc.TajoWorkerProtocol.Partition;
import org.apache.tajo.ipc.TajoWorkerProtocol.TaskCompletionReport;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.util.TajoIdUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryUnitAttempt implements EventHandler<TaskAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(QueryUnitAttempt.class);

  private final static int EXPIRE_TIME = 15000;

  private final QueryUnitAttemptId id;
  private final QueryUnit queryUnit;
  final EventHandler eventHandler;

  private String hostName;
  private int port;
  private int expire;

  private final Lock readLock;
  private final Lock writeLock;

  private final List<String> diagnostics = new ArrayList<String>();

  protected static final StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory = new StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptState.TA_NEW)

      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_SCHEDULE, new TaskAttemptScheduleTransition())
      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_RESCHEDULE, new TaskAttemptScheduleTransition())

      .addTransition(TaskAttemptState.TA_UNASSIGNED, TaskAttemptState.TA_ASSIGNED,
          TaskAttemptEventType.TA_ASSIGNED, new LaunchTransition())

      // from assigned
      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_ASSIGNED,
          TaskAttemptEventType.TA_ASSIGNED, new AlreadyAssignedTransition())
      .addTransition(TaskAttemptState.TA_ASSIGNED,
          EnumSet.of(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_KILLED),
          TaskAttemptEventType.TA_UPDATE, new StatusUpdateTransition())

      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new SucceededTransition())

      .addTransition(TaskAttemptState.TA_ASSIGNED, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())

      // from running
      .addTransition(TaskAttemptState.TA_RUNNING,
          EnumSet.of(TaskAttemptState.TA_RUNNING),
          TaskAttemptEventType.TA_UPDATE, new StatusUpdateTransition())

      .addTransition(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new SucceededTransition())

      .addTransition(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())

      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_UPDATE)
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_DONE, new AlreadyDoneTransition())
      .addTransition(TaskAttemptState.TA_SUCCEEDED, TaskAttemptState.TA_FAILED,
          TaskAttemptEventType.TA_FATAL_ERROR, new FailedTransition())

      .installTopology();

  private final StateMachine<TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;


  public QueryUnitAttempt(final QueryUnitAttemptId id, final QueryUnit queryUnit,
                          final EventHandler eventHandler) {
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

  private void fillTaskStatistics(TaskCompletionReport report) {
    if (report.getPartitionsCount() > 0) {
      this.getQueryUnit().setPartitions(report.getPartitionsList());

      List<IntermediateEntry> partitions = new ArrayList<IntermediateEntry>();
      for (Partition p : report.getPartitionsList()) {
        IntermediateEntry entry = new IntermediateEntry(getId().getQueryUnitId().getId(),
            getId().getId(), p.getPartitionKey(), getHost(), getPullServerPort());
        partitions.add(entry);
      }
      this.getQueryUnit().setIntermediateData(partitions);
    }
    if (report.hasResultStats()) {
      this.getQueryUnit().setStats(new TableStats(report.getResultStats()));
    }
  }

  private static class TaskAttemptScheduleTransition implements
    SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
      TaskAttemptScheduleEvent castEvent = (TaskAttemptScheduleEvent) taskAttemptEvent;
      taskAttempt.eventHandler.handle(
          TaskSchedulerEventFactory.getTaskSchedulerEvent(castEvent.getConf(), taskAttempt, EventType.T_SCHEDULE));
    }
  }

  private static class LaunchTransition
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent event) {
      TaskAttemptAssignedEvent castEvent = (TaskAttemptAssignedEvent) event;
      taskAttempt.setHost(castEvent.getHostName());
      taskAttempt.setPullServerPort(castEvent.getPullServerPort());
      taskAttempt.eventHandler.handle(
          new TaskTAttemptEvent(taskAttempt.getId(),
              TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }

  private static class StatusUpdateTransition
      implements MultipleArcTransition<QueryUnitAttempt, TaskAttemptEvent, TaskAttemptState> {

    @Override
    public TaskAttemptState transition(QueryUnitAttempt taskAttempt,
                                       TaskAttemptEvent event) {
      TaskAttemptStatusUpdateEvent updateEvent =
          (TaskAttemptStatusUpdateEvent) event;

      switch (updateEvent.getStatus().getState()) {
        case TA_PENDING:
        case TA_RUNNING:
          return TaskAttemptState.TA_RUNNING;

        default:
          return taskAttempt.getState();
      }
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
      }
    }
  }

  private static class FailedTransition implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent>{
    @Override
    public void transition(QueryUnitAttempt taskAttempt, TaskAttemptEvent event) {
      TaskFatalErrorEvent errorEvent = (TaskFatalErrorEvent) event;
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(taskAttempt.getId(), TaskEventType.T_ATTEMPT_FAILED));
      taskAttempt.addDiagnosticInfo(errorEvent.errorMessage());
      LOG.error("FROM " + taskAttempt.getHost() + " >> " + errorEvent.errorMessage());
    }
  }

  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptId() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state of "
            + event.getTaskAttemptId() + ")", e);
        eventHandler.handle(new QueryEvent(TajoIdUtils.parseQueryId(getId().toString()),
            QueryEventType.INTERNAL_ERROR));
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
