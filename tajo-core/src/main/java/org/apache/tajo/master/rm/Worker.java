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

package org.apache.tajo.master.rm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;

import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * It contains resource and various information for a worker.
 */
public class Worker implements EventHandler<WorkerEvent>, Comparable<Worker> {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(Worker.class);

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  /** context of {@link org.apache.tajo.master.rm.TajoWorkerResourceManager} */
  private final TajoRMContext rmContext;

  /** last heartbeat time */
  private long lastHeartbeatTime;

  /** Resource capability */
  private final WorkerResource resource;

  /** Worker connection information */
  private WorkerConnectionInfo connectionInfo;

  private static final ReconnectNodeTransition RECONNECT_NODE_TRANSITION = new ReconnectNodeTransition();
  private static final StatusUpdateTransition STATUS_UPDATE_TRANSITION = new StatusUpdateTransition();

  private static final StateMachineFactory<Worker,
      WorkerState,
      WorkerEventType,
      WorkerEvent> stateMachineFactory
      = new StateMachineFactory<Worker,
      WorkerState,
      WorkerEventType,
      WorkerEvent>(WorkerState.NEW)

      // Transition from NEW
      .addTransition(WorkerState.NEW, WorkerState.RUNNING,
          WorkerEventType.STARTED,
          new AddNodeTransition())

      // Transition from RUNNING
      .addTransition(WorkerState.RUNNING, EnumSet.of(WorkerState.RUNNING, WorkerState.UNHEALTHY),
          WorkerEventType.STATE_UPDATE,
          STATUS_UPDATE_TRANSITION)
      .addTransition(WorkerState.RUNNING, WorkerState.LOST,
          WorkerEventType.EXPIRE,
          new DeactivateNodeTransition(WorkerState.LOST))
      .addTransition(WorkerState.RUNNING, WorkerState.RUNNING,
          WorkerEventType.RECONNECTED,
          RECONNECT_NODE_TRANSITION)

      // Transitions from UNHEALTHY state
      .addTransition(WorkerState.UNHEALTHY, EnumSet.of(WorkerState.RUNNING, WorkerState.UNHEALTHY),
          WorkerEventType.STATE_UPDATE,
          STATUS_UPDATE_TRANSITION)
      .addTransition(WorkerState.UNHEALTHY, WorkerState.LOST,
          WorkerEventType.EXPIRE,
          new DeactivateNodeTransition(WorkerState.LOST))
      .addTransition(WorkerState.UNHEALTHY, WorkerState.UNHEALTHY,
          WorkerEventType.RECONNECTED,
          RECONNECT_NODE_TRANSITION);

  private final StateMachine<WorkerState, WorkerEventType, WorkerEvent> stateMachine =
      stateMachineFactory.make(this, WorkerState.NEW);

  public Worker(TajoRMContext rmContext, WorkerResource resource, WorkerConnectionInfo connectionInfo) {
    this.rmContext = rmContext;

    this.connectionInfo = connectionInfo;
    this.lastHeartbeatTime = System.currentTimeMillis();
    this.resource = resource;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  public int getWorkerId() {
    return connectionInfo.getId();
  }

  public WorkerConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  public void setLastHeartbeatTime(long lastheartbeatReportTime) {
    this.writeLock.lock();

    try {
      this.lastHeartbeatTime = lastheartbeatReportTime;
    } finally {
      this.writeLock.unlock();
    }
  }

  public long getLastHeartbeatTime() {
    this.readLock.lock();

    try {
      return this.lastHeartbeatTime;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   *
   * @return the current state of worker
   */
  public WorkerState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   *
   * @return the current resource capability of worker
   */
  public WorkerResource getResource() {
    return this.resource;
  }

  @Override
  public int compareTo(Worker o) {
    if(o == null) {
      return 1;
    }
    return connectionInfo.compareTo(o.connectionInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Worker worker = (Worker) o;

    if (lastHeartbeatTime != worker.lastHeartbeatTime) return false;
    if (connectionInfo != null ? !connectionInfo.equals(worker.connectionInfo) : worker.connectionInfo != null)
      return false;
    if (readLock != null ? !readLock.equals(worker.readLock) : worker.readLock != null) return false;
    if (resource != null ? !resource.equals(worker.resource) : worker.resource != null) return false;
    if (rmContext != null ? !rmContext.equals(worker.rmContext) : worker.rmContext != null) return false;
    if (stateMachine != null ? !stateMachine.equals(worker.stateMachine) : worker.stateMachine != null) return false;
    if (writeLock != null ? !writeLock.equals(worker.writeLock) : worker.writeLock != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = readLock != null ? readLock.hashCode() : 0;
    result = 31 * result + (writeLock != null ? writeLock.hashCode() : 0);
    result = 31 * result + (rmContext != null ? rmContext.hashCode() : 0);
    result = 31 * result + (int) (lastHeartbeatTime ^ (lastHeartbeatTime >>> 32));
    result = 31 * result + (resource != null ? resource.hashCode() : 0);
    result = 31 * result + (connectionInfo != null ? connectionInfo.hashCode() : 0);
    result = 31 * result + (stateMachine != null ? stateMachine.hashCode() : 0);
    return result;
  }

  public static class AddNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {
    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {

      worker.rmContext.getQueryMasterWorker().add(worker.getWorkerId());
      LOG.info("Worker with " + worker.getResource() + " is joined to Tajo cluster");
    }
  }

  public static class StatusUpdateTransition implements
      MultipleArcTransition<Worker, WorkerEvent, WorkerState> {

    @Override
    public WorkerState transition(Worker worker, WorkerEvent event) {
      if (!(event instanceof WorkerStatusEvent)) {
        throw new IllegalArgumentException("event should be a WorkerStatusEvent type.");
      }
      WorkerStatusEvent statusEvent = (WorkerStatusEvent) event;
      worker.updateStatus(statusEvent);

      return WorkerState.RUNNING;
    }
  }

  private void updateStatus(WorkerStatusEvent statusEvent) {
    this.writeLock.lock();

    try {
      lastHeartbeatTime = System.currentTimeMillis();
      resource.setNumRunningTasks(statusEvent.getRunningTaskNum());
      resource.setMaxHeap(statusEvent.maxHeap());
      resource.setFreeHeap(statusEvent.getFreeHeap());
      resource.setTotalHeap(statusEvent.getTotalHeap());
    } finally {
      this.writeLock.unlock();
    }
  }

  public static class DeactivateNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {
    private final WorkerState finalState;

    public DeactivateNodeTransition(WorkerState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {

      worker.rmContext.getWorkers().remove(worker.getWorkerId());
      LOG.info("Deactivating Node " + worker.getWorkerId() + " as it is now " + finalState);
      worker.rmContext.getInactiveWorkers().putIfAbsent(worker.getWorkerId(), worker);
    }
  }

  public static class ReconnectNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {

    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {
      if (!(workerEvent instanceof WorkerReconnectEvent)) {
        throw new IllegalArgumentException("workerEvent should be a WorkerReconnectEvent type.");
      }
      WorkerReconnectEvent castedEvent = (WorkerReconnectEvent) workerEvent;

      Worker newWorker = castedEvent.getWorker();
      worker.rmContext.getWorkers().put(castedEvent.getWorkerId(), newWorker);
      worker.rmContext.getDispatcher().getEventHandler().handle(
          new WorkerEvent(worker.getWorkerId(), WorkerEventType.STARTED));
    }
  }

  @Override
  public void handle(WorkerEvent event) {
    LOG.debug("Processing " + event.getWorkerId() + " of type " + event.getType());
    try {
      writeLock.lock();
      WorkerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", eventType:" + event.getType().name()
            + ", oldState:" + oldState.name()
            + ", nextState:" + getState().name()
            , e);
        LOG.error("Invalid event " + event.getType() + " on Worker  " + getWorkerId());
      }
      if (oldState != getState()) {
        LOG.info(getWorkerId() + " Node Transitioned from " + oldState + " to " + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
