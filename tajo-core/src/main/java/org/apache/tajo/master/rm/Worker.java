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

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.util.TUtil;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * It contains resource and various information for a worker.
 */
public class Worker implements EventHandler<WorkerEvent>, Comparable<Worker> {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(Worker.class);

  /** context of {@link TajoResourceManager} */
  private final TajoRMContext rmContext;

  /** last heartbeat time */
  private volatile long lastHeartbeatTime;

  private volatile int numRunningTasks;

  private volatile int numRunningQueryMaster;

  private static AtomicLongFieldUpdater HEARTBEAT_TIME_UPDATER;
  private static AtomicIntegerFieldUpdater RUNNING_TASK_UPDATER;
  private static AtomicIntegerFieldUpdater RUNNING_QM_UPDATER;

  static {
    HEARTBEAT_TIME_UPDATER = PlatformDependent.newAtomicLongFieldUpdater(Worker.class, "lastHeartbeatTime");
    if (HEARTBEAT_TIME_UPDATER == null) {
      HEARTBEAT_TIME_UPDATER = AtomicLongFieldUpdater.newUpdater(Worker.class, "lastHeartbeatTime");
      RUNNING_TASK_UPDATER = AtomicIntegerFieldUpdater.newUpdater(Worker.class, "numRunningTasks");
      RUNNING_QM_UPDATER = AtomicIntegerFieldUpdater.newUpdater(Worker.class, "numRunningQueryMaster");
    } else {
      RUNNING_TASK_UPDATER = PlatformDependent.newAtomicIntegerFieldUpdater(Worker.class, "numRunningTasks");
      RUNNING_QM_UPDATER = PlatformDependent.newAtomicIntegerFieldUpdater(Worker.class, "numRunningQueryMaster");
    }
  }

  /** Available resources on the node. */
  private final NodeResource availableResource;

  /** Total resources on the node. */
  private final NodeResource totalResourceCapability;

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

  public Worker(TajoRMContext rmContext, NodeResource totalResourceCapability, WorkerConnectionInfo connectionInfo) {
    this.rmContext = rmContext;

    this.connectionInfo = connectionInfo;
    this.lastHeartbeatTime = System.currentTimeMillis();
    this.totalResourceCapability = totalResourceCapability;
    this.availableResource = NodeResources.clone(totalResourceCapability);
  }

  public int getWorkerId() {
    return connectionInfo.getId();
  }

  public WorkerConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  public void setLastHeartbeatTime(long lastheartbeatReportTime) {
    HEARTBEAT_TIME_UPDATER.lazySet(this, lastheartbeatReportTime);
  }

  public void setNumRunningQueryMaster(int numRunningQueryMaster) {
    RUNNING_QM_UPDATER.lazySet(this, numRunningQueryMaster);
  }

  public int getNumRunningQueryMaster() {
    return numRunningQueryMaster;
  }

  public void setNumRunningTasks(int numRunningTasks) {
    RUNNING_TASK_UPDATER.lazySet(this, numRunningTasks);
  }

  public int getNumRunningTasks() {
    return numRunningTasks;
  }

  public long getLastHeartbeatTime() {
    return this.lastHeartbeatTime;
  }

  /**
   *
   * @return the current state of worker
   */
  public WorkerState getState() {
    return this.stateMachine.getCurrentState();
  }

  /**
   * Get current resources on the node.
   *
   * @return current resources on the node.
   */
  public NodeResource getAvailableResource() {
    return this.availableResource;
  }

  /**
   * Get total resources on the node.
   *
   * @return total resources on the node.
   */
  public NodeResource getTotalResourceCapability() {
    return totalResourceCapability;
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

    if (connectionInfo != null ? !connectionInfo.equals(worker.connectionInfo) : worker.connectionInfo != null)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (connectionInfo != null ? connectionInfo.hashCode() : 0);
    return result;
  }

  public static class AddNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {
    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {

      worker.rmContext.getQueryMasterWorker().add(worker.getWorkerId());
      LOG.info("Worker with " + worker.getTotalResourceCapability() + " is joined to Tajo cluster");
    }
  }

  public static class StatusUpdateTransition implements
      MultipleArcTransition<Worker, WorkerEvent, WorkerState> {

    @Override
    public WorkerState transition(Worker worker, WorkerEvent event) {

      WorkerStatusEvent statusEvent = TUtil.checkTypeAndGet(event, WorkerStatusEvent.class);
      worker.updateStatus(statusEvent);

      return WorkerState.RUNNING;
    }
  }

  private void updateStatus(WorkerStatusEvent statusEvent) {
    setLastHeartbeatTime(System.currentTimeMillis());
    setNumRunningTasks(statusEvent.getRunningTaskNum());
    setNumRunningQueryMaster(statusEvent.getRunningQMNum());
    NodeResources.update(availableResource, statusEvent.getAvailableResource());

    if(statusEvent.getTotalResource() != null) {
      NodeResources.update(totalResourceCapability, statusEvent.getTotalResource());
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

      WorkerReconnectEvent castedEvent = TUtil.checkTypeAndGet(workerEvent, WorkerReconnectEvent.class);
      Worker newWorker = castedEvent.getWorker();
      worker.rmContext.getWorkers().put(castedEvent.getWorkerId(), newWorker);
      worker.rmContext.getDispatcher().getEventHandler().handle(
          new WorkerEvent(worker.getWorkerId(), WorkerEventType.STARTED));
    }
  }

  @Override
  public void handle(WorkerEvent event) {
    LOG.debug("Processing " + event.getWorkerId() + " of type " + event.getType());
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
}
