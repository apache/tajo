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
 * It contains resource and various information for a node.
 */
public class NodeStatus implements EventHandler<NodeEvent>, Comparable<NodeStatus> {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(NodeStatus.class);

  /** context of {@link TajoResourceManager} */
  private final TajoRMContext rmContext;

  /** last heartbeat time */
  private volatile long lastHeartbeatTime;

  @SuppressWarnings("unused")
  private volatile int numRunningTasks;

  @SuppressWarnings("unused")
  private volatile int numRunningQueryMaster;

  private static AtomicLongFieldUpdater HEARTBEAT_TIME_UPDATER;
  private static AtomicIntegerFieldUpdater RUNNING_TASK_UPDATER;
  private static AtomicIntegerFieldUpdater RUNNING_QM_UPDATER;

  static {
    HEARTBEAT_TIME_UPDATER = PlatformDependent.newAtomicLongFieldUpdater(NodeStatus.class, "lastHeartbeatTime");
    if (HEARTBEAT_TIME_UPDATER == null) {
      HEARTBEAT_TIME_UPDATER = AtomicLongFieldUpdater.newUpdater(NodeStatus.class, "lastHeartbeatTime");
      RUNNING_TASK_UPDATER = AtomicIntegerFieldUpdater.newUpdater(NodeStatus.class, "numRunningTasks");
      RUNNING_QM_UPDATER = AtomicIntegerFieldUpdater.newUpdater(NodeStatus.class, "numRunningQueryMaster");
    } else {
      RUNNING_TASK_UPDATER = PlatformDependent.newAtomicIntegerFieldUpdater(NodeStatus.class, "numRunningTasks");
      RUNNING_QM_UPDATER = PlatformDependent.newAtomicIntegerFieldUpdater(NodeStatus.class, "numRunningQueryMaster");
    }
  }

  /** Reserved resources for scheduler calculation. */
  private final NodeResource reservedResource;

  /** Available resources on the node. */
  private final NodeResource availableResource;

  /** Total resources on the node. */
  private final NodeResource totalResourceCapability;

  /** Node connection information */
  private WorkerConnectionInfo connectionInfo;

  private static final ReconnectNodeTransition RECONNECT_NODE_TRANSITION = new ReconnectNodeTransition();
  private static final StatusUpdateTransition STATUS_UPDATE_TRANSITION = new StatusUpdateTransition();

  private static final StateMachineFactory<NodeStatus,
      NodeState,
      NodeEventType,
      NodeEvent> stateMachineFactory
      = new StateMachineFactory<NodeStatus,
      NodeState,
      NodeEventType,
      NodeEvent>(NodeState.NEW)

      // Transition from NEW
      .addTransition(NodeState.NEW, NodeState.RUNNING,
          NodeEventType.STARTED,
          new AddNodeTransition())

      // Transition from RUNNING
      .addTransition(NodeState.RUNNING, EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          NodeEventType.STATE_UPDATE,
          STATUS_UPDATE_TRANSITION)
      .addTransition(NodeState.RUNNING, NodeState.LOST,
          NodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          NodeEventType.RECONNECTED,
          RECONNECT_NODE_TRANSITION)

      // Transitions from UNHEALTHY state
      .addTransition(NodeState.UNHEALTHY, EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          NodeEventType.STATE_UPDATE,
          STATUS_UPDATE_TRANSITION)
      .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
          NodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          NodeEventType.RECONNECTED,
          RECONNECT_NODE_TRANSITION);

  private final StateMachine<NodeState, NodeEventType, NodeEvent> stateMachine =
      stateMachineFactory.make(this, NodeState.NEW);

  public NodeStatus(TajoRMContext rmContext, NodeResource totalResourceCapability, WorkerConnectionInfo connectionInfo) {
    this.rmContext = rmContext;

    this.connectionInfo = connectionInfo;
    this.lastHeartbeatTime = System.currentTimeMillis();
    this.totalResourceCapability = totalResourceCapability;
    this.availableResource = NodeResources.clone(totalResourceCapability);
    this.reservedResource = NodeResources.clone(availableResource);
  }

  public int getWorkerId() {
    return connectionInfo.getId();
  }

  public WorkerConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  public void setLastHeartbeatTime(long lastHeartbeatTime) {
    HEARTBEAT_TIME_UPDATER.lazySet(this, lastHeartbeatTime);
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
   * @return the current state of node
   */
  public NodeState getState() {
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
   * Get current reserved resources on the node.
   *
   * @return current reserved resources on the node.
   */
  public NodeResource getReservedResource() {
    return this.reservedResource;
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
  public int compareTo(NodeStatus o) {
    if(o == null) {
      return 1;
    }
    return connectionInfo.compareTo(o.connectionInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeStatus nodeStatus = (NodeStatus) o;

    if (connectionInfo != null ? !connectionInfo.equals(nodeStatus.connectionInfo) : nodeStatus.connectionInfo != null)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (connectionInfo != null ? connectionInfo.hashCode() : 0);
    return result;
  }

  public static class AddNodeTransition implements SingleArcTransition<NodeStatus, NodeEvent> {
    @Override
    public void transition(NodeStatus nodeStatus, NodeEvent nodeEvent) {

      nodeStatus.rmContext.getQueryMasterWorker().add(nodeStatus.getWorkerId());
      LOG.info("Node with " + nodeStatus.getTotalResourceCapability() + " is joined to Tajo cluster");
    }
  }

  public static class StatusUpdateTransition implements
      MultipleArcTransition<NodeStatus, NodeEvent, NodeState> {

    @Override
    public NodeState transition(NodeStatus nodeStatus, NodeEvent event) {

      NodeStatusEvent statusEvent = TUtil.checkTypeAndGet(event, NodeStatusEvent.class);
      nodeStatus.updateStatus(statusEvent);

      return NodeState.RUNNING;
    }
  }

  private void updateStatus(NodeStatusEvent statusEvent) {
    setLastHeartbeatTime(System.currentTimeMillis());
    setNumRunningTasks(statusEvent.getRunningTaskNum());
    setNumRunningQueryMaster(statusEvent.getRunningQMNum());
    NodeResources.update(availableResource, statusEvent.getAvailableResource());
    NodeResources.update(reservedResource, availableResource);

    if(statusEvent.getTotalResource() != null) {
      NodeResources.update(totalResourceCapability, statusEvent.getTotalResource());
    }
  }

  public static class DeactivateNodeTransition implements SingleArcTransition<NodeStatus, NodeEvent> {
    private final NodeState finalState;

    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(NodeStatus nodeStatus, NodeEvent nodeEvent) {

      nodeStatus.rmContext.getNodes().remove(nodeStatus.getWorkerId());
      LOG.info("Deactivating Node " + nodeStatus.getWorkerId() + " as it is now " + finalState);
      nodeStatus.rmContext.getInactiveNodes().putIfAbsent(nodeStatus.getWorkerId(), nodeStatus);
    }
  }

  public static class ReconnectNodeTransition implements SingleArcTransition<NodeStatus, NodeEvent> {

    @Override
    public void transition(NodeStatus nodeStatus, NodeEvent nodeEvent) {

      NodeReconnectEvent castedEvent = TUtil.checkTypeAndGet(nodeEvent, NodeReconnectEvent.class);
      NodeStatus newNodeStatus = castedEvent.getNodeStatus();
      nodeStatus.rmContext.getNodes().put(castedEvent.getWorkerId(), newNodeStatus);
      nodeStatus.rmContext.getDispatcher().getEventHandler().handle(
          new NodeEvent(nodeStatus.getWorkerId(), NodeEventType.STARTED));
    }
  }

  @Override
  public void handle(NodeEvent event) {
    LOG.debug("Processing " + event.getWorkerId() + " of type " + event.getType());
    NodeState oldState = getState();
    try {
      stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.error("Can't handle this event at current state"
          + ", eventType:" + event.getType().name()
          + ", oldState:" + oldState.name()
          + ", nextState:" + getState().name()
          , e);
      LOG.error("Invalid event " + event.getType() + " on NodeStatus  " + getWorkerId());
    }
    if (oldState != getState()) {
      LOG.info(getWorkerId() + " Node Transitioned from " + oldState + " to " + getState());
    }
  }
}
