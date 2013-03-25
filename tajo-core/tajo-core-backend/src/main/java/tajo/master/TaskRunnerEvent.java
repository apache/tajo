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

package tajo.master;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import tajo.SubQueryId;
import tajo.master.TaskRunnerEvent.EventType;

public abstract class TaskRunnerEvent extends AbstractEvent<EventType> {
  public enum EventType {
    CONTAINER_REMOTE_LAUNCH,
    CONTAINER_REMOTE_CLEANUP
  }

  protected final SubQueryId subQueryId;
  protected final Container container;
  protected final String containerMgrAddress;
  public TaskRunnerEvent(EventType eventType,
                         SubQueryId subQueryId,
                         Container container) {
    super(eventType);
    this.subQueryId = subQueryId;
    this.container = container;
    NodeId nodeId = container.getNodeId();
    containerMgrAddress = nodeId.getHost() + ":" + nodeId.getPort();
  }

  public ContainerId getContainerId() {
    return container.getId();
  }

  public Container getContainer() {
    return container;
  }

  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

  public ContainerToken getContainerToken() {
    return container.getContainerToken();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + container.getId().hashCode();
    result = prime * result
        + containerMgrAddress.hashCode();
    result = prime * result
        + container.getContainerToken().hashCode();
    result = prime * result
        + ((subQueryId == null) ? 0 : subQueryId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TaskRunnerEvent other = (TaskRunnerEvent) obj;
    if (!container.getId().equals(other.getContainerId()))
      return false;
    if (!containerMgrAddress.equals(other.containerMgrAddress))
      return false;
    if (!container.getContainerToken().equals(other.getContainerToken()))
      return false;
    if (!subQueryId.equals(other.subQueryId))
      return false;
    return true;
  }
}
