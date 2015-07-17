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

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.resource.NodeResource;

/**
 * {@link TajoResourceTracker} produces this event, and its destination is
 * {@link NodeStatus.StatusUpdateTransition} of {@link NodeStatus}.
 */
public class NodeStatusEvent extends NodeEvent {
  private final int runningTaskNum;
  private final int runningQMNum;
  private final NodeResource available;
  private final NodeResource total;

  public NodeStatusEvent(int workerId, int runningTaskNum, int runningQMNum,
                         NodeResource available, @Nullable NodeResource total) {
    super(workerId, NodeEventType.STATE_UPDATE);
    this.runningTaskNum = runningTaskNum;
    this.runningQMNum = runningQMNum;
    this.available = available;
    this.total = total;
  }

  public int getRunningTaskNum() {
    return runningTaskNum;
  }

  public int getRunningQMNum() {
    return runningQMNum;
  }

  public NodeResource getAvailableResource() {
    return available;
  }

  public NodeResource getTotalResource() {
    return total;
  }
}
