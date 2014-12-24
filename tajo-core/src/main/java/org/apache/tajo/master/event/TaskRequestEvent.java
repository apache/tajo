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

package org.apache.tajo.master.event;

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.TaskRequestProto;
import org.apache.tajo.master.event.TaskRequestEvent.TaskRequestEventType;
import org.apache.tajo.master.container.TajoContainerId;

public class TaskRequestEvent extends AbstractEvent<TaskRequestEventType> {

  public enum TaskRequestEventType {
    TASK_REQ
  }

  private final int workerId;
  private final TajoContainerId containerId;
  private final ExecutionBlockId executionBlockId;

  private final RpcCallback<TaskRequestProto> callback;

  public TaskRequestEvent(int workerId,
                          TajoContainerId containerId,
                          ExecutionBlockId executionBlockId,
                          RpcCallback<TaskRequestProto> callback) {
    super(TaskRequestEventType.TASK_REQ);
    this.workerId = workerId;
    this.containerId = containerId;
    this.executionBlockId = executionBlockId;
    this.callback = callback;
  }

  public int getWorkerId() {
    return this.workerId;
  }

  public TajoContainerId getContainerId() {
    return this.containerId;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public RpcCallback<TajoWorkerProtocol.TaskRequestProto> getCallback() {
    return this.callback;
  }
}
