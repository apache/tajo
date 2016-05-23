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

package org.apache.tajo.worker.event;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.worker.NodeResourceManager.Allocation;

import static org.apache.tajo.ResourceProtos.TaskRequestProto;

public class TaskStartEvent extends TaskManagerEvent {

  private Allocation allocation;
  private TaskRequestProto taskRequest;
  private TaskAttemptId taskAttemptId;

  public TaskStartEvent(TaskRequestProto taskRequest,
                        Allocation allocation) {
    super(EventType.TASK_START);
    this.taskRequest = taskRequest;
    this.allocation = allocation;
    this.taskAttemptId = new TaskAttemptId(taskRequest.getId());
  }

  public Allocation getAllocation() {
    return allocation;
  }

  public TaskRequestProto getTaskRequest() {
    return taskRequest;
  }

  public TaskAttemptId getTaskAttemptId() {
    return taskAttemptId;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return taskAttemptId.getTaskId().getExecutionBlockId();
  }
}
