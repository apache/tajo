/*
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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.querymaster.TaskAttempt;
import org.apache.tajo.master.container.TajoContainerId;

public class TaskAttemptToSchedulerEvent extends TaskSchedulerEvent {
  private final TaskAttemptScheduleContext context;
  private final TaskAttempt taskAttempt;

  public TaskAttemptToSchedulerEvent(EventType eventType, ExecutionBlockId executionBlockId,
                                     TaskAttemptScheduleContext context, TaskAttempt taskAttempt) {
    super(eventType, executionBlockId);
    this.context = context;
    this.taskAttempt = taskAttempt;
  }

  public TaskAttempt getTaskAttempt() {
    return taskAttempt;
  }

  public TaskAttemptScheduleContext getContext() {
    return context;
  }

  public static class TaskAttemptScheduleContext {
    private TajoContainerId containerId;
    private String host;
    private RpcCallback<TajoWorkerProtocol.TaskRequestProto> callback;

    public TaskAttemptScheduleContext() {

    }

    public TaskAttemptScheduleContext(TajoContainerId containerId,
                                      String host,
                                      RpcCallback<TajoWorkerProtocol.TaskRequestProto> callback) {
      this.containerId = containerId;
      this.host = host;
      this.callback = callback;
    }

    public TajoContainerId getContainerId() {
      return containerId;
    }

    public void setContainerId(TajoContainerId containerId) {
      this.containerId = containerId;
    }

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public RpcCallback<TajoWorkerProtocol.TaskRequestProto> getCallback() {
      return callback;
    }

    public void setCallback(RpcCallback<TajoWorkerProtocol.TaskRequestProto> callback) {
      this.callback = callback;
    }
  }
}
