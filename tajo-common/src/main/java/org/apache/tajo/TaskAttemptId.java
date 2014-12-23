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

package org.apache.tajo;

import com.google.common.base.Objects;

public class TaskAttemptId implements Comparable<TaskAttemptId> {
  public static final String QUA_ID_PREFIX = "ta";

  private TaskId taskId;
  private int id;

  public TaskId getTaskId() {
    return taskId;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public TaskAttemptId(TaskId taskId, int id) {
    this.taskId = taskId;
    this.id = id;
  }

  public TaskAttemptId(TajoIdProtos.TaskAttemptIdProto proto) {
    this(new TaskId(proto.getTaskId()), proto.getId());
  }

  public TajoIdProtos.TaskAttemptIdProto getProto() {
    return TajoIdProtos.TaskAttemptIdProto.newBuilder()
        .setTaskId(taskId.getProto())
        .setId(id)
        .build();
  }

  @Override
  public int compareTo(TaskAttemptId taskAttemptId) {
    int result = taskId.compareTo(taskAttemptId.taskId);
    if (result == 0) {
      return id - taskAttemptId.id;
    } else {
      return result;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if(!(obj instanceof TaskAttemptId)) {
      return false;
    }
    return compareTo((TaskAttemptId)obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(taskId, id);
  }

  @Override
  public String toString() {
    return QUA_ID_PREFIX + QueryId.SEPARATOR + toStringNoPrefix();
  }

  public String toStringNoPrefix() {
    return taskId.toStringNoPrefix() + QueryId.SEPARATOR + QueryIdFactory.ATTEMPT_ID_FORMAT.format(id);
  }
}
