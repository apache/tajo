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

public class TaskId implements Comparable<TaskId> {
  public static final String QU_ID_PREFIX = "t";

  private ExecutionBlockId executionBlockId;
  private int id;

  public TaskId(ExecutionBlockId executionBlockId, int id) {
    this.executionBlockId = executionBlockId;
    this.id = id;
  }

  public TaskId(TajoIdProtos.TaskIdProto proto) {
    this(new ExecutionBlockId(proto.getExecutionBlockId()), proto.getId());
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public int getId() {
    return id;
  }

  public TajoIdProtos.TaskIdProto getProto() {
    return TajoIdProtos.TaskIdProto.newBuilder()
        .setExecutionBlockId(executionBlockId.getProto())
        .setId(id)
        .build();
  }

  @Override
  public int compareTo(TaskId taskId) {
    int result = executionBlockId.compareTo(taskId.executionBlockId);
    if (result == 0) {
      return id - taskId.id;
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
    if (!(obj instanceof TaskId)) {
      return false;
    }
    return compareTo((TaskId) obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(executionBlockId, id);
  }

  @Override
  public String toString() {
    return QU_ID_PREFIX + QueryId.SEPARATOR + toStringNoPrefix();
  }

  public String toStringNoPrefix() {
    return executionBlockId.toStringNoPrefix() + QueryId.SEPARATOR + QueryIdFactory.QU_ID_FORMAT.format(id);
  }
}
