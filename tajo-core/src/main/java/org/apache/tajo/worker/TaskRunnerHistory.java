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

package org.apache.tajo.worker;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.hadoop.service.Service;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.container.TajoConverterUtils;

import java.util.Collections;
import java.util.Map;

import static org.apache.tajo.ipc.TajoWorkerProtocol.TaskHistoryProto;
import static org.apache.tajo.ipc.TajoWorkerProtocol.TaskRunnerHistoryProto;

/**
 * The history class for TaskRunner processing.
 */
public class TaskRunnerHistory implements ProtoObject<TaskRunnerHistoryProto> {

  private Service.STATE state;
  private TajoContainerId containerId;
  private long startTime;
  private long finishTime;
  private ExecutionBlockId executionBlockId;
  private Map<TaskAttemptId, TaskHistory> taskHistoryMap = null;

  public TaskRunnerHistory(TajoContainerId containerId, ExecutionBlockId executionBlockId) {
    init();
    this.containerId = containerId;
    this.executionBlockId = executionBlockId;
  }

  public TaskRunnerHistory(TaskRunnerHistoryProto proto) {
    this.state = Service.STATE.valueOf(proto.getState());
    this.containerId = TajoConverterUtils.toTajoContainerId(proto.getContainerId());
    this.startTime = proto.getStartTime();
    this.finishTime = proto.getFinishTime();
    this.executionBlockId = new ExecutionBlockId(proto.getExecutionBlockId());
    this.taskHistoryMap = Maps.newTreeMap();
    for (TaskHistoryProto taskHistoryProto : proto.getTaskHistoriesList()) {
      TaskHistory taskHistory = new TaskHistory(taskHistoryProto);
      taskHistoryMap.put(taskHistory.getTaskAttemptId(), taskHistory);
    }
  }

  private void init() {
    this.taskHistoryMap = Maps.newHashMap();
  }

  public int size() {
    return this.taskHistoryMap.size();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(containerId, executionBlockId, state, startTime,
        finishTime, taskHistoryMap.size());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TaskRunnerHistory) {
      TaskRunnerHistory other = (TaskRunnerHistory) o;
      return getProto().equals(other.getProto());
    }
    return false;
  }

  @Override
  public TaskRunnerHistoryProto getProto() {
    TaskRunnerHistoryProto.Builder builder = TaskRunnerHistoryProto.newBuilder();
    builder.setContainerId(containerId.toString());
    builder.setState(state.toString());
    builder.setExecutionBlockId(executionBlockId.getProto());
    builder.setStartTime(startTime);
    builder.setFinishTime(finishTime);
    for (TaskHistory taskHistory : taskHistoryMap.values()){
      builder.addTaskHistories(taskHistory.getProto());
    }
    return builder.build();
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public Service.STATE getState() {
    return state;
  }

  public void setState(Service.STATE state) {
    this.state = state;
  }

  public TajoContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(TajoContainerId containerId) {
    this.containerId = containerId;
  }

  public TaskHistory getTaskHistory(TaskAttemptId taskAttemptId) {
    return taskHistoryMap.get(taskAttemptId);
  }

  public Map<TaskAttemptId, TaskHistory> getTaskHistoryMap() {
    return Collections.unmodifiableMap(taskHistoryMap);
  }

  public void addTaskHistory(TaskAttemptId taskAttemptId, TaskHistory taskHistory) {
    taskHistoryMap.put(taskAttemptId, taskHistory);
  }
}
