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
import com.google.common.collect.Lists;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.history.History;

import java.util.Collections;
import java.util.List;

import static org.apache.tajo.TajoProtos.TaskAttemptState;
import static org.apache.tajo.ResourceProtos.FetcherHistoryProto;
import static org.apache.tajo.ResourceProtos.TaskHistoryProto;

/**
 * The history class for Task processing.
 */
public class TaskHistory implements ProtoObject<TaskHistoryProto>, History {

  private TaskAttemptId taskAttemptId;
  private TaskAttemptState state;
  private float progress;
  private long startTime;
  private long finishTime;
  private CatalogProtos.TableStatsProto inputStats;
  private CatalogProtos.TableStatsProto outputStats;
  private String outputPath;
  private String workingPath;
  private String physicalPlan;

  private int finishedFetchCount;
  private int totalFetchCount;
  private List<FetcherHistoryProto> fetcherHistories;

  public TaskHistory(TaskAttemptId taskAttemptId, TaskAttemptState state, float progress,
                     long startTime, long finishTime, CatalogProtos.TableStatsProto inputStats) {
    init();
    this.taskAttemptId = taskAttemptId;
    this.state = state;
    this.progress = progress;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.inputStats = inputStats;
  }

  public TaskHistory(TaskHistoryProto proto) {
    this.taskAttemptId = new TaskAttemptId(proto.getTaskAttemptId());
    this.state = proto.getState();
    this.progress = proto.getProgress();
    this.startTime = proto.getStartTime();
    this.finishTime = proto.getFinishTime();
    this.inputStats = proto.getInputStats();

    if (proto.hasOutputStats()) {
      this.outputStats = proto.getOutputStats();
    }

    if (proto.hasOutputPath()) {
      this.outputPath = proto.getOutputPath();
    }

    if (proto.hasWorkingPath()) {
      this.workingPath = proto.getWorkingPath();
    }

    if (proto.hasFinishedFetchCount()) {
      this.finishedFetchCount = proto.getFinishedFetchCount();
    }

    if (proto.hasTotalFetchCount()) {
      this.totalFetchCount = proto.getTotalFetchCount();
    }

    if(proto.hasPhysicalPlan()) {
      this.physicalPlan = proto.getPhysicalPlan();
    }

    this.fetcherHistories = proto.getFetcherHistoriesList();
  }

  private void init() {
    this.fetcherHistories = Lists.newArrayList();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(taskAttemptId, state);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TaskHistory) {
      TaskHistory other = (TaskHistory) o;
      return getProto().equals(other.getProto());
    }
    return false;
  }

  @Override
  public TaskHistoryProto getProto() {
    TaskHistoryProto.Builder builder = TaskHistoryProto.newBuilder();
    builder.setTaskAttemptId(taskAttemptId.getProto());
    builder.setState(state);
    builder.setProgress(progress);
    builder.setStartTime(startTime);
    builder.setFinishTime(finishTime);
    builder.setInputStats(inputStats);

    if (outputStats != null) {
      builder.setOutputStats(outputStats);
    }

    if (workingPath != null) {
      builder.setWorkingPath(workingPath);
    }

    if (totalFetchCount > 0) {
      builder.setTotalFetchCount(totalFetchCount);
      builder.setFinishedFetchCount(finishedFetchCount);
    }

    if(physicalPlan != null) {
      builder.setPhysicalPlan(physicalPlan);
    }

    builder.addAllFetcherHistories(fetcherHistories);
    return builder.build();
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public List<FetcherHistoryProto> getFetcherHistories() {
    return Collections.unmodifiableList(fetcherHistories);
  }

  public boolean hasFetcherHistories(){
    return totalFetchCount > 0;
  }

  public void addFetcherHistory(FetcherHistoryProto fetcherHistory) {
    fetcherHistories.add(fetcherHistory);
  }

  public TaskAttemptId getTaskAttemptId() {
    return taskAttemptId;
  }

  public TaskAttemptState getState() {
    return state;
  }

  public float getProgress() {
    return progress;
  }

  public CatalogProtos.TableStatsProto getInputStats() {
    return inputStats;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public String getWorkingPath() {
    return workingPath;
  }

  public void setWorkingPath(String workingPath) {
    this.workingPath = workingPath;
  }

  public Integer getFinishedFetchCount() {
    return finishedFetchCount;
  }

  public void setFinishedFetchCount(int finishedFetchCount) {
    this.finishedFetchCount = finishedFetchCount;
  }

  public Integer getTotalFetchCount() {
    return totalFetchCount;
  }

  public void setTotalFetchCount(int totalFetchCount) {
    this.totalFetchCount = totalFetchCount;
  }

  public CatalogProtos.TableStatsProto getOutputStats() {
    return outputStats;
  }

  public void setOutputStats(CatalogProtos.TableStatsProto outputStats) {
    this.outputStats = outputStats;
  }

  public String getPhysicalPlan() {
    return physicalPlan;
  }

  public void setPhysicalPlan(String physicalPlan) {
    this.physicalPlan = physicalPlan;
  }

  @Override
  public HistoryType getHistoryType() {
    return HistoryType.TASK;
  }
}
