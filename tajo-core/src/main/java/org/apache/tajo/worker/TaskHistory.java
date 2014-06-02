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
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;

import java.util.Collections;
import java.util.List;

import static org.apache.tajo.TajoProtos.TaskAttemptState;
import static org.apache.tajo.ipc.TajoWorkerProtocol.FetcherHistoryProto;
import static org.apache.tajo.ipc.TajoWorkerProtocol.TaskHistoryProto;

/**
 * The history class for Task processing.
 */
public class TaskHistory implements ProtoObject<TaskHistoryProto> {

  private QueryUnitAttemptId queryUnitAttemptId;
  private TaskAttemptState state;
  private float progress;
  private long startTime;
  private long finishTime;
  private CatalogProtos.TableStatsProto inputStats;
  private CatalogProtos.TableStatsProto outputStats;
  private String outputPath;
  private String workingPath;

  private int finishedFetchCount;
  private int totalFetchCount;
  private List<FetcherHistoryProto> fetcherHistories;

  public TaskHistory(QueryUnitAttemptId queryUnitAttemptId, TaskAttemptState state, float progress,
                     long startTime, long finishTime, CatalogProtos.TableStatsProto inputStats) {
    init();
    this.queryUnitAttemptId = queryUnitAttemptId;
    this.state = state;
    this.progress = progress;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.inputStats = inputStats;
  }

  public TaskHistory(TaskHistoryProto proto) {
    this.queryUnitAttemptId = new QueryUnitAttemptId(proto.getQueryUnitAttemptId());
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

    this.fetcherHistories = proto.getFetcherHistoriesList();
  }

  private void init() {
    this.fetcherHistories = Lists.newArrayList();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryUnitAttemptId, state);
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
    builder.setQueryUnitAttemptId(queryUnitAttemptId.getProto());
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

  public QueryUnitAttemptId getQueryUnitAttemptId() {
    return queryUnitAttemptId;
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
}
