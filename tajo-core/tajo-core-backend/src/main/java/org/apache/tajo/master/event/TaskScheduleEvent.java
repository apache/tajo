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

import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.storage.DataLocation;

import java.util.Arrays;
import java.util.List;

public class TaskScheduleEvent extends TaskSchedulerEvent {
  private final QueryUnitAttemptId attemptId;
  private final boolean isLeafQuery;
  private final List<DataLocation> dataLocations;
  private final String[] racks;

  public TaskScheduleEvent(final QueryUnitAttemptId attemptId,
                           final EventType eventType, boolean isLeafQuery,
                           final List<DataLocation> dataLocations,
                           final String[] racks) {
    super(eventType, attemptId.getQueryUnitId().getExecutionBlockId());
    this.attemptId = attemptId;
    this.isLeafQuery = isLeafQuery;
    this.dataLocations = dataLocations;
    this.racks = racks;
  }

  public QueryUnitAttemptId getAttemptId() {
    return this.attemptId;
  }

  public boolean isLeafQuery() {
    return this.isLeafQuery;
  }

  public List<DataLocation> getDataLocations() {
    return this.dataLocations;
  }

  public String[] getRacks() {
    return this.racks;
  }

  @Override
  public String toString() {
    return "TaskScheduleEvent{" +
        "attemptId=" + attemptId +
        ", isLeafQuery=" + isLeafQuery +
        ", hosts=" + (dataLocations == null ? null : dataLocations) +
        ", racks=" + (racks == null ? null : Arrays.asList(racks)) +
        '}';
  }
}
