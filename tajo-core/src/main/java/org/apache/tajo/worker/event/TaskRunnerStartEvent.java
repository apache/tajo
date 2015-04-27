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
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;

public class TaskRunnerStartEvent extends TaskRunnerEvent {

  private final QueryContext queryContext;
  private final WorkerConnectionInfo queryMaster;
  private final String containerId;
  private final String plan;
  private final PlanProto.ShuffleType shuffleType;

  public TaskRunnerStartEvent(WorkerConnectionInfo queryMaster,
                              ExecutionBlockId executionBlockId,
                              String containerId,
                              QueryContext context,
                              String plan,
                              PlanProto.ShuffleType shuffleType) {
    super(EventType.START, executionBlockId);
    this.queryMaster = queryMaster;
    this.containerId = containerId;
    this.queryContext = context;
    this.plan = plan;
    this.shuffleType = shuffleType;
  }

  public WorkerConnectionInfo getQueryMaster() {
    return queryMaster;
  }

  public String getContainerId() {
    return containerId;
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public String getPlan() {
    return plan;
  }

  public PlanProto.ShuffleType getShuffleType() {
    return shuffleType;
  }
}
