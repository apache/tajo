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

package org.apache.tajo.querymaster;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.querymaster.QueryMasterTask;

public class TaskSchedulerContext {
  private QueryMasterTask.QueryMasterTaskContext masterContext;
  private boolean isLeafQuery;
  private ExecutionBlockId blockId;
  private int taskSize;
  private int estimatedTaskNum;

  public TaskSchedulerContext(QueryMasterTask.QueryMasterTaskContext masterContext, boolean isLeafQuery,
                              ExecutionBlockId blockId) {
    this.masterContext = masterContext;
    this.isLeafQuery = isLeafQuery;
    this.blockId = blockId;
  }

  public QueryMasterTask.QueryMasterTaskContext getMasterContext() {
    return masterContext;
  }

  public boolean isLeafQuery() {
    return isLeafQuery;
  }

  public ExecutionBlockId getBlockId() {
    return blockId;
  }

  public int getTaskSize() {
    return taskSize;
  }

  public int getEstimatedTaskNum() {
    return estimatedTaskNum;
  }

  public void setTaskSize(int taskSize) {
    this.taskSize = taskSize;
  }

  public void setEstimatedTaskNum(int estimatedTaskNum) {
    this.estimatedTaskNum = estimatedTaskNum;
  }
}
