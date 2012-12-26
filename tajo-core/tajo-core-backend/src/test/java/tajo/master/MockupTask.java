/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.TaskAttemptState;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService.Interface;

public class MockupTask {
  private QueryUnitAttemptId unitId;
  private TaskAttemptState state;
  private int leftTime;
  private Interface masterProxy;

  public MockupTask(QueryUnitAttemptId unitId, Interface masterProxy,
                    int runTime) {
    this.unitId = unitId;
    this.state = TaskAttemptState.TA_PENDING;
    this.leftTime = runTime;
    this.masterProxy = masterProxy;
  }

  public QueryUnitAttemptId getId() {
    return this.unitId;
  }

  public TaskAttemptState getState() {
    return this.state;
  }

  public int getLeftTime() {
    return this.leftTime;
  }

  public void updateTime(int time) {
    this.leftTime -= time;
  }

  public void setState(TaskAttemptState state) {
    this.state = state;
  }
}
