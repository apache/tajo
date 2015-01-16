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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.querymaster.StageState;

public class StageCompletedEvent extends QueryEvent {
  private final ExecutionBlockId executionBlockId;
  private final StageState finalState;

  public StageCompletedEvent(final ExecutionBlockId executionBlockId,
                             StageState finalState) {
    super(executionBlockId.getQueryId(), QueryEventType.STAGE_COMPLETED);
    this.executionBlockId = executionBlockId;
    this.finalState = finalState;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public StageState getState() {
    return finalState;
  }
}
