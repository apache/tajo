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

package org.apache.tajo.master;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.container.TajoContainer;

import java.util.Collection;

public class LaunchTaskRunnersEvent extends TaskRunnerGroupEvent {
  private final QueryContext queryContext;
  private final String planJson;

  public LaunchTaskRunnersEvent(ExecutionBlockId executionBlockId,
                                Collection<TajoContainer> containers, QueryContext queryContext,
                                String planJson) {
    super(EventType.CONTAINER_REMOTE_LAUNCH, executionBlockId, containers);
    this.queryContext = queryContext;
    this.planJson = planJson;
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public String getPlanJson() {
    return planJson;
  }
}
