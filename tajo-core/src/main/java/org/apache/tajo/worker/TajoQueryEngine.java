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

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;

public class TajoQueryEngine {

  private final PhysicalPlanner phyPlanner;

  public TajoQueryEngine(TajoConf conf) throws IOException {
    this.phyPlanner = new PhysicalPlannerImpl(conf);
  }
  
  public PhysicalExec createPlan(TaskAttemptContext ctx, LogicalNode plan)
      throws InternalException {
    return phyPlanner.createPlan(ctx, plan);
  }
  
  public void stop() throws IOException {
  }
}
