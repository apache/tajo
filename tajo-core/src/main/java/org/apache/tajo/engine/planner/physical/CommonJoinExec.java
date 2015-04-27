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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

// common join exec except HashLeftOuterJoinExec
public abstract class CommonJoinExec extends BinaryPhysicalExec {

  // from logical plan
  protected JoinNode plan;
  protected final boolean hasJoinQual;

  protected EvalNode joinQual;

  // projection
  protected Projector projector;

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                        PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.hasJoinQual = plan.hasJoinQual();

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());
  }

  @Override
  public void init() throws IOException {
    super.init();
    if (hasJoinQual) {
      joinQual.bind(context.getEvalContext(), inSchema);
    }
  }

  @Override
  protected void compile() {
    if (hasJoinQual) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
  }

  public JoinNode getPlan() {
    return plan;
  }

  @Override
  public void close() throws IOException {
    super.close();
    plan = null;
    joinQual = null;
    projector = null;
  }
}
