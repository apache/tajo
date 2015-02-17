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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public abstract class AbstractJoinExec extends BinaryPhysicalExec {

  protected JoinNode plan;
  protected EvalNode joinQual;
  protected EvalNode joinFilter;

  private Projector projector;
  private Tuple outTuple;
  private FrameTuple frameTuple;
  private boolean filterSatisfied = false;
  private boolean projected = false;

  public AbstractJoinExec(final TaskAttemptContext context, final JoinNode plan,
                          final PhysicalExec outer, PhysicalExec inner) {
    this(context, plan, plan.getInSchema(), plan.getOutSchema(), outer, inner);
  }

  public AbstractJoinExec(final TaskAttemptContext context, final JoinNode plan,
                          final Schema inSchema, final Schema outSchema,
                          final PhysicalExec outer, PhysicalExec inner) {
    super(context, inSchema, outSchema, outer, inner);
    this.plan = plan;
    this.joinQual = plan.hasJoinQual() ? plan.getJoinQual() : null;
    this.joinFilter = plan.hasJoinFilter() ? plan.getJoinFilter() : null;

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());
  }

  public JoinNode getPlan() {
    return plan;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public void setPrecompiledJoinPredicates() {
    if (hasJoinQual()) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
    if (hasJoinFilter()) {
      joinFilter = context.getPrecompiledEval(inSchema, joinFilter);
    }
  }

  public EvalNode getJoinQual() {
    return joinQual;
  }

  public boolean hasJoinFilter() {
    return this.joinFilter != null;
  }

  public void setJoinFilter(EvalNode joinFilter) {
    this.joinFilter = joinFilter;
  }

  public EvalNode getJoinFilter() {
    return joinFilter;
  }

  public boolean evalFilter() {
    filterSatisfied = hasJoinFilter() ?
        joinFilter.eval(inSchema, frameTuple).isTrue() : true;
    return filterSatisfied;
  }

  public boolean evalQual() {
    return hasJoinQual() ?
        joinQual.eval(inSchema, frameTuple).isTrue() : true;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.plan = null;
    this.joinQual = null;
    this.joinFilter = null;
    this.projector = null;
    this.outTuple = null;
    this.frameTuple = null;
    this.filterSatisfied = this.projected = false;
  }

  protected void updateFrameTuple(Tuple left, Tuple right) {
    frameTuple.set(left, right);
    filterSatisfied = false;
    projected = false;
  }

  protected void doProject() {
    projector.eval(frameTuple, outTuple);
    projected = true;
  }

  protected Tuple projectAndReturn() {
    doProject();
    return returnOutputTuple();
  }

  protected Tuple returnOutputTuple() {
    if (!filterSatisfied) {
      throw new RuntimeException("The join filters must be satisfied before return a tuple.");
    }
    if (!projected) {
      throw new RuntimeException("The output tuple is not projected.");
    }
    return outTuple;
  }

  protected Tuple returnWithFilterIgnore() {
    if (!projected) {
      throw new RuntimeException("The output tuple is not projected.");
    }
    return outTuple;
  }
}
