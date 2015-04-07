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

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.util.Pair;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

// common join exec except HashLeftOuterJoinExec
public abstract class CommonJoinExec extends BinaryPhysicalExec {

  // from logical plan
  protected JoinNode plan;
  protected final boolean hasJoinQual;
  protected final boolean hasJoinFilter;

  protected EvalNode joinQual;    // ex) a.id = b.id
  protected EvalNode joinFilter;  // ex) a > 10

  // projection
  protected Projector projector;

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                        PhysicalExec inner) {
    this(context, plan, outer, inner, false);
  }

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                        PhysicalExec inner, boolean extractJoinFilter) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    if (plan.hasJoinQual() && extractJoinFilter) {
      Pair<EvalNode, EvalNode> extracted = extractJoinConditions(plan.getJoinQual());
      joinQual = extracted.getFirst();
      joinFilter = extracted.getSecond();
    } else {
      joinQual = plan.getJoinQual();
    }
    this.hasJoinQual = joinQual != null;
    this.hasJoinFilter = joinFilter != null;

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());
  }

  private Pair<EvalNode, EvalNode> extractJoinConditions(EvalNode joinQual) {
    List<EvalNode> joinQuals = Lists.newArrayList();
    List<EvalNode> joinFilters = Lists.newArrayList();
    for (EvalNode eachQual : AlgebraicUtil.toConjunctiveNormalFormArray(joinQual)) {
      if (EvalTreeUtil.isJoinQual(eachQual, true)) {
        joinQuals.add(eachQual);
      } else {
        joinFilters.add(eachQual);
      }
    }

    return new Pair<EvalNode, EvalNode>(
        joinQuals.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(joinQuals),
        joinFilters.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(joinFilters)
    );
  }

  @Override
  public void init(boolean leftRescan, boolean rightRescan) throws IOException {
    super.init(leftRescan, rightRescan);
    if (hasJoinQual) {
      joinQual.bind(inSchema);
    }
    if (hasJoinFilter) {
      joinFilter.bind(inSchema);
    }
  }

  @Override
  protected void compile() {
    if (hasJoinQual) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
    // compile filter?
  }

  public JoinNode getPlan() {
    return plan;
  }

  @Override
  public void close() throws IOException {
    super.close();
    plan = null;
    joinQual = null;
    joinFilter = null;
    projector = null;
  }
}
