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

package org.apache.tajo.engine.planner;

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.util.List;

public class Projector {
  private final TaskAttemptContext context;
  private final Schema inSchema;

  // for projection
  private final EvalNode[] evals;

  private final Tuple outTuple;

  public Projector(TaskAttemptContext context, Schema inSchema, Schema outSchema, List<Target> targets) {
    this.context = context;
    this.inSchema = inSchema;
    List<Target> realTargets;
    if (targets == null) {
      realTargets = PlannerUtil.schemaToTargets(outSchema);
    } else {
      realTargets = targets;
    }

    outTuple = new VTuple(realTargets.size());
    evals = new EvalNode[realTargets.size()];

    if (context.getQueryContext().getBool(SessionVars.CODEGEN)) {
      EvalNode eval;
      for (int i = 0; i < realTargets.size(); i++) {
        eval = realTargets.get(i).getEvalTree();
        evals[i] = context.getPrecompiledEval(inSchema, eval);
      }
    } else {
      for (int i = 0; i < realTargets.size(); i++) {
        evals[i] = realTargets.get(i).getEvalTree();
      }
    }
    init();
  }

  public void init() {
    for (EvalNode eval : evals) {
      eval.bind(context.getEvalContext(), inSchema);
    }
  }

  public Tuple eval(Tuple in) {
    for (int i = 0; i < evals.length; i++) {
      outTuple.put(i, evals[i].eval(in));
    }
    return outTuple;
  }
}
