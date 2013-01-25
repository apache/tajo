/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.planner.physical;

import tajo.TaskAttemptContext;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.JoinNode;
import tajo.storage.FrameTuple;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

public class NLJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode plan;
  private EvalNode joinQual;


  // temporal tuples and states for nested loop join
  private boolean needNewOuter;
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private EvalContext qualCtx;

  // projection
  private final EvalContext [] evalContexts;
  private final Projector projector;

  public NLJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    this.plan = plan;

    if (plan.hasJoinQual()) {
      this.joinQual = plan.getJoinQual();
      this.qualCtx = this.joinQual.newContext();
    }

    // for projection
    projector = new Projector(inSchema, outSchema, plan.getTargets());
    evalContexts = projector.renew();

    // for join
    needNewOuter = true;
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
  }

  public Tuple next() throws IOException {
    for (;;) {
      if (needNewOuter) {
        outerTuple = outerChild.next();
        if (outerTuple == null) {
          return null;
        }
        needNewOuter = false;
      }

      innerTuple = innerChild.next();
      if (innerTuple == null) {
        needNewOuter = true;
        innerChild.rescan();
        continue;
      }

      frameTuple.set(outerTuple, innerTuple);
      if (joinQual != null) {
        joinQual.eval(qualCtx, inSchema, frameTuple);
        if (joinQual.terminate(qualCtx).asBool()) {
          projector.eval(evalContexts, frameTuple);
          projector.terminate(evalContexts, outTuple);
          return outTuple;
        }
      } else {
        projector.eval(evalContexts, frameTuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    needNewOuter = true;
  }
}
