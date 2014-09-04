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

import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

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

  // projection
  private final Projector projector;

  public NLJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    this.plan = plan;

    if (plan.hasJoinQual()) {
      this.joinQual = plan.getJoinQual();
    }

    // for projection
    projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    needNewOuter = true;
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());
  }

  public JoinNode getPlan() {
    return this.plan;
  }

  public Tuple next() throws IOException {
    for (;;) {
      if (needNewOuter) {
        outerTuple = leftChild.next();
        if (outerTuple == null) {
          return null;
        }
        needNewOuter = false;
      }

      innerTuple = rightChild.next();
      if (innerTuple == null) {
        needNewOuter = true;
        rightChild.rescan();
        continue;
      }

      frameTuple.set(outerTuple, innerTuple);
      if (joinQual != null) {
        if (joinQual.eval(inSchema, frameTuple).isTrue()) {
          projector.eval(frameTuple, outTuple);
          return outTuple;
        }
      } else {
        projector.eval(frameTuple, outTuple);
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
