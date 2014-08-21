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
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class NLLeftOuterJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode plan;
  private EvalNode joinQual;

  // temporal tuples and states for nested loop join
  private boolean needNextRightTuple;
  private FrameTuple frameTuple;
  private Tuple leftTuple = null;
  private Tuple rightTuple = null;
  private Tuple outTuple = null;

  // projection
  private final Projector projector;

  private boolean foundAtLeastOneMatch;
  private int rightNumCols;

  public NLLeftOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftChild,
                             PhysicalExec rightChild) {
    super(context, plan.getInSchema(), plan.getOutSchema(), leftChild, rightChild);
    this.plan = plan;

    if (plan.hasJoinQual()) {
      this.joinQual = plan.getJoinQual();
    }

    // for projection
    projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    needNextRightTuple = true;
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());

    foundAtLeastOneMatch = false;
    rightNumCols = rightChild.getSchema().size();
  }

  public JoinNode getPlan() {
    return this.plan;
  }

  public Tuple next() throws IOException {
    for (;;) {
      if (needNextRightTuple) {
        leftTuple = leftChild.next();
        if (leftTuple == null) {
          return null;
        }
        needNextRightTuple = false;
        // a new tuple from the left child has initially no matches on the right operand
        foundAtLeastOneMatch = false;
      }
      rightTuple = rightChild.next();

      if (rightTuple == null) {
        // the scan of the right operand is finished with no matches found
        if(foundAtLeastOneMatch == false){
          //output a tuple with the nulls padded rightTuple
          Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
          frameTuple.set(leftTuple, nullPaddedTuple);
          projector.eval(frameTuple, outTuple);
          // we simulate we found a match, which is exactly the null padded one
          foundAtLeastOneMatch = true;
          needNextRightTuple = true;
          rightChild.rescan();
          return outTuple;
        } else {
          needNextRightTuple = true;
          rightChild.rescan();
          continue;
        }
      }

      frameTuple.set(leftTuple, rightTuple);
      ;
      if (joinQual.eval(inSchema, frameTuple).isTrue()) {
        projector.eval(frameTuple, outTuple);
        foundAtLeastOneMatch = true;
        return outTuple;
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    needNextRightTuple = true;
  }
}
