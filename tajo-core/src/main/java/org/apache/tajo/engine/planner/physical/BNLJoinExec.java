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
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BNLJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode plan;
  private final boolean hasJoinQual;
  private EvalNode joinQual;

  private List<Tuple> leftTupleSlots;
  private List<Tuple> rightTupleSlots;
  private Iterator<Tuple> leftIterator;
  private Iterator<Tuple> rightIterator;

  private boolean leftEnd;
  private boolean rightEnd;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple leftTuple = null;
  private Tuple outputTuple = null;
  private Tuple rightNext = null;

  private final int TUPLE_SLOT_SIZE = 10000;

  // projection
  private Projector projector;

  public BNLJoinExec(final TaskAttemptContext context, final JoinNode plan,
                     final PhysicalExec leftExec, PhysicalExec rightExec) {
    super(context, plan.getInSchema(), plan.getOutSchema(), leftExec, rightExec);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    if (joinQual != null) { // if join type is not 'cross join'
      hasJoinQual = true;
    } else {
      hasJoinQual = false;
    }
    this.leftTupleSlots = new ArrayList<Tuple>(TUPLE_SLOT_SIZE);
    this.rightTupleSlots = new ArrayList<Tuple>(TUPLE_SLOT_SIZE);
    this.leftIterator = leftTupleSlots.iterator();
    this.rightIterator = rightTupleSlots.iterator();
    this.rightEnd = false;
    this.leftEnd = false;

    // for projection
    if (!plan.hasTargets()) {
      plan.setTargets(PlannerUtil.schemaToTargets(outSchema));
    }

    projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outputTuple = new VTuple(outSchema.size());
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

  public Tuple next() throws IOException {

    if (leftTupleSlots.isEmpty()) {
      for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
        Tuple t = leftChild.next();
        if (t == null) {
          leftEnd = true;
          break;
        }
        leftTupleSlots.add(t);
      }
      leftIterator = leftTupleSlots.iterator();
      leftTuple = leftIterator.next();
    }

    if (rightTupleSlots.isEmpty()) {
      for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
        Tuple t = rightChild.next();
        if (t == null) {
          rightEnd = true;
          break;
        }
        rightTupleSlots.add(t);
      }
      rightIterator = rightTupleSlots.iterator();
    }

    if((rightNext = rightChild.next()) == null){
      rightEnd = true;
    }

    while (true) {
      if (!rightIterator.hasNext()) { // if leftIterator ended
        if (leftIterator.hasNext()) { // if rightTupleslot remains
          leftTuple = leftIterator.next();
          rightIterator = rightTupleSlots.iterator();
        } else {
          if (rightEnd) {
            rightChild.rescan();
            rightEnd = false;
            
            if (leftEnd) {
              return null;
            }
            leftTupleSlots.clear();
            for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
              Tuple t = leftChild.next();
              if (t == null) {
                leftEnd = true;
                break;
              }
              leftTupleSlots.add(t);
            }
            if (leftTupleSlots.isEmpty()) {
              return null;
            }
            leftIterator = leftTupleSlots.iterator();
            leftTuple = leftIterator.next();
            
          } else {
            leftIterator = leftTupleSlots.iterator();
            leftTuple = leftIterator.next();
          }
          
          rightTupleSlots.clear();
          if (rightNext != null) {
            rightTupleSlots.add(rightNext);
            for (int k = 1; k < TUPLE_SLOT_SIZE; k++) { // fill right
              Tuple t = rightChild.next();
              if (t == null) {
                rightEnd = true;
                break;
              }
              rightTupleSlots.add(t);
            }
          } else {
            for (int k = 0; k < TUPLE_SLOT_SIZE; k++) { // fill right
              Tuple t = rightChild.next();
              if (t == null) {
                rightEnd = true;
                break;
              }
              rightTupleSlots.add(t);
            }
          }
          
          if ((rightNext = rightChild.next()) == null) {
            rightEnd = true;
          }
          rightIterator = rightTupleSlots.iterator();
        }
      }

      frameTuple.set(leftTuple, rightIterator.next());
      if (hasJoinQual) {
        if (joinQual.eval(inSchema, frameTuple).isTrue()) {
          projector.eval(frameTuple, outputTuple);
          return outputTuple;
        }
      } else {
        projector.eval(frameTuple, outputTuple);
        return outputTuple;
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    rightEnd = false;
    rightTupleSlots.clear();
    leftTupleSlots.clear();
    rightIterator = rightTupleSlots.iterator();
    leftIterator = leftTupleSlots.iterator();
  }

  @Override
  public void close() throws IOException {
    super.close();

    rightTupleSlots.clear();
    leftTupleSlots.clear();
    rightTupleSlots = null;
    leftTupleSlots = null;
    rightIterator = null;
    leftIterator = null;
    plan = null;
    joinQual = null;
    projector = null;
  }
}
