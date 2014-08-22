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

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MergeFullOuterJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private EvalNode joinQual;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple leftTuple = null;
  private Tuple rightTuple = null;
  private Tuple outTuple = null;
  private Tuple leftNext = null;

  private List<Tuple> leftTupleSlots;
  private List<Tuple> rightTupleSlots;

  private JoinTupleComparator joincomparator = null;
  private TupleComparator[] tupleComparator = null;

  private final static int INITIAL_TUPLE_SLOT = 10000;

  private boolean end = false;

  // projection
  private Projector projector;

  private int rightNumCols;
  private int leftNumCols;
  private int posRightTupleSlots = -1;
  private int posLeftTupleSlots = -1;
  boolean endInPopulationStage = false;
  private boolean initRightDone = false;

  public MergeFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftChild,
                                PhysicalExec rightChild, SortSpec[] leftSortKey, SortSpec[] rightSortKey) {
    super(context, plan.getInSchema(), plan.getOutSchema(), leftChild, rightChild);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");
    this.joinNode = plan;
    this.joinQual = plan.getJoinQual();

    this.leftTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.rightTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = leftSortKey;
    sortSpecs[1] = rightSortKey;

    this.joincomparator = new JoinTupleComparator(leftChild.getSchema(),
        rightChild.getSchema(), sortSpecs);
    this.tupleComparator = PlannerUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), leftChild.getSchema(), rightChild.getSchema());

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());

    leftNumCols = leftChild.getSchema().size();
    rightNumCols = rightChild.getSchema().size();
  }

  @Override
  protected void compile() {
    joinQual = context.getPrecompiledEval(inSchema, joinQual);
  }

  public JoinNode getPlan(){
    return this.joinNode;
  }

  public Tuple next() throws IOException {
    Tuple previous;

    for (;;) {
      boolean newRound = false;
      if((posRightTupleSlots == -1) && (posLeftTupleSlots == -1)) {
        newRound = true;
      }
      if ((posRightTupleSlots == rightTupleSlots.size()) && (posLeftTupleSlots == leftTupleSlots.size())) {
        newRound = true;
      }

      if(newRound == true){

        if (end) {

          ////////////////////////////////////////////////////////////////////////
          // FINALIZING STAGE
          ////////////////////////////////////////////////////////////////////////
          // the finalizing stage, where remaining tuples on the right are
          // transformed into left-padded results while tuples on the left
          // are transformed into right-padded results

          // before exit, a left-padded tuple should be built for all remaining
          // right side and a right-padded tuple should be built for all remaining
          // left side

          if (initRightDone == false) {
            // maybe the left operand was empty => the right one didn't have the chance to initialize
            rightTuple = rightChild.next();
            initRightDone = true;
          }

          if((leftTuple == null) && (rightTuple == null)) {
            return null;
          }

          if((leftTuple == null) && (rightTuple != null)){
            // output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, rightTuple);
            projector.eval(frameTuple, outTuple);
            // we simulate we found a match, which is exactly the null padded one
            rightTuple = rightChild.next();
            return outTuple;
          }

          if((leftTuple != null) && (rightTuple == null)){
            // output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
            frameTuple.set(leftTuple, nullPaddedTuple);
            projector.eval(frameTuple, outTuple);
            // we simulate we found a match, which is exactly the null padded one
            leftTuple = leftChild.next();
            return outTuple;
          }
        } // if end

        ////////////////////////////////////////////////////////////////////////
        // INITIALIZING STAGE
        ////////////////////////////////////////////////////////////////////////
        // initializing stage, reading the first tuple on each side
        if (leftTuple == null) {
          leftTuple = leftChild.next();
          if( leftTuple == null){
            end = true;
            continue;
          }
        }
        if (rightTuple == null) {
          rightTuple = rightChild.next();
          initRightDone = true;
          if (rightTuple == null) {
            end = true;
            continue;
          }
        }

        // reset tuple slots for a new round
        leftTupleSlots.clear();
        rightTupleSlots.clear();
        posRightTupleSlots = -1;
        posLeftTupleSlots = -1;

        ////////////////////////////////////////////////////////////////////////
        // Comparison and Move Forward Stage
        ////////////////////////////////////////////////////////////////////////
        // advance alternatively on each side until a match is found
        int cmp;
        while (!end && ((cmp = joincomparator.compare(leftTuple, rightTuple)) != 0)) {

          if (cmp > 0) {

            //before getting a new tuple from the right,  a leftnullpadded tuple should be built
            //output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, rightTuple);
            projector.eval(frameTuple, outTuple);
            // BEFORE RETURN, MOVE FORWARD
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              end = true;
            }

            return outTuple;

          } else if (cmp < 0) {
            // before getting a new tuple from the left,  a rightnullpadded tuple should be built
            // output a tuple with the nulls padded rightTuple
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
            frameTuple.set(leftTuple, nullPaddedTuple);
            projector.eval(frameTuple, outTuple);
            // we simulate we found a match, which is exactly the null padded one
            // BEFORE RETURN, MOVE FORWARD
            leftTuple = leftChild.next();
            if(leftTuple == null) {
              end = true;
            }

            return outTuple;

          } // if (cmp < 0)
        } //while


        ////////////////////////////////////////////////////////////////////////
        // SLOTS POPULATION STAGE
        ////////////////////////////////////////////////////////////////////////
        // once a match is found, retain all tuples with this key in tuple slots
        // on each side
        if(!end) {
          endInPopulationStage = false;

          boolean endLeft = false;
          boolean endRight = false;

          previous = new VTuple(leftTuple);
          do {
            leftTupleSlots.add(new VTuple(leftTuple));
            leftTuple = leftChild.next();
            if(leftTuple == null) {
              endLeft = true;
            }


          } while ((endLeft != true) && (tupleComparator[0].compare(previous, leftTuple) == 0));
          posLeftTupleSlots = 0;


          previous = new VTuple(rightTuple);
          do {
            rightTupleSlots.add(new VTuple(rightTuple));
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              endRight = true;
            }

          } while ((endRight != true) && (tupleComparator[1].compare(previous, rightTuple) == 0) );
          posRightTupleSlots = 0;

          if ((endLeft == true) || (endRight == true)) {
            end = true;
            endInPopulationStage = true;
          }

        } // if end false
      } // if newRound


      ////////////////////////////////////////////////////////////////////////
      // RESULTS STAGE
      ////////////////////////////////////////////////////////////////////////
      // now output result matching tuples from the slots
      // if either we haven't reached end on neither side, or we did reach end
      // on one(or both) sides but that happened in the slots population step
      // (i.e. refers to next round)
      if(!end || (end && endInPopulationStage)){
        if(posLeftTupleSlots == 0){
          leftNext = new VTuple (leftTupleSlots.get(posLeftTupleSlots));
          posLeftTupleSlots = posLeftTupleSlots + 1;
        }

        if(posRightTupleSlots <= (rightTupleSlots.size() -1)) {
          Tuple aTuple = new VTuple(rightTupleSlots.get(posRightTupleSlots));
          posRightTupleSlots = posRightTupleSlots + 1;
          frameTuple.set(leftNext, aTuple);
          joinQual.eval(inSchema, frameTuple);
          projector.eval(frameTuple, outTuple);
          return outTuple;
        } else {
          // right (inner) slots reached end and should be rewind if there are still tuples in the outer slots
          if(posLeftTupleSlots <= (leftTupleSlots.size()-1)) {
            //rewind the right slots position
            posRightTupleSlots = 0;
            Tuple aTuple = new VTuple(rightTupleSlots.get(posRightTupleSlots));
            posRightTupleSlots = posRightTupleSlots + 1;
            leftNext = new VTuple (leftTupleSlots.get(posLeftTupleSlots));
            posLeftTupleSlots = posLeftTupleSlots + 1;

            frameTuple.set(leftNext, aTuple);
            joinQual.eval(inSchema, frameTuple);
            projector.eval(frameTuple, outTuple);
            return outTuple;
          }
        }
      } // the second if end false
    } // for
  }


  @Override
  public void rescan() throws IOException {
    super.rescan();
    leftTupleSlots.clear();
    rightTupleSlots.clear();
    posRightTupleSlots = -1;
    posLeftTupleSlots = -1;
  }

  @Override
  public void close() throws IOException {
    super.close();
    leftTupleSlots.clear();
    rightTupleSlots.clear();
    leftTupleSlots = null;
    rightTupleSlots = null;
    joinNode = null;
    joinQual = null;
    projector = null;
  }
}
