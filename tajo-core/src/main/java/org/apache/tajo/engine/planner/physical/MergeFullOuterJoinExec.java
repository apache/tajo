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
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;


public class MergeFullOuterJoinExec extends CommonJoinExec {

  // temporal tuples and states for nested loop join
  private Tuple leftTuple = null;
  private Tuple rightTuple = null;
  private Tuple leftNext = null;
  private Tuple prevLeftTuple = null;
  private Tuple prevRightTuple = null;

  private Tuple preservedTuple = null;

  private TupleList leftTupleSlots;
  private TupleList rightTupleSlots;

  private JoinTupleComparator joincomparator = null;
  private TupleComparator[] tupleComparator = null;

  private boolean end = false;

  private int rightNumCols;
  private int leftNumCols;
  private int posRightTupleSlots = -1;
  private int posLeftTupleSlots = -1;
  boolean endInPopulationStage = false;
  private boolean initRightDone = false;

  private final Tuple leftNullTuple;
  private final Tuple rightNullTuple;

  public MergeFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftChild,
                                PhysicalExec rightChild, SortSpec[] leftSortKey, SortSpec[] rightSortKey) {
    super(context, plan, leftChild, rightChild);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");

    final int INITIAL_TUPLE_SLOT = context.getQueryContext().getInt(SessionVars.JOIN_HASH_TABLE_SIZE);
    this.leftTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);
    this.rightTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);

    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = leftSortKey;
    sortSpecs[1] = rightSortKey;

    this.joincomparator = new JoinTupleComparator(leftChild.getSchema(),
        rightChild.getSchema(), sortSpecs);
    this.tupleComparator = PhysicalPlanUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), leftChild.getSchema(), rightChild.getSchema());

    leftNumCols = leftChild.getSchema().size();
    rightNumCols = rightChild.getSchema().size();

    prevLeftTuple = new VTuple(leftChild.getSchema().size());
    prevRightTuple = new VTuple(rightChild.getSchema().size());

    leftNullTuple = NullTuple.create(leftNumCols);
    rightNullTuple = NullTuple.create(rightNumCols);
  }

  public Tuple next() throws IOException {
    Tuple outTuple;

    while (!context.isStopped()) {
      if (preservedTuple != null) {
        outTuple = preservedTuple;
        preservedTuple = null;
        return outTuple;
      }

      boolean newRound = false;
      if((posRightTupleSlots == -1) && (posLeftTupleSlots == -1)) {
        newRound = true;
      }
      if ((posRightTupleSlots == rightTupleSlots.size()) && (posLeftTupleSlots == leftTupleSlots.size())) {
        newRound = true;
      }

      if(newRound){

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

          if (!initRightDone) {
            // maybe the left operand was empty => the right one didn't have the chance to initialize
            rightTuple = rightChild.next();
            initRightDone = true;
          }

          if((leftTuple == null) && (rightTuple == null)) {
            return null;
          }

          if((leftTuple == null) && (rightTuple != null)){
            // output a tuple with the nulls padded leftTuple
            frameTuple.set(leftNullTuple, rightTuple);
            outTuple = projector.eval(frameTuple);
            // we simulate we found a match, which is exactly the null padded one
            rightTuple = rightChild.next();
            return outTuple;
          }

          if((leftTuple != null) && (rightTuple == null)){
            // output a tuple with the nulls padded leftTuple
            frameTuple.set(leftTuple, rightNullTuple);
            outTuple = projector.eval(frameTuple);
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
        if (!end && ((cmp = joincomparator.compare(leftTuple, rightTuple)) != 0)) {

          if (cmp > 0) {

            //before getting a new tuple from the right,  a leftnullpadded tuple should be built
            //output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = leftNullTuple;
            frameTuple.set(nullPaddedTuple, rightTuple);
            outTuple = projector.eval(frameTuple);
            // BEFORE RETURN, MOVE FORWARD
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              end = true;
            }

            return outTuple;

          } else if (cmp < 0) {
            // before getting a new tuple from the left,  a rightnullpadded tuple should be built
            // output a tuple with the nulls padded rightTuple
            Tuple nullPaddedTuple = rightNullTuple;
            frameTuple.set(leftTuple, nullPaddedTuple);
            outTuple = projector.eval(frameTuple);
            // we simulate we found a match, which is exactly the null padded one
            // BEFORE RETURN, MOVE FORWARD
            leftTuple = leftChild.next();
            if(leftTuple == null) {
              end = true;
            }

            return outTuple;

          }
        }

        // Check null values
        Tuple leftKey = leftKeyExtractor.project(leftTuple);
        boolean containNull = false;
        for (int i = 0; i < leftKey.size(); i++) {
          if (leftKey.isBlankOrNull(i)) {
            containNull = true;
            break;
          }
        }

        if (containNull) {
          frameTuple.set(leftTuple, rightNullTuple);
          outTuple = projector.eval(frameTuple);
          frameTuple.set(leftNullTuple, rightTuple);
          preservedTuple = new VTuple(projector.eval(frameTuple));
          leftTuple = leftChild.next();
          rightTuple = rightChild.next();

          if (leftTuple == null || rightTuple == null) {
            end = true;
          }
          return outTuple;
        }

        ////////////////////////////////////////////////////////////////////////
        // SLOTS POPULATION STAGE
        ////////////////////////////////////////////////////////////////////////
        // once a match is found, retain all tuples with this key in tuple slots
        // on each side
        if(!end) {
          endInPopulationStage = false;

          boolean endLeft = false;
          boolean endRight = false;

          prevLeftTuple.put(leftTuple.getValues());
          do {
            leftTupleSlots.add(leftTuple);
            leftTuple = leftChild.next();
            if(leftTuple == null) {
              endLeft = true;
            }

          } while ((!endLeft) && (tupleComparator[0].compare(prevLeftTuple, leftTuple) == 0));
          posLeftTupleSlots = 0;

          prevRightTuple.put(rightTuple.getValues());
          do {
            rightTupleSlots.add(rightTuple);
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              endRight = true;
            }

          } while ((!endRight) && (tupleComparator[1].compare(prevRightTuple, rightTuple) == 0) );
          posRightTupleSlots = 0;

          if ((endLeft) || (endRight)) {
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
          leftNext = leftTupleSlots.get(posLeftTupleSlots);
          posLeftTupleSlots++;
        }

        if(posRightTupleSlots <= (rightTupleSlots.size() -1)) {
          Tuple aTuple = rightTupleSlots.get(posRightTupleSlots);
          posRightTupleSlots++;
          frameTuple.set(leftNext, aTuple);
          joinQual.eval(frameTuple);
          return projector.eval(frameTuple);
        } else {
          // right (inner) slots reached end and should be rewind if there are still tuples in the outer slots
          if(posLeftTupleSlots <= (leftTupleSlots.size()-1)) {
            //rewind the right slots position
            posRightTupleSlots = 0;
            Tuple aTuple = rightTupleSlots.get(posRightTupleSlots);
            posRightTupleSlots = posRightTupleSlots + 1;
            leftNext = leftTupleSlots.get(posLeftTupleSlots);
            posLeftTupleSlots = posLeftTupleSlots + 1;

            frameTuple.set(leftNext, aTuple);
            joinQual.eval(frameTuple);
            return projector.eval(frameTuple);
          }
        }
      } // the second if end false
    } // for
    return null;
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
  }
}
