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

public class RightOuterMergeJoinExec extends CommonJoinExec {
  // temporal tuples and states for nested loop join
  private Tuple leftTuple = null;
  private Tuple rightTuple = null;
  private Tuple prevLeftTuple;
  private Tuple prevRightTuple;
  private Tuple nextLeft = null;
  private Tuple nullPaddedTuple;

  private TupleList leftTupleSlots;
  private TupleList innerTupleSlots;

  private JoinTupleComparator joinComparator = null;
  private TupleComparator [] tupleComparator = null;

  private boolean end = false;

  private int leftNumCols;
  private int posRightTupleSlots = -1;
  private int posLeftTupleSlots = -1;
  private boolean endInPopulationStage = false;
  private boolean initRightDone = false;

  public RightOuterMergeJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                                 PhysicalExec inner, SortSpec[] outerSortKey, SortSpec[] innerSortKey) {
    super(context, plan, outer, inner);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");

    final int INITIAL_TUPLE_SLOT = context.getQueryContext().getInt(SessionVars.JOIN_HASH_TABLE_SIZE);
    this.leftTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joinComparator = new JoinTupleComparator(outer.getSchema(), inner.getSchema(), sortSpecs);
    this.tupleComparator = PhysicalPlanUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), outer.getSchema(), inner.getSchema());

    leftNumCols = outer.getSchema().size();
    nullPaddedTuple = NullTuple.create(leftNumCols);
  }

  /**
   *
   * Right outer merge join consists of four stages
   * <ul>
   *   <li>initialization stage: </li>
   *   <li>finalizing stage: </li>
   * </ul>
   *
   * @return
   * @throws IOException
   */
  @Override
  public Tuple next() throws IOException {
    Tuple outTuple;

    while (!context.isStopped()) {
      boolean newRound = false;
      if((posRightTupleSlots == -1) && (posLeftTupleSlots == -1)) {
        newRound = true;
      }
      if ((posRightTupleSlots == innerTupleSlots.size()) && (posLeftTupleSlots == leftTupleSlots.size())) {
        newRound = true;
      }

      if (newRound) {

        //////////////////////////////////////////////////////////////////////
        // BEGIN FINALIZING STAGE
        //////////////////////////////////////////////////////////////////////

        // The finalizing stage, where remaining tuples on the only right are transformed into left-padded results
        if (end) {
          if (!initRightDone) {
            // maybe the left operand was empty => the right one didn't have the chance to initialize
            rightTuple = rightChild.next();
            initRightDone = true;
          }

          if(rightTuple == null) {
            return null;
          } else {
            // output a tuple with the nulls padded leftTuple
            frameTuple.set(nullPaddedTuple, rightTuple);
            outTuple = projector.eval(frameTuple);

            // we simulate we found a match, which is exactly the null padded one
            rightTuple = rightChild.next();

            return outTuple;
          }
        }
        //////////////////////////////////////////////////////////////////////
        // END FINALIZING STAGE
        //////////////////////////////////////////////////////////////////////


        //////////////////////////////////////////////////////////////////////
        // BEGIN INITIALIZATION STAGE
        //////////////////////////////////////////////////////////////////////

        // This stage reads the first tuple on each side
        if (leftTuple == null) {
          leftTuple = leftChild.next();

          if (leftTuple == null) {
            end = true;
            continue;
          }
        }

        if(rightTuple == null) {
          rightTuple = rightChild.next();
          if (rightTuple == null) {
            initRightDone = true;
            end = true;
            continue;
          }
        }
        if (rightFiltered(rightTuple)) {
          frameTuple.set(nullPaddedTuple, rightTuple);
          outTuple = projector.eval(frameTuple);
          rightTuple = null;

          return outTuple;
        }
        initRightDone = true;

        //////////////////////////////////////////////////////////////////////
        // END INITIALIZATION STAGE
        //////////////////////////////////////////////////////////////////////

        // reset tuple slots for a new round
        leftTupleSlots.clear();
        innerTupleSlots.clear();
        posRightTupleSlots = -1;
        posLeftTupleSlots = -1;


        //////////////////////////////////////////////////////////////////////
        // BEGIN MOVE FORWARDING STAGE
        //////////////////////////////////////////////////////////////////////

        // This stage moves forward a tuple cursor on each side relation until a match
        // is found
        int cmp;
        while ((end != true) && ((cmp = joinComparator.compare(leftTuple, rightTuple)) != 0)) {

          // if right is lower than the left tuple, it means that all right tuples s.t. cmp <= 0 are
          // matched tuples.
          if (cmp > 0) {
            // before getting a new tuple from the right,  a left null padded tuple should be built
            // output a tuple with the nulls padded left tuple
            frameTuple.set(nullPaddedTuple, rightTuple);
            outTuple = projector.eval(frameTuple);

            // we simulate we found a match, which is exactly the null padded one
            // BEFORE RETURN, MOVE FORWARD
            rightTuple = null;
            return outTuple;

          } else if (cmp < 0) {
            // If the left tuple is lower than the right tuple, just move forward the left tuple cursor.
            leftTuple = leftChild.next();
            if(leftTuple == null) {
              end = true;
              // in original algorithm we had return null ,
              // but now we need to continue the end processing phase for remaining unprocessed right tuples
            }
          } // if (cmp<0)
        } // while
        //////////////////////////////////////////////////////////////////////
        // END MOVE FORWARDING STAGE
        //////////////////////////////////////////////////////////////////////

        // Check null values
        Tuple leftKey = rightKeyExtractor.project(rightTuple);
        boolean containNull = false;
        for (int i = 0; i < leftKey.size(); i++) {
          if (leftKey.isBlankOrNull(i)) {
            containNull = true;
            break;
          }
        }

        if (containNull) {
          frameTuple.set(nullPaddedTuple, rightTuple);
          outTuple = projector.eval(frameTuple);
          leftTuple = leftChild.next();
          rightTuple = rightChild.next();

          if (leftTuple == null || rightTuple == null) {
            end = true;
          }
          return outTuple;
        }

        // once a match is found, retain all tuples with this key in tuple slots on each side
        if(!end) {
          endInPopulationStage = false;

          boolean endOuter = false;
          boolean endInner = false;

          if (prevLeftTuple == null) {
            prevLeftTuple = new VTuple(leftTuple.getValues());
          } else {
            prevLeftTuple.put(leftTuple.getValues());
          }
          do {
            leftTupleSlots.add(leftTuple);
            leftTuple = leftChild.next();
            if( leftTuple == null) {
              endOuter = true;
            }
          } while ((endOuter != true) && (tupleComparator[0].compare(prevLeftTuple, leftTuple) == 0));
          posLeftTupleSlots = 0;

          if (prevRightTuple == null) {
            prevRightTuple = new VTuple(rightTuple.getValues());
          } else {
            prevRightTuple.put(rightTuple.getValues());
          }
          do {
            innerTupleSlots.add(rightTuple);
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              endInner = true;
            }

          } while ((endInner != true) && (tupleComparator[1].compare(prevRightTuple, rightTuple) == 0) );
          posRightTupleSlots = 0;

          if ((endOuter == true) || (endInner == true)) {
            end = true;
            endInPopulationStage = true;
          }
        } // if end false
        if (prevRightTuple != null && rightFiltered(prevRightTuple)) {
          frameTuple.set(nullPaddedTuple, prevRightTuple);
          outTuple = projector.eval(frameTuple);

          // reset tuple slots for a new round
          leftTupleSlots.clear();
          innerTupleSlots.clear();
          posRightTupleSlots = -1;
          posLeftTupleSlots = -1;

          return outTuple;
        }
      } // if newRound


      // Now output result matching tuples from the slots
      // if either we haven't reached end on neither side, or we did reach end on one(or both) sides
      // but that happened in the slots population step (i.e. refers to next round)

      if ((end == false) || ((end == true) && (endInPopulationStage == true))){

        if(posLeftTupleSlots == 0){
          nextLeft = leftTupleSlots.get(posLeftTupleSlots);
          posLeftTupleSlots = posLeftTupleSlots + 1;
        }


        if(posRightTupleSlots <= (innerTupleSlots.size() -1)) {

          Tuple aTuple = innerTupleSlots.get(posRightTupleSlots);
          posRightTupleSlots = posRightTupleSlots + 1;

          frameTuple.set(nextLeft, aTuple);
          if (joinQual.eval(frameTuple).isTrue()) {
            return projector.eval(frameTuple);
          } else {
            // padding null
            frameTuple.set(nullPaddedTuple, aTuple);
            return projector.eval(frameTuple);
          }
        } else {
          // right (inner) slots reached end and should be rewind if there are still tuples in the outer slots
          if(posLeftTupleSlots <= (leftTupleSlots.size() - 1)) {
            //rewind the right slots position
            posRightTupleSlots = 0;
            Tuple aTuple = innerTupleSlots.get(posRightTupleSlots);
            posRightTupleSlots = posRightTupleSlots + 1;
            nextLeft = leftTupleSlots.get(posLeftTupleSlots);
            posLeftTupleSlots = posLeftTupleSlots + 1;

            frameTuple.set(nextLeft, aTuple);

            if (joinQual.eval(frameTuple).isTrue()) {
              return projector.eval(frameTuple);
            } else {
              // padding null
              frameTuple.set(nullPaddedTuple, aTuple);
              return projector.eval(frameTuple);
            }
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
    innerTupleSlots.clear();
    posRightTupleSlots = -1;
    posLeftTupleSlots = -1;
    leftTuple = rightTuple = null;
    prevLeftTuple = prevRightTuple = null;
    nextLeft = null;
  }

  @Override
  public void close() throws IOException {
    super.close();
    leftTupleSlots.clear();
    innerTupleSlots.clear();
    leftTupleSlots = null;
    innerTupleSlots = null;
  }
}

