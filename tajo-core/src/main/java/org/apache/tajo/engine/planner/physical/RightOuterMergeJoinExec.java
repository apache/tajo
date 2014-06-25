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
import org.apache.tajo.datum.DatumFactory;
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

public class RightOuterMergeJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private EvalNode joinQual;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple leftTuple = null;
  private Tuple rightTuple = null;
  private Tuple outTuple = null;
  private Tuple nextLeft = null;

  private List<Tuple> leftTupleSlots;
  private List<Tuple> innerTupleSlots;

  private JoinTupleComparator joinComparator = null;
  private TupleComparator[] tupleComparator = null;

  private final static int INITIAL_TUPLE_SLOT = 10000;

  private boolean end = false;

  // projection
  private Projector projector;

  private int rightNumCols;
  private int leftNumCols;
  private int posRightTupleSlots = -1;
  private int posLeftTupleSlots = -1;
  private boolean endInPopulationStage = false;
  private boolean initRightDone = false;

  public RightOuterMergeJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                                 PhysicalExec inner, SortSpec[] outerSortKey, SortSpec[] innerSortKey) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");
    this.joinNode = plan;
    this.joinQual = plan.getJoinQual();

    this.leftTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joinComparator = new JoinTupleComparator(outer.getSchema(), inner.getSchema(), sortSpecs);
    this.tupleComparator = PlannerUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), outer.getSchema(), inner.getSchema());

    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());

    leftNumCols = outer.getSchema().size();
  }

  public JoinNode getPlan() {
    return this.joinNode;
  }

  /**
   * creates a tuple of a given size filled with NULL values in all fields
   */
  private Tuple createNullPaddedTuple(int columnNum){
    VTuple tuple = new VTuple(columnNum);
    for (int i = 0; i < columnNum; i++) {
      tuple.put(i, DatumFactory.createNullDatum());
    }
    return tuple;
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
  public Tuple next() throws IOException {
    Tuple previous;

    for (;;) {
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
          if (initRightDone == false) {
            // maybe the left operand was empty => the right one didn't have the chance to initialize
            rightTuple = rightChild.next();
            initRightDone = true;
          }

          if(rightTuple == null) {
            return null;
          } else {
            // output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, rightTuple);
            projector.eval(frameTuple, outTuple);

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

        if(rightTuple == null){
          rightTuple = rightChild.next();

          if(rightTuple != null){
            initRightDone = true;
          }
          else {
            initRightDone = true;
            end = true;
            continue;
          }
        }
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
            Tuple nullPaddedTuple = createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, rightTuple);
            projector.eval(frameTuple, outTuple);

            // we simulate we found a match, which is exactly the null padded one
            // BEFORE RETURN, MOVE FORWARD
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              end = true;
            }
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

        // once a match is found, retain all tuples with this key in tuple slots on each side
        if(!end) {
          endInPopulationStage = false;

          boolean endOuter = false;
          boolean endInner = false;

          previous = new VTuple(leftTuple);
          do {
            leftTupleSlots.add(new VTuple(leftTuple));
            leftTuple = leftChild.next();
            if( leftTuple == null) {
              endOuter = true;
            }
          } while ((endOuter != true) && (tupleComparator[0].compare(previous, leftTuple) == 0));
          posLeftTupleSlots = 0;

          previous = new VTuple(rightTuple);

          do {
            innerTupleSlots.add(new VTuple(rightTuple));
            rightTuple = rightChild.next();
            if(rightTuple == null) {
              endInner = true;
            }

          } while ((endInner != true) && (tupleComparator[1].compare(previous, rightTuple) == 0) );
          posRightTupleSlots = 0;

          if ((endOuter == true) || (endInner == true)) {
            end = true;
            endInPopulationStage = true;
          }
        } // if end false
      } // if newRound


      // Now output result matching tuples from the slots
      // if either we haven't reached end on neither side, or we did reach end on one(or both) sides
      // but that happened in the slots population step (i.e. refers to next round)

      if ((end == false) || ((end == true) && (endInPopulationStage == true))){

        if(posLeftTupleSlots == 0){
          nextLeft = new VTuple (leftTupleSlots.get(posLeftTupleSlots));
          posLeftTupleSlots = posLeftTupleSlots + 1;
        }


        if(posRightTupleSlots <= (innerTupleSlots.size() -1)) {

          Tuple aTuple = new VTuple(innerTupleSlots.get(posRightTupleSlots));
          posRightTupleSlots = posRightTupleSlots + 1;

          frameTuple.set(nextLeft, aTuple);
          if (joinQual.eval(inSchema, frameTuple).asBool()) {
            projector.eval(frameTuple, outTuple);
            return outTuple;
          } else {
            // padding null
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, aTuple);
            projector.eval(frameTuple, outTuple);
            return outTuple;
          }
        } else {
          // right (inner) slots reached end and should be rewind if there are still tuples in the outer slots
          if(posLeftTupleSlots <= (leftTupleSlots.size() - 1)) {
            //rewind the right slots position
            posRightTupleSlots = 0;
            Tuple aTuple = new VTuple(innerTupleSlots.get(posRightTupleSlots));
            posRightTupleSlots = posRightTupleSlots + 1;
            nextLeft = new VTuple (leftTupleSlots.get(posLeftTupleSlots));
            posLeftTupleSlots = posLeftTupleSlots + 1;

            frameTuple.set(nextLeft, aTuple);

            if (joinQual.eval(inSchema, frameTuple).asBool()) {
              projector.eval(frameTuple, outTuple);
              return outTuple;
            } else {
              // padding null
              Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(leftNumCols);
              frameTuple.set(nullPaddedTuple, aTuple);
              projector.eval(frameTuple, outTuple);
              return outTuple;
            }
          }
        }
      } // the second if end false
    } // for
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    leftTupleSlots.clear();
    innerTupleSlots.clear();
    posRightTupleSlots = -1;
    posLeftTupleSlots = -1;
  }

  @Override
  public void close() throws IOException {
    super.close();
    leftTupleSlots.clear();
    innerTupleSlots.clear();
    leftTupleSlots = null;
    innerTupleSlots = null;
    joinNode = null;
    joinQual = null;
    projector = null;
  }
}

