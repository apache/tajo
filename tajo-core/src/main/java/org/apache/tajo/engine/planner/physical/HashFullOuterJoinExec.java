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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.codegen.CompilationError;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;


public class HashFullOuterJoinExec extends BinaryPhysicalExec {
  // from logical plan
  protected JoinNode plan;
  protected EvalNode joinQual;

  protected List<Column[]> joinKeyPairs;

  // temporal tuples and states for nested loop join
  protected boolean first = true;
  protected FrameTuple frameTuple;
  protected Tuple outTuple = null;
  protected Map<Tuple, List<Tuple>> tupleSlots;
  protected Iterator<Tuple> iterator = null;
  protected Tuple leftTuple;
  protected Tuple leftKeyTuple;

  protected int [] leftKeyList;
  protected int [] rightKeyList;

  protected boolean finished = false;
  protected boolean shouldGetLeftTuple = true;

  // projection
  protected final Projector projector;

  private int rightNumCols;
  private int leftNumCols;
  private Map<Tuple, Boolean> matched;

  public HashFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                               PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

    // this hashmap mirrors the evolution of the tupleSlots, with the same keys. For each join key,
    // we have a boolean flag, initially false (whether this join key had at least one match on the left operand)
    this.matched = new HashMap<Tuple, Boolean>(10000);

    // HashJoin only can manage equi join key pairs.
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(), inner.getSchema(),
        false);

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = outer.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = inner.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());
    leftKeyTuple = new VTuple(leftKeyList.length);

    leftNumCols = outer.getSchema().size();
    rightNumCols = inner.getSchema().size();
  }

  @Override
  protected void compile() throws CompilationError {
    joinQual = context.getPrecompiledEval(inSchema, joinQual);
  }

  protected void getKeyLeftTuple(final Tuple outerTuple, Tuple keyTuple) {
    for (int i = 0; i < leftKeyList.length; i++) {
      keyTuple.put(i, outerTuple.get(leftKeyList[i]));
    }
  }

  public Tuple getNextUnmatchedRight() {

    List<Tuple> newValue;
    Tuple returnedTuple;
    // get a keyTUple from the matched hashmap with a boolean false value
    for(Tuple aKeyTuple : matched.keySet()) {
      if(matched.get(aKeyTuple) == false) {
        newValue = tupleSlots.get(aKeyTuple);
        returnedTuple = newValue.remove(0);
        tupleSlots.put(aKeyTuple, newValue);

        // after taking the last element from the list in tupleSlots, set flag true in matched as well
        if(newValue.isEmpty()){
          matched.put(aKeyTuple, true);
        }

        return returnedTuple;
      }
    }
    return null;
  }

  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    Tuple rightTuple;
    boolean found = false;

    while(!finished) {
      if (shouldGetLeftTuple) { // initially, it is true.
        // getting new outer
        leftTuple = leftChild.next(); // it comes from a disk
        if (leftTuple == null) { // if no more tuples in left tuples on disk, a join is completed.
          // in this stage we can begin outputing tuples from the right operand (which were before in tupleSlots) null padded on the left side
          Tuple unmatchedRightTuple = getNextUnmatchedRight();
          if( unmatchedRightTuple == null) {
            finished = true;
            outTuple = null;
            return null;
          } else {
            Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(leftNumCols);
            frameTuple.set(nullPaddedTuple, unmatchedRightTuple);
            projector.eval(frameTuple, outTuple);

            return outTuple;
          }
        }

        // getting corresponding right
        getKeyLeftTuple(leftTuple, leftKeyTuple); // get a left key tuple
        List<Tuple> rightTuples = tupleSlots.get(leftKeyTuple);
        if (rightTuples != null) { // found right tuples on in-memory hash table.
          iterator = rightTuples.iterator();
          shouldGetLeftTuple = false;
        } else {
          //this left tuple doesn't have a match on the right.But full outer join => we should keep it anyway
          //output a tuple with the nulls padded rightTuple
          Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
          frameTuple.set(leftTuple, nullPaddedTuple);
          projector.eval(frameTuple, outTuple);
          // we simulate we found a match, which is exactly the null padded one
          shouldGetLeftTuple = true;
          return outTuple;
        }
      }

      // getting a next right tuple on in-memory hash table.
      rightTuple = iterator.next();
      frameTuple.set(leftTuple, rightTuple); // evaluate a join condition on both tuples

      if (joinQual.eval(inSchema, frameTuple).isTrue()) { // if both tuples are joinable
        projector.eval(frameTuple, outTuple);
        found = true;
        getKeyLeftTuple(leftTuple, leftKeyTuple);
        matched.put(leftKeyTuple, true);
      }

      if (!iterator.hasNext()) { // no more right tuples for this hash key
        shouldGetLeftTuple = true;
      }

      if (found) {
        break;
      }
    }
    return outTuple;
  }

  protected void loadRightToHashTable() throws IOException {
    Tuple tuple;
    Tuple keyTuple;

    while ((tuple = rightChild.next()) != null) {
      keyTuple = new VTuple(joinKeyPairs.size());
      for (int i = 0; i < rightKeyList.length; i++) {
        keyTuple.put(i, tuple.get(rightKeyList[i]));
      }

      List<Tuple> newValue = tupleSlots.get(keyTuple);
      if (newValue != null) {
        newValue.add(tuple);
      } else {
        newValue = new ArrayList<Tuple>();
        newValue.add(tuple);
        tupleSlots.put(keyTuple, newValue);
        matched.put(keyTuple,false);
      }
    }
    first = false;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    tupleSlots.clear();
    first = true;

    finished = false;
    iterator = null;
    shouldGetLeftTuple = true;
  }

  @Override
  public void close() throws IOException {
    super.close();
    tupleSlots.clear();
    matched.clear();
    tupleSlots = null;
    matched = null;
    iterator = null;
    plan = null;
    joinQual = null;
  }

  public JoinNode getPlan() {
    return this.plan;
  }
}

