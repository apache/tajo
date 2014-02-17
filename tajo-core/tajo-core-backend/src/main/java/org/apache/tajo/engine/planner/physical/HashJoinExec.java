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
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class HashJoinExec extends BinaryPhysicalExec {
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

  public HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftExec,
      PhysicalExec rightExec) {
    super(context, SchemaUtil.merge(leftExec.getSchema(), rightExec.getSchema()), plan.getOutSchema(),
        leftExec, rightExec);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual,
        leftExec.getSchema(), rightExec.getSchema());

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = leftExec.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = rightExec.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
    leftKeyTuple = new VTuple(leftKeyList.length);
  }

  protected void getKeyLeftTuple(final Tuple outerTuple, Tuple keyTuple) {
    for (int i = 0; i < leftKeyList.length; i++) {
      keyTuple.put(i, outerTuple.get(leftKeyList[i]));
    }
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
          finished = true;
          return null;
        }

        // getting corresponding right
        getKeyLeftTuple(leftTuple, leftKeyTuple); // get a left key tuple
        if (tupleSlots.containsKey(leftKeyTuple)) { // finds right tuples on in-memory hash table.
          iterator = tupleSlots.get(leftKeyTuple).iterator();
          shouldGetLeftTuple = false;
        } else {
          shouldGetLeftTuple = true;
          continue;
        }
      }

      // getting a next right tuple on in-memory hash table.
      rightTuple = iterator.next();
      frameTuple.set(leftTuple, rightTuple); // evaluate a join condition on both tuples
      if (joinQual.eval(inSchema, frameTuple).isTrue()) { // if both tuples are joinable
        projector.eval(frameTuple, outTuple);
        found = true;
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
      List<Tuple> newValue;
      for (int i = 0; i < rightKeyList.length; i++) {
        keyTuple.put(i, tuple.get(rightKeyList[i]));
      }

      if (tupleSlots.containsKey(keyTuple)) {
        newValue = tupleSlots.get(keyTuple);
        newValue.add(tuple);
        tupleSlots.put(keyTuple, newValue);
      } else {
        newValue = new ArrayList<Tuple>();
        newValue.add(tuple);
        tupleSlots.put(keyTuple, newValue);
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
    if (tupleSlots != null) {
      tupleSlots.clear();
      tupleSlots = null;
    }

    iterator = null;
    plan = null;
    joinQual = null;
  }

  public JoinNode getPlan() {
    return this.plan;
  }
}
