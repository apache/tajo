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
import org.apache.tajo.engine.planner.physical.ComparableVector.ComparableTuple;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Pair;
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
  protected Map<ComparableTuple, Pair<Boolean, List<Tuple>>> tupleSlots;

  protected boolean needEvaluation; // true for matched
  protected Iterator<Tuple> iterator;

  protected ComparableTuple leftKeyTuple;
  protected ComparableTuple rightKeyTuple;

  protected int [] leftKeyList;
  protected int [] rightKeyList;

  protected boolean finished = false;
  protected boolean finalLoop = false;

  // projection
  protected final Projector projector;

  private int rightNumCols;
  private int leftNumCols;

  public HashFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                               PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.tupleSlots = new HashMap<ComparableTuple, Pair<Boolean, List<Tuple>>>(10000);

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

    leftNumCols = outer.getSchema().size();
    rightNumCols = inner.getSchema().size();

    leftKeyTuple = new ComparableTuple(outer.getSchema(), leftKeyList);
    rightKeyTuple = new ComparableTuple(inner.getSchema(), rightKeyList);
  }

  @Override
  protected void compile() throws CompilationError {
    joinQual = context.getPrecompiledEval(inSchema, joinQual);
  }

  public Iterator<Tuple> getNextUnmatchedRight() {

    return new Iterator<Tuple>() {

      private Iterator<Pair<Boolean, List<Tuple>>> iterator1 = tupleSlots.values().iterator();
      private Iterator<Tuple> iterator2;

      @Override
      public boolean hasNext() {
        if (iterator2 != null && iterator2.hasNext()) {
          return true;
        }
        for (iterator2 = null; iterator2 == null && iterator1.hasNext();) {
          Pair<Boolean, List<Tuple>> next = iterator1.next();
          if (!next.getFirst()) {
            iterator2 = next.getSecond().iterator();
          }
        }
        return iterator2 != null && iterator2.hasNext();
      }

      @Override
      public Tuple next() {
        return iterator2.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    while(!context.isStopped() && !finished) {
      if (iterator != null && iterator.hasNext()) {
        frameTuple.setRight(iterator.next());
        if (needEvaluation && !joinQual.eval(inSchema, frameTuple).isTrue()) {
          continue;
        }
        projector.eval(frameTuple, outTuple);
        return outTuple;
      }
      if (finalLoop) {
        finished = true;
        outTuple = null;
        return null;
      }
      Tuple leftTuple = leftChild.next(); // it comes from a disk
      if (leftTuple == null) { // if no more tuples in left tuples on disk, a join is completed.
        // in this stage we can begin outputing tuples from the right operand (which were before in tupleSlots) null padded on the left side
        frameTuple.setLeft(TupleUtil.createNullPaddedTuple(leftNumCols));
        iterator = getNextUnmatchedRight();
        needEvaluation = false;
        finalLoop = true;
        continue;
      }

      frameTuple.setLeft(leftTuple);

      // getting corresponding right
      leftKeyTuple.set(leftTuple);
      Pair<Boolean, List<Tuple>> rightTuples = tupleSlots.get(leftKeyTuple);
      if (rightTuples == null) {
        //this left tuple doesn't have a match on the right.But full outer join => we should keep it anyway
        //output a tuple with the nulls padded rightTuple
        iterator = Arrays.asList(TupleUtil.createNullPaddedTuple(rightNumCols)).iterator();
        needEvaluation = false;
        continue;
      }
      rightTuples.setFirst(true);
      iterator = rightTuples.getSecond().iterator();
      needEvaluation = true;
    }
    return outTuple;
  }

  protected void loadRightToHashTable() throws IOException {
    Tuple tuple;
    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      rightKeyTuple.set(tuple);
      Pair<Boolean, List<Tuple>> newValue = tupleSlots.get(rightKeyTuple);
      if (newValue == null) {
        tupleSlots.put(rightKeyTuple.copy(),
            newValue = new Pair<Boolean, List<Tuple>>(false, new ArrayList<Tuple>()));
      }
      newValue.getSecond().add(tuple);
    }
    first = false;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    for (Pair<Boolean, List<Tuple>> value : tupleSlots.values()) {
      value.setFirst(false);
    }
    finished = false;
    finalLoop = false;
    iterator = null;
    needEvaluation = false;
  }

  @Override
  public void close() throws IOException {
    super.close();
    tupleSlots.clear();
    tupleSlots = null;
    iterator = null;
    plan = null;
    joinQual = null;
  }

  public JoinNode getPlan() {
    return this.plan;
  }
}

