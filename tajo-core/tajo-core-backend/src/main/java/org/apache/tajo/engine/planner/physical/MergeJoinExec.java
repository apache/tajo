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
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private EvalNode joinQual;
  private EvalContext qualCtx;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private Tuple outerNext = null;

  private final List<Tuple> outerTupleSlots;
  private final List<Tuple> innerTupleSlots;
  private Iterator<Tuple> outerIterator;
  private Iterator<Tuple> innerIterator;

  private JoinTupleComparator joincomparator = null;
  private TupleComparator[] tupleComparator = null;

  private final static int INITIAL_TUPLE_SLOT = 10000;

  private boolean end = false;

  // projection
  private final Projector projector;
  private final EvalContext [] evalContexts;

  public MergeJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner, SortSpec[] outerSortKey, SortSpec[] innerSortKey) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");
    this.joinNode = plan;
    this.joinQual = plan.getJoinQual();
    this.qualCtx = this.joinQual.newContext();

    this.outerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joincomparator = new JoinTupleComparator(outer.getSchema(),
        inner.getSchema(), sortSpecs);
    this.tupleComparator = PlannerUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), outer.getSchema(), inner.getSchema());
    this.outerIterator = outerTupleSlots.iterator();
    this.innerIterator = innerTupleSlots.iterator();
    
    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
  }

  public JoinNode getPlan(){
    return this.joinNode;
  }

  public Tuple next() throws IOException {
    Tuple previous;

    for (;;) {
      if (!outerIterator.hasNext() && !innerIterator.hasNext()) {
        if(end){
          return null;
        }

        if(outerTuple == null){
          outerTuple = leftChild.next();
        }
        if(innerTuple == null){
          innerTuple = rightChild.next();
        }

        outerTupleSlots.clear();
        innerTupleSlots.clear();

        int cmp;
        while ((cmp = joincomparator.compare(outerTuple, innerTuple)) != 0) {
          if (cmp > 0) {
            innerTuple = rightChild.next();
          } else if (cmp < 0) {
            outerTuple = leftChild.next();
          }
          if (innerTuple == null || outerTuple == null) {
            return null;
          }
        }

        try {
          previous = outerTuple.clone();
          do {
            outerTupleSlots.add(outerTuple.clone());
            outerTuple = leftChild.next();
            if (outerTuple == null) {
              end = true;
              break;
            }
          } while (tupleComparator[0].compare(previous, outerTuple) == 0);
          outerIterator = outerTupleSlots.iterator();
          outerNext = outerIterator.next();

          previous = innerTuple.clone();
          do {
            innerTupleSlots.add(innerTuple.clone());
            innerTuple = rightChild.next();
            if (innerTuple == null) {
              end = true;
              break;
            }
          } while (tupleComparator[1].compare(previous, innerTuple) == 0);
          innerIterator = innerTupleSlots.iterator();
        } catch (CloneNotSupportedException e) {

        }
      }

      if(!innerIterator.hasNext()){
        outerNext = outerIterator.next();
        innerIterator = innerTupleSlots.iterator();
      }

      frameTuple.set(outerNext, innerIterator.next());
      joinQual.eval(qualCtx, inSchema, frameTuple);
      if (joinQual.terminate(qualCtx).asBool()) {
        projector.eval(evalContexts, frameTuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    outerTupleSlots.clear();
    innerTupleSlots.clear();
    outerIterator = outerTupleSlots.iterator();
    innerIterator = innerTupleSlots.iterator();
  }
}
