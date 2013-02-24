/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.planner.physical;

import tajo.TaskAttemptContext;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.logical.JoinNode;
import tajo.engine.utils.SchemaUtil;
import tajo.storage.FrameTuple;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BNLJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private EvalNode joinQual;
  private EvalContext qualCtx;

  private final List<Tuple> outerTupleSlots;
  private final List<Tuple> innerTupleSlots;
  private Iterator<Tuple> outerIterator;
  private Iterator<Tuple> innerIterator;

  private boolean innerEnd;
  private boolean outerEnd;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple outputTuple = null;
  private Tuple innext = null;

  private final int TUPLE_SLOT_SIZE = 10000;

  // projection
  private final int[] targetIds;

  public BNLJoinExec(final TaskAttemptContext context, final JoinNode join,
                     final PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        SchemaUtil.merge(outer.getSchema(), inner.getSchema()), outer, inner);
    this.joinQual = join.getJoinQual();
    this.qualCtx = this.joinQual.newContext();
    this.outerTupleSlots = new ArrayList<Tuple>(TUPLE_SLOT_SIZE);
    this.innerTupleSlots = new ArrayList<Tuple>(TUPLE_SLOT_SIZE);
    this.outerIterator = outerTupleSlots.iterator();
    this.innerIterator = innerTupleSlots.iterator();
    this.innerEnd = false;
    this.outerEnd = false;

    // for projection
    targetIds = RowStoreUtil.getTargetIds(inSchema, outSchema);

    // for join
    frameTuple = new FrameTuple();
    outputTuple = new VTuple(outSchema.getColumnNum());
  }

  public Tuple next() throws IOException {

    if (outerTupleSlots.isEmpty()) {
      for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
        Tuple t = outerChild.next();
        if (t == null) {
          outerEnd = true;
          break;
        }
        outerTupleSlots.add(t);
      }
      outerIterator = outerTupleSlots.iterator();
      outerTuple = outerIterator.next();
    }

    if (innerTupleSlots.isEmpty()) {
      for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
        Tuple t = innerChild.next();
        if (t == null) {
          innerEnd = true;
          break;
        }
        innerTupleSlots.add(t);
      }
      innerIterator = innerTupleSlots.iterator();
    }

    if((innext = innerChild.next()) == null){
      innerEnd = true;
    }

    while (true) {
      if (!innerIterator.hasNext()) { // if inneriterator ended
        if (outerIterator.hasNext()) { // if outertupleslot remains
          outerTuple = outerIterator.next();
          innerIterator = innerTupleSlots.iterator();
        } else {
          if (innerEnd) {
            innerChild.rescan();
            innerEnd = false;
            
            if (outerEnd) {
              return null;
            }
            outerTupleSlots.clear();
            for (int k = 0; k < TUPLE_SLOT_SIZE; k++) {
              Tuple t = outerChild.next();
              if (t == null) {
                outerEnd = true;
                break;
              }
              outerTupleSlots.add(t);
            }
            if (outerTupleSlots.isEmpty()) {
              return null;
            }
            outerIterator = outerTupleSlots.iterator();
            outerTuple = outerIterator.next();
            
          } else {
            outerIterator = outerTupleSlots.iterator();
            outerTuple = outerIterator.next();
          }
          
          innerTupleSlots.clear();
          if (innext != null) {
            innerTupleSlots.add(innext);
            for (int k = 1; k < TUPLE_SLOT_SIZE; k++) { // fill inner
              Tuple t = innerChild.next();
              if (t == null) {
                innerEnd = true;
                break;
              }
              innerTupleSlots.add(t);
            }
          } else {
            for (int k = 0; k < TUPLE_SLOT_SIZE; k++) { // fill inner
              Tuple t = innerChild.next();
              if (t == null) {
                innerEnd = true;
                break;
              }
              innerTupleSlots.add(t);
            }
          }
          
          if ((innext = innerChild.next()) == null) {
            innerEnd = true;
          }
          innerIterator = innerTupleSlots.iterator();
        }
      }

      frameTuple.set(outerTuple, innerIterator.next());
      if (joinQual != null) {
        joinQual.eval(qualCtx, inSchema, frameTuple);
        if (joinQual.terminate(qualCtx).asBool()) {
          RowStoreUtil.project(frameTuple, outputTuple, targetIds);
          return outputTuple;
        }
      } else {
        RowStoreUtil.project(frameTuple, outputTuple, targetIds);
        return outputTuple;
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    innerEnd = false;
    innerTupleSlots.clear();
    outerTupleSlots.clear();
    innerIterator = innerTupleSlots.iterator();
    outerIterator = outerTupleSlots.iterator();
  }
}
