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
import tajo.engine.planner.logical.GroupbyNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * This is the sort-based Aggregation Operator.
 * 
 * @author Hyunsik Choi
 */
public class SortAggregateExec extends AggregationExec {
  private Tuple prevKey = null;
  private boolean finished = false;


  public SortAggregateExec(TaskAttemptContext context, GroupbyNode plan,
                           PhysicalExec child) throws IOException {
    super(context, plan, child);
  }

  @Override
  public Tuple next() throws IOException {
    Tuple curKey;
    Tuple tuple;
    Tuple finalTuple = null;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      // build a key tuple
      curKey = new VTuple(keylist.length);
      for(int i = 0; i < keylist.length; i++) {
        curKey.put(i, tuple.get(keylist[i]));
      }

      if (prevKey == null || prevKey.equals(curKey)) {
        if (prevKey == null) {
          for(int i = 0; i < outSchema.getColumnNum(); i++) {
            evalContexts[i] = evals[i].newContext();
            evals[i].eval(evalContexts[i], inSchema, tuple);
          }
          prevKey = curKey;
        } else {
          // aggregate
          for (int idx : measureList) {
            evals[idx].eval(evalContexts[idx], inSchema, tuple);
          }
        }
      } else {
        // finalize aggregate and return
        finalTuple = new VTuple(outSchema.getColumnNum());
        for(int i = 0; i < outSchema.getColumnNum(); i++) {
          finalTuple.put(i, evals[i].terminate(evalContexts[i]));
        }

        for(int i = 0; i < outSchema.getColumnNum(); i++) {
          evalContexts[i] = evals[i].newContext();
          evals[i].eval(evalContexts[i], inSchema, tuple);
        }
        prevKey = curKey;
        return finalTuple;
      }
    } // while loop

    if (!finished) {
      finalTuple = new VTuple(outSchema.getColumnNum());
      for(int i = 0; i < outSchema.getColumnNum(); i++) {
        finalTuple.put(i, evals[i].terminate(evalContexts[i]));
      }
      finished = true;
    }
    return finalTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    prevKey = null;
    finished = false;
  }
}
