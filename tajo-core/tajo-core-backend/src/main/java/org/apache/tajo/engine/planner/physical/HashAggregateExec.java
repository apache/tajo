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

import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is the hash-based GroupBy Operator.
 */
public class HashAggregateExec extends AggregationExec {
  private Tuple tuple = null;
  private Map<Tuple, EvalContext[]> tupleSlots;
  private boolean computed = false;
  private Iterator<Entry<Tuple, EvalContext []>> iterator = null;

  /**
   * @throws java.io.IOException
	 * 
	 */
  public HashAggregateExec(TaskAttemptContext ctx, GroupbyNode annotation,
                           PhysicalExec subOp) throws IOException {
    super(ctx, annotation, subOp);
    tupleSlots = new HashMap<Tuple, EvalContext[]>(10000);
    this.tuple = new VTuple(evalSchema.getColumnNum());
  }
  
  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    int targetLength = plan.getTargets().length;
    while((tuple = child.next()) != null && !context.isStopped()) {
      keyTuple = new VTuple(keylist.length);
      // build one key tuple
      for(int i = 0; i < keylist.length; i++) {
        keyTuple.put(i, tuple.get(keylist[i]));
      }
      
      if(tupleSlots.containsKey(keyTuple)) {
        EvalContext [] tmpTuple = tupleSlots.get(keyTuple);
        for(int i = 0; i < measureList.length; i++) {
          evals[measureList[i]].eval(tmpTuple[measureList[i]], inSchema, tuple);
        }
      } else { // if the key occurs firstly
        EvalContext evalCtx [] = new EvalContext[targetLength];
        for(int i = 0; i < targetLength; i++) {
          evalCtx[i] = evals[i].newContext();
          evals[i].eval(evalCtx[i], inSchema, tuple);
        }
        tupleSlots.put(keyTuple, evalCtx);
      }
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(!computed) {
      compute();
      iterator = tupleSlots.entrySet().iterator();
      computed = true;
    }

    EvalContext [] ctx;
    if (havingQual == null) {
      if (iterator.hasNext()) {
      ctx =  iterator.next().getValue();

      for (int i = 0; i < ctx.length; i++) {
        tuple.put(i, evals[i].terminate(ctx[i]));
      }

      return tuple;
      } else {
        return null;
      }
    } else {
      while(iterator.hasNext()) {
        ctx =  iterator.next().getValue();
        for (int i = 0; i < ctx.length; i++) {
          tuple.put(i, evals[i].terminate(ctx[i]));
        }
        havingQual.eval(havingContext, evalSchema, tuple);
        if (havingQual.terminate(havingContext).asBool()) {
          return tuple;
        }
      }
      return null;
    }
  }

  @Override
  public void rescan() throws IOException {    
    iterator = tupleSlots.entrySet().iterator();
  }

  @Override
  public void close() throws IOException {
    super.close();
    tupleSlots.clear();
  }
}
