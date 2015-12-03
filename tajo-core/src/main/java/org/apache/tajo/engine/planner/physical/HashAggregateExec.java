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

import org.apache.tajo.SessionVars;
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * This is the hash-based GroupBy Operator.
 */
public class HashAggregateExec extends AggregationExec {
  private Tuple tuple = null;
  private TupleMap<FunctionContext[]> hashTable;
  private KeyProjector hashKeyProjector;
  private boolean computed = false;
  private Iterator<Entry<KeyTuple, FunctionContext []>> iterator = null;

  public HashAggregateExec(TaskAttemptContext ctx, GroupbyNode plan, PhysicalExec subOp) throws IOException {
    super(ctx, plan, subOp);
    hashKeyProjector = new KeyProjector(inSchema, plan.getGroupingColumns());
    hashTable = new TupleMap<>(ctx.getQueryContext().getInt(SessionVars.AGG_HASH_TABLE_SIZE));
    this.tuple = new VTuple(plan.getOutSchema().size());
  }

  private void compute() throws IOException {
    Tuple tuple;
    KeyTuple keyTuple;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      keyTuple = hashKeyProjector.project(tuple);

      FunctionContext [] contexts = hashTable.get(keyTuple);
      if(contexts != null) {
        for(int i = 0; i < aggFunctions.size(); i++) {
          aggFunctions.get(i).merge(contexts[i], tuple);
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for(int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions.get(i).newContext();
          aggFunctions.get(i).merge(contexts[i], tuple);
        }
        hashTable.put(keyTuple, contexts);
      }
    }

    // If HashAggregateExec received NullDatum and didn't has any grouping keys,
    // it should return primitive values for NullLDatum.
    if (groupingKeyNum == 0 && aggFunctionsNum > 0 && hashTable.entrySet().size() == 0) {
      FunctionContext[] contexts = new FunctionContext[aggFunctionsNum];
      for(int i = 0; i < aggFunctionsNum; i++) {
        contexts[i] = aggFunctions.get(i).newContext();
      }
      hashTable.put(null, contexts);
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(!computed) {
      compute();
      iterator = hashTable.entrySet().iterator();
      computed = true;
    }

    FunctionContext [] contexts;

    if (iterator.hasNext()) {
      Entry<KeyTuple, FunctionContext []> entry = iterator.next();
      Tuple keyTuple = entry.getKey();
      contexts =  entry.getValue();

      int tupleIdx = 0;
      for (; tupleIdx < groupingKeyNum; tupleIdx++) {
        tuple.put(tupleIdx, keyTuple.asDatum(tupleIdx));
      }
      for (int funcIdx = 0; funcIdx < aggFunctionsNum; funcIdx++, tupleIdx++) {
        tuple.put(tupleIdx, aggFunctions.get(funcIdx).terminate(contexts[funcIdx]));
      }

      return tuple;
    } else {
      return null;
    }
  }

  @Override
  public void rescan() throws IOException {
    iterator = hashTable.entrySet().iterator();
  }

  @Override
  public void close() throws IOException {
    super.close();
    hashTable.clear();
    hashTable = null;
    iterator = null;
  }
}
