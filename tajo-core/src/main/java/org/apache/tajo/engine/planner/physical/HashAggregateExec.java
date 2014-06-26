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

import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

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
  private Map<Tuple, FunctionContext[]> hashTable;
  private boolean computed = false;
  private Iterator<Entry<Tuple, FunctionContext []>> iterator = null;

  public HashAggregateExec(TaskAttemptContext ctx, GroupbyNode plan, PhysicalExec subOp) throws IOException {
    super(ctx, plan, subOp);
    hashTable = new HashMap<Tuple, FunctionContext []>(100000);
    this.tuple = new VTuple(plan.getOutSchema().size());
  }

  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    while((tuple = child.next()) != null && !context.isStopped()) {
      keyTuple = new VTuple(groupingKeyIds.length);
      // build one key tuple
      for(int i = 0; i < groupingKeyIds.length; i++) {
        keyTuple.put(i, tuple.get(groupingKeyIds[i]));
      }

      FunctionContext [] contexts = hashTable.get(keyTuple);
      if(contexts != null) {
        for(int i = 0; i < aggFunctions.length; i++) {
          aggFunctions[i].merge(contexts[i], inSchema, tuple);
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for(int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions[i].newContext();
          aggFunctions[i].merge(contexts[i], inSchema, tuple);
        }
        hashTable.put(keyTuple, contexts);
      }
    }

    // If HashAggregateExec received NullDatum and didn't has any grouping keys,
    // it should return primitive values for NullLDatum.
    if (groupingKeyNum == 0 && aggFunctionsNum > 0 && hashTable.entrySet().size() == 0) {
      FunctionContext[] contexts = new FunctionContext[aggFunctionsNum];
      for(int i = 0; i < aggFunctionsNum; i++) {
        contexts[i] = aggFunctions[i].newContext();
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
      Entry<Tuple, FunctionContext []> entry = iterator.next();
      Tuple keyTuple = entry.getKey();
      contexts =  entry.getValue();

      int tupleIdx = 0;
      for (; tupleIdx < groupingKeyNum; tupleIdx++) {
        tuple.put(tupleIdx, keyTuple.get(tupleIdx));
      }
      for (int funcIdx = 0; funcIdx < aggFunctionsNum; funcIdx++, tupleIdx++) {
        tuple.put(tupleIdx, aggFunctions[funcIdx].terminate(contexts[funcIdx]));
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
