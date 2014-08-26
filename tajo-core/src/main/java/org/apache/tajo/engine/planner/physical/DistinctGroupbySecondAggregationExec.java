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
import org.apache.tajo.datum.DistinctNullDatum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class DistinctGroupbySecondAggregationExec extends UnaryPhysicalExec {
  private DistinctGroupbyNode plan;
  private PhysicalExec child;
  private float progress;
  private Tuple tuple = null;
  private Map<Tuple, FunctionContext[]> hashTable;
  private Map<Tuple, Tuple> allTuples;
  private boolean computed = false;
  private Iterator<Map.Entry<Tuple, FunctionContext []>> iterator = null;
  private byte[] distinctNullBytes;

  protected final int groupingKeyNum;
  protected int groupingKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval aggFunctions[];

  public DistinctGroupbySecondAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), subOp);

    this.plan = plan;

    this.child = subOp;
    this.child.init();

    final Column [] keyColumns = plan.getGroupingColumns();
    groupingKeyNum = keyColumns.length;
    groupingKeyIds = new int[groupingKeyNum];
    Column col;
    for (int idx = 0; idx < plan.getGroupingColumns().length; idx++) {
      col = keyColumns[idx];
      if (col.hasQualifier()) {
        groupingKeyIds[idx] = inSchema.getColumnId(col.getQualifiedName());
      } else {
        groupingKeyIds[idx] = inSchema.getColumnIdByName(col.getSimpleName());
      }
    }

    hashTable = new HashMap<Tuple, FunctionContext []>(100000);
    allTuples = new HashMap<Tuple, Tuple>(100000);

    this.tuple = new VTuple(plan.getOutSchema().size());

    aggFunctions = plan.getAggFunctions();
    aggFunctionsNum = aggFunctions.length;

    distinctNullBytes = DistinctNullDatum.get().asTextBytes();
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
      Map.Entry<Tuple, FunctionContext []> entry = iterator.next();
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
          if ((aggFunctions[i].isDistinct() && hasDistinctNull(tuple))
              || (!aggFunctions[i].isDistinct() && !hasDistinctNull(tuple))) {
            aggFunctions[i].mergeOnMultiStages(contexts[i], inSchema, tuple);
          }
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for(int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions[i].newContext();

          if ((aggFunctions[i].isDistinct() && hasDistinctNull(tuple))
              || (!aggFunctions[i].isDistinct() && !hasDistinctNull(tuple))) {
            aggFunctions[i].mergeOnMultiStages(contexts[i], inSchema, tuple);
          }
        }

        hashTable.put(keyTuple, contexts);
      }
    }

    // If DistinctGroupbyIntermediateAggregationExec received NullDatum and didn't has any grouping keys,
    // it should return primitive values for NullLDatum.
    if (groupingKeyNum == 0 && aggFunctionsNum > 0 && hashTable.entrySet().size() == 0) {
      FunctionContext[] contexts = new FunctionContext[aggFunctionsNum];
      for(int i = 0; i < aggFunctionsNum; i++) {
        contexts[i] = aggFunctions[i].newContext();
      }
      hashTable.put(null, contexts);
    }
  }

  private boolean hasDistinctNull(Tuple tuple) {
    boolean retValue = false;

    for (int i = 0; i < tuple.size(); i++) {
      if (Bytes.compareTo(tuple.get(i).asTextBytes(), distinctNullBytes) == 0) {
        retValue = true;
        break;
      }
    }

    return retValue;
  }

//  @Override
//  public void init() throws IOException {
//    progress = 0.0f;
//    if (child != null) {
//      child.init();
//    }
//  }

  @Override
  public void rescan() throws IOException {
//    progress = 0.0f;
//    if (child != null) {
//      child.rescan();
//    }
    super.rescan();
    iterator = hashTable.entrySet().iterator();
  }

  @Override
  public void close() throws IOException {
//    progress = 1.0f;
//    if (child != null) {
//      child.close();
//      child = null;
//    }
    super.close();

    hashTable.clear();
    hashTable = null;
    iterator = null;
  }
//
//  @Override
//  public float getProgress() {
//    if (child != null) {
//      return child.getProgress();
//    } else {
//      return progress;
//    }
//  }

}
