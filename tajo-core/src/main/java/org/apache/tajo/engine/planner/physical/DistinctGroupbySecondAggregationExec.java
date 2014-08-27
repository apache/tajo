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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 * This is the hash-based aggregation operator for count distinct second stage
 */
public class DistinctGroupbySecondAggregationExec extends UnaryPhysicalExec {
  private DistinctGroupbyNode plan;
  private PhysicalExec child;
  private Tuple tuple = null;
  private Map<Tuple, FunctionContext[]> hashTable;
  private Map<Tuple, Int8Datum> countRows;
  private boolean computed = false;
  private Iterator<Map.Entry<Tuple, FunctionContext []>> iterator = null;

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
    countRows = new HashMap<Tuple, Int8Datum>(100000);

    aggFunctions = plan.getAggFunctions();
    aggFunctionsNum = aggFunctions.length;

    this.tuple = new VTuple(plan.getOutSchema().size());
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
        if (!aggFunctions[funcIdx].isDistinct() && aggFunctions[funcIdx].getName().equals("count")) {
          tuple.put(tupleIdx, countRows.get(keyTuple));
        } else {
          tuple.put(tupleIdx, aggFunctions[funcIdx].terminate(contexts[funcIdx]));
        }
      }

      return tuple;
    } else {
      return null;
    }
  }


  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    boolean isOriginalTuple = false;

    while((tuple = child.next()) != null && !context.isStopped()) {
      isOriginalTuple = true;

      // build one key tuple
      keyTuple = new VTuple(groupingKeyIds.length);
      for(int i = 0; i < groupingKeyIds.length; i++) {
        keyTuple.put(i, tuple.get(groupingKeyIds[i]));
      }

      FunctionContext [] contexts = hashTable.get(keyTuple);
      if (contexts == null) {
        contexts = new FunctionContext[aggFunctionsNum];
      }

      // Find original raw data. It has not NullDatum at aggregation column.
      // Because DistinctGroupbyFirstAggregationExec just set NullDatum at at aggregation column.
      for(int i = 0; i < aggFunctions.length; i++) {
        if (contexts[i] == null) {
          contexts[i] = aggFunctions[i].newContext();
        }

        Tuple param = getAggregationParams(aggFunctions[i].getArgs(), inSchema, tuple);
        if (param.size() > 0 && param.get(0).isNull()) {
          isOriginalTuple = false;
        }
      }

      // Merge aggregation context in two cases. First case is that current tuple is origin and aggregation function
      // is not count function. And second case is that aggregation function is distinct function.
      for(int i = 0; i < aggFunctions.length; i++) {
        if ((isOriginalTuple && !aggFunctions[i].getName().equals("count"))
            || (aggFunctions[i].isDistinct())) {
          aggFunctions[i].merge(contexts[i], inSchema, tuple);
        }
      }

      hashTable.put(keyTuple, contexts);

      // If original data was found, we must add it to global count rows.
      if (isOriginalTuple) {
        if (countRows.get(keyTuple) == null) {
          countRows.put(keyTuple, DatumFactory.createInt8(1l));
        } else {
          Int8Datum rows = countRows.get(keyTuple);
          countRows.put(keyTuple, (Int8Datum)rows.plus(DatumFactory.createInt8(1l)));
        }
      }
    } //end while loop


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

  public Tuple getAggregationParams(EvalNode[] argEvals, Schema schema, Tuple tuple) {
    Tuple params = new VTuple(argEvals.length);

    if (argEvals != null) {
      for (int i = 0; i < argEvals.length; i++) {
        params.put(i, argEvals[i].eval(schema, tuple));
      }
    }

    return params;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
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
