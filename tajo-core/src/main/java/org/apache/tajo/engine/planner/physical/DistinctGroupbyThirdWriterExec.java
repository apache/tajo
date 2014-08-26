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


import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.builtin.CountRows;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the sort-based aggregation operator for count distinct third stage
 */
public class DistinctGroupbyThirdWriterExec extends AggregationExec {
  private DistinctGroupbyNode distinctGroupbyPlan;
  private Tuple lastKey = null;
  private boolean finished = false;
  private FunctionContext contexts[];
  private Map<Tuple, Map<Integer, Datum>> hashTable;

  public DistinctGroupbyThirdWriterExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getGroupbyPlan(), child);

    distinctGroupbyPlan = plan;

    inSchema = plan.getInSchema();
    outSchema = plan.getOutSchema();

    contexts = new FunctionContext[plan.getAggFunctions() == null ? 0 : plan.getAggFunctions().length];
    hashTable = new HashMap<Tuple, Map<Integer, Datum>>(100000);
  }

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey;
    Tuple tuple = null;
    Tuple outputTuple = null;
    Tuple keyTuple;

    String[] aggFunctionNames = new String[aggFunctionsNum];
    for (int i = 0; i < aggFunctionsNum; i++) {
      aggFunctionNames[i] = aggFunctions[i].getFuncDesc().getFuncClass().getCanonicalName();
    }

    while(!context.isStopped() && (tuple = child.next()) != null) {
      // get a key tuple
      currentKey = new VTuple(groupingKeyIds.length);
      for(int i = 0; i < groupingKeyIds.length; i++) {
        currentKey.put(i, tuple.get(groupingKeyIds[i]));
      }

      /** Aggregation State */
      if (lastKey == null || lastKey.equals(currentKey)) {
        if (lastKey == null) {
          for(int i = 0; i < aggFunctionsNum; i++) {
            contexts[i] = aggFunctions[i].newContext();

            // Merge when aggregator doesn't receive NullDatum
              Datum param = getParam(aggFunctions[i], tuple);
              setCountValues(currentKey, Integer.valueOf(aggFunctions[i].hashCode()), param);
          }
          lastKey = currentKey;
        } else {
          // aggregate
          for (int i = 0; i < aggFunctionsNum; i++) {
            Datum param = getParam(aggFunctions[i], tuple);
            setCountValues(currentKey, Integer.valueOf(aggFunctions[i].hashCode()), param);
          }
        }
      } else { /** Finalization State */
        // finalize aggregate and return
        outputTuple = new VTuple(outSchema.size());
        int tupleIdx = 0;

        for(; tupleIdx < groupingKeyNum; tupleIdx++) {
          outputTuple.put(tupleIdx, lastKey.get(tupleIdx));
        }
        for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
          outputTuple.put(tupleIdx, aggFunctions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
        }

        for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
          contexts[evalIdx] = aggFunctions[evalIdx].newContext();
          Datum param = getParam(aggFunctions[evalIdx], tuple);
          setCountValues(currentKey, Integer.valueOf(aggFunctions[evalIdx].hashCode()), param);
        }

        lastKey = currentKey;
        setOutputTuple(outputTuple);
        return outputTuple;
      }
    } // while loop

    if (tuple == null && lastKey == null) {
      finished = true;
      return null;
    }

    if (!finished) {
      outputTuple = new VTuple(outSchema.size());

      int tupleIdx = 0;
      for(; tupleIdx < groupingKeyNum; tupleIdx++) {
        outputTuple.put(tupleIdx, lastKey.get(tupleIdx));
      }

      setOutputTuple(outputTuple);
      finished = true;
    }

    return outputTuple;
  }

  private void setOutputTuple(Tuple outputTuple) {
    Tuple keyTuple = new VTuple(groupingKeyIds.length);
    for(int i = 0; i < groupingKeyIds.length; i++) {
      keyTuple.put(i, outputTuple.get(groupingKeyIds[i]));
    }

    for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
      int index = getColumnIndex(aggFunctions[evalIdx]);
      if (index > -1) {
        if (hashTable.get(keyTuple) != null) {
          outputTuple.put(index, hashTable.get(keyTuple).get(Integer.valueOf(aggFunctions[evalIdx].hashCode())));
        }
      }
    }
  }

  private void setCountValues(Tuple keyTuple, Integer hashCode, Datum param) {
    if (hashTable.get(keyTuple) == null) {
      Map<Integer, Datum> sum = TUtil.newHashMap();
      sum.put(hashCode, param);
      hashTable.put(keyTuple, sum);
    } else {
      if (hashTable.get(keyTuple).get(hashCode) == null) {
        hashTable.get(keyTuple).put(hashCode, param);
      } else {
        hashTable.get(keyTuple).get(hashCode).plus(param);
      }
    }
  }

  private int getColumnIndex(AggregationFunctionCallEval aggFunction) {
    int retValue = -1;

    for (int i = 0; i < inSchema.getColumns().size(); i++) {
      if (inSchema.getColumn(i).getQualifiedName().equals(aggFunction.getAlias())) {
        retValue = i;
        break;
      }
    }
    return retValue;
  }

  private Datum getParam(AggregationFunctionCallEval aggFunction, Tuple tuple) {
    int index = getColumnIndex(aggFunction);
    if (index > -1) {
      return tuple.get(index);
    } else {
      return DatumFactory.createNullDatum();
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    finished = false;
  }
}

