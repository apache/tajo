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
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is the sort-based aggregation operator.
 *
 * <h3>Implementation</h3>
 * Sort Aggregation has two states while running.
 *
 * <h4>Aggregate state</h4>
 * If lastkey is null or lastkey is equivalent to the current key, sort aggregation is changed to this state.
 * In this state, this operator aggregates measure values via aggregation functions.
 *
 * <h4>Finalize state</h4>
 * If currentKey is different from the last key, it computes final aggregation results, and then
 * it makes an output tuple.
 */
public class SortAggregateExec extends AggregationExec {
  private final int groupingKeyIds[];
  private Tuple lastKey = null;
  private final Tuple currentKey;
  private final Tuple outTuple;
  private boolean finished = false;
  private FunctionContext contexts[];

  public SortAggregateExec(TaskAttemptContext context, GroupbyNode plan, PhysicalExec child) throws IOException {
    super(context, plan, child);
    contexts = new FunctionContext[plan.getAggFunctions() == null ? 0 : plan.getAggFunctions().size()];

    final Column [] keyColumns = plan.getGroupingColumns();
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
    currentKey = new VTuple(groupingKeyNum);
    outTuple = new VTuple(outSchema.size());
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = null;

    while(!context.isStopped() && (tuple = child.next()) != null) {
      // get a key tuple
      for(int i = 0; i < groupingKeyIds.length; i++) {
        currentKey.put(i, tuple.asDatum(groupingKeyIds[i]));
      }

      /** Aggregation State */
      if (lastKey == null || lastKey.equals(currentKey)) {
        if (lastKey == null) {
          for(int i = 0; i < aggFunctionsNum; i++) {
            contexts[i] = aggFunctions.get(i).newContext();

            // Merge when aggregator doesn't receive NullDatum
            if (!(groupingKeyNum == 0 && aggFunctionsNum == tuple.size()
                && tuple.isBlankOrNull(i))) {
              aggFunctions.get(i).merge(contexts[i], tuple);
            }
          }
          lastKey = new VTuple(currentKey.getValues());
        } else {
          // aggregate
          for (int i = 0; i < aggFunctionsNum; i++) {
            aggFunctions.get(i).merge(contexts[i], tuple);
          }
        }

      } else { /** Finalization State */
        // finalize aggregate and return
        int tupleIdx = 0;

        for(; tupleIdx < groupingKeyNum; tupleIdx++) {
          outTuple.put(tupleIdx, lastKey.asDatum(tupleIdx));
        }
        for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
          outTuple.put(tupleIdx, aggFunctions.get(aggFuncIdx).terminate(contexts[aggFuncIdx]));
        }

        for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
          contexts[evalIdx] = aggFunctions.get(evalIdx).newContext();
          aggFunctions.get(evalIdx).merge(contexts[evalIdx], tuple);
        }

        lastKey.put(currentKey.getValues());
        return outTuple;
      }
    } // while loop

    if (tuple == null && lastKey == null) {
      finished = true;
      return null;
    }
    if (!finished) {
      int tupleIdx = 0;
      for(; tupleIdx < groupingKeyNum; tupleIdx++) {
        outTuple.put(tupleIdx, lastKey.asDatum(tupleIdx));
      }
      for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
        outTuple.put(tupleIdx, aggFunctions.get(aggFuncIdx).terminate(contexts[aggFuncIdx]));
      }
      finished = true;
      return outTuple;
    }
    return null;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    finished = false;
  }
}
