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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.DistinctNullDatum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

// 첫번째 단계의 결과 파일에 Distinct를 적용한 결과를 만든다.
// 두번째 단계에서 distinct를 하기 위해 첫단계에서 pre aggregation 해주니 데이터가 많이 줄게됩니다.
// 그리고 두번째 단계에서 키가 많아져서 분산도 잘되구요

// 세번째
// distinct 적용한 결과가 두번째에서 distinct 적용한 결과가 grouping key와 하나의 distinct aggregation일 텐데요. 이걸 튜플로 이어 붙이고
public class DistinctGroupbyIntermediateAggregationExec extends PhysicalExec {
  private DistinctGroupbyNode plan;
  private PhysicalExec child;
  private float progress;
  private Tuple tuple = null;
  private Map<Tuple, FunctionContext[]> hashTable;
  private Map<Tuple, Tuple> allTuples;
  private boolean computed = false;
  private Iterator<Map.Entry<Tuple, FunctionContext []>> iterator = null;
  private byte[] distinctNullBytes;

  //--------------------------------------------------------------------------------------
  // Original raw data
  //--------------------------------------------------------------------------------------
  // col1 | col2 | col3 |
  //--------------------------------------------------------------------------------------
  // 1| 10 | AA |
  // 2 | 15 | BB |
  // 2 | 20 | BB |
  // 3 | 5 | CC |
  // 4 | 5 | DD |
  // 5 | 30 | EE |
  //--------------------------------------------------------------------------------------


  //--------------------------------------------------------------------------------------
  // Input data: It had created on first execution block.
  //--------------------------------------------------------------------------------------
  // col1 | col2 | col3 |
  //--------------------------------------------------------------------------------------
  // 1| 10 | AA |
  // 1| 10 | DISTINCT_NULL |
  // 1| DISTINCT_NULL | AA |
  // 2 | 15 | BB |
  // 2 | 15 | DISTINCT_NULL |
  // 2 | DISTINCT_NULL | BB |
  // 2 | 20 | BB |
  // 2 | 20 | DISTINCT_NULL |
  // 2 | DISTINCT_NULL | BB |
  // 3 | 5 | CC |
  // 3 | 5 | DISTINCT_NULL |
  // 3 | DISTINCT_NULL | CC |
  // 4 | 5 | DD |
  // 4 | 5 | DISTINCT_NULL |
  // 4 | DISTINCT_NULL | DD |
  // 5 | 30 | EE |
  // 5 | 30 | DISTINCT_NULL |
  // 5 | DISTINCT_NULL | EE |
  //--------------------------------------------------------------------------------------


  //--------------------------------------------------------------------------------------
  // Output data
  //--------------------------------------------------------------------------------------
  // col1 | col2 | col3 |
  //--------------------------------------------------------------------------------------
  // 1| 1 | DISTINCT_NULL |
  // 1| DISTINCT_NULL | 1 |
  // 2 | 2 | DISTINCT_NULL |
  // 2 | DISTINCT_NULL | 1 |
  // 3 | 1 | DISTINCT_NULL |
  // 3 | DISTINCT_NULL | 1 |
  // 4 | 1 | DISTINCT_NULL |
  // 4 | DISTINCT_NULL | 1 |
  // 5 | 1 | DISTINCT_NULL |
  // 5 | DISTINCT_NULL | 1 |
  //--------------------------------------------------------------------------------------

  protected final int groupingKeyNum;
  protected int groupingKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval aggFunctions[];

  public DistinctGroupbyIntermediateAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

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

  @Override
  public void init() throws IOException {
    progress = 0.0f;
    if (child != null) {
      child.init();
    }
  }

  @Override
  public void rescan() throws IOException {
    progress = 0.0f;
    if (child != null) {
      child.rescan();
    }
    iterator = hashTable.entrySet().iterator();
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
    if (child != null) {
      child.close();
      child = null;
    }

    hashTable.clear();
    hashTable = null;
    iterator = null;
  }

  @Override
  public float getProgress() {
    if (child != null) {
      return child.getProgress();
    } else {
      return progress;
    }
  }

}
