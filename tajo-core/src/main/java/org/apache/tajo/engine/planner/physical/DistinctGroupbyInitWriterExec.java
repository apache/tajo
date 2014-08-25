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
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class DistinctGroupbyInitWriterExec extends PhysicalExec {
  private DistinctGroupbyNode plan;
  private PhysicalExec child;
  private float progress;
  private Map<Tuple, Tuple> hashTable;
  private boolean computed = false;
  private Iterator<Map.Entry<Tuple, Tuple>> iterator = null;

  protected final int groupingKeyNum;
  protected int groupingKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval aggFunctions[];

  public DistinctGroupbyInitWriterExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
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

    hashTable = new HashMap<Tuple, Tuple>(100000);
    aggFunctions = plan.getAggFunctions();
    aggFunctionsNum = aggFunctions.length;
  }

  @Override
  public Tuple next() throws IOException {
    if(!computed) {
      compute();
      iterator = hashTable.entrySet().iterator();
      computed = true;
    }

    if (iterator.hasNext()) {
      Map.Entry<Tuple, Tuple> entry = iterator.next();
      return entry.getValue();
    } else {
      return null;
    }
  }


  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    Tuple outputTuple;

    while((tuple = child.next()) != null && !context.isStopped()) {
      keyTuple = new VTuple(groupingKeyIds.length);
      // build one key tuple
      for(int i = 0; i < groupingKeyIds.length; i++) {
        keyTuple.put(i, tuple.get(groupingKeyIds[i]));
      }

      outputTuple = tuple;
      hashTable.put(outputTuple, outputTuple);

      for (int i = 0; i < aggFunctionsNum; i++) {
        outputTuple = new VTuple(outColumnNum);
        int tupleIdx = 0;
        for (; tupleIdx < groupingKeyNum; tupleIdx++) {
          outputTuple.put(tupleIdx, keyTuple.get(tupleIdx));
        }

        for (int funcIdx = 0; funcIdx < aggFunctionsNum; funcIdx++, tupleIdx++) {
          if (i == funcIdx) {
            outputTuple.put(tupleIdx, tuple.get(tupleIdx));
          } else {
            outputTuple.put(tupleIdx, DatumFactory.createDistinctNullDatum());
          }
        }
        hashTable.put(outputTuple, outputTuple);
      }
    }
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
