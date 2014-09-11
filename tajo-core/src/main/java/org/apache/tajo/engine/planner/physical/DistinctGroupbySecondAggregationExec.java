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
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistinctGroupbySecondAggregationExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(DistinctGroupbySecondAggregationExec.class);
  private DistinctGroupbyNode plan;
  private PhysicalExec child;

  private boolean finished = false;

  private int numGroupingColumns;
  private int[][] distinctKeyIndexes;
  private FunctionContext[] nonDistinctAggrContexts;
  private AggregationFunctionCallEval[] nonDistinctAggrFunctions;
  private int nonDistinctAggrTupleStartIndex = -1;

  public DistinctGroupbySecondAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, SortExec sortExec)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), sortExec);
    this.plan = plan;
    this.child = sortExec;
  }

  @Override
  public void init() throws IOException {
    this.child.init();

    numGroupingColumns = plan.getGroupingColumns().length;

    List<GroupbyNode> groupbyNodes = plan.getGroupByNodes();

    // Finding distinct group by column index.
    Set<Integer> groupingKeyIndexSet = new HashSet<Integer>();
    for (Column col: plan.getGroupingColumns()) {
      int keyIndex;
      if (col.hasQualifier()) {
        keyIndex = inSchema.getColumnId(col.getQualifiedName());
      } else {
        keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
      }
      groupingKeyIndexSet.add(keyIndex);
    }

    int numDistinct = 0;
    for (GroupbyNode eachGroupby : groupbyNodes) {
      if (eachGroupby.isDistinct()) {
        numDistinct++;
      } else {
        nonDistinctAggrFunctions = eachGroupby.getAggFunctions();
        if (nonDistinctAggrFunctions != null) {
          for (AggregationFunctionCallEval eachFunction: nonDistinctAggrFunctions) {
            eachFunction.setIntermediatePhase();
          }
          nonDistinctAggrContexts = new FunctionContext[nonDistinctAggrFunctions.length];
        }
      }
    }

    int index = 0;
    distinctKeyIndexes = new int[numDistinct][];
    for (GroupbyNode eachGroupby : groupbyNodes) {
      if (eachGroupby.isDistinct()) {
        List<Integer> distinctGroupingKeyIndex = new ArrayList<Integer>();
        Column[] distinctGroupingColumns = eachGroupby.getGroupingColumns();
        for (int idx = 0; idx < distinctGroupingColumns.length; idx++) {
          Column col = distinctGroupingColumns[idx];
          int keyIndex;
          if (col.hasQualifier()) {
            keyIndex = inSchema.getColumnId(col.getQualifiedName());
          } else {
            keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
          }
          if (!groupingKeyIndexSet.contains(keyIndex)) {
            distinctGroupingKeyIndex.add(keyIndex);
          }
        }
        int i = 0;
        distinctKeyIndexes[index] = new int[distinctGroupingKeyIndex.size()];
        for (int eachIdx : distinctGroupingKeyIndex) {
          distinctKeyIndexes[index][i++] = eachIdx;
        }
        index++;
      }
    }
    if (nonDistinctAggrFunctions != null) {
      nonDistinctAggrTupleStartIndex = inSchema.size() - nonDistinctAggrFunctions.length;
    }
  }

  Tuple prevKeyTuple = null;
  Tuple prevTuple = null;
  int prevSeq = -1;

  @Override
  public Tuple next() throws IOException {
    if (finished) {
      return null;
    }

    Tuple result = null;
    while (!context.isStopped()) {
      Tuple childTuple = child.next();
      if (childTuple == null) {
        finished = true;

        if (prevTuple == null) {
          // Empty case
          return null;
        }
        if (prevSeq == 0 && nonDistinctAggrFunctions != null) {
          terminatedNonDistinctAggr(prevTuple);
        }
        result = prevTuple;
        break;
      }

      Tuple tuple = null;
      try {
        tuple = childTuple.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e.getMessage(), e);
      }

      int distinctSeq = tuple.get(0).asInt2();
      Tuple keyTuple = getKeyTuple(distinctSeq, tuple);

      if (prevKeyTuple == null) {
        // First
        if (distinctSeq == 0 && nonDistinctAggrFunctions != null) {
          initNonDistinctAggrContext();
          mergeNonDistinctAggr(tuple);
        }
        prevKeyTuple = keyTuple;
        prevTuple = tuple;
        prevSeq = distinctSeq;
        continue;
      }

      if (!prevKeyTuple.equals(keyTuple)) {
        // new grouping key
        if (prevSeq == 0 && nonDistinctAggrFunctions != null) {
          terminatedNonDistinctAggr(prevTuple);
        }
        result = prevTuple;

        prevKeyTuple = keyTuple;
        prevTuple = tuple;
        prevSeq = distinctSeq;

        if (distinctSeq == 0 && nonDistinctAggrFunctions != null) {
          initNonDistinctAggrContext();
          mergeNonDistinctAggr(tuple);
        }
        break;
      } else {
        prevKeyTuple = keyTuple;
        prevTuple = tuple;
        prevSeq = distinctSeq;
        if (distinctSeq == 0 && nonDistinctAggrFunctions != null) {
          mergeNonDistinctAggr(tuple);
        }
      }
    }

    return result;
  }

  private void initNonDistinctAggrContext() {
    if (nonDistinctAggrFunctions != null) {
      nonDistinctAggrContexts = new FunctionContext[nonDistinctAggrFunctions.length];
      for (int i = 0; i < nonDistinctAggrFunctions.length; i++) {
        nonDistinctAggrContexts[i] = nonDistinctAggrFunctions[i].newContext();
      }
    }
  }

  private void mergeNonDistinctAggr(Tuple tuple) {
    if (nonDistinctAggrFunctions == null) {
      return;
    }
    for (int i = 0; i < nonDistinctAggrFunctions.length; i++) {
      nonDistinctAggrFunctions[i].merge(nonDistinctAggrContexts[i], inSchema, tuple);
    }
  }

  private void terminatedNonDistinctAggr(Tuple tuple) {
    if (nonDistinctAggrFunctions == null) {
      return;
    }
    for (int i = 0; i < nonDistinctAggrFunctions.length; i++) {
      tuple.put(nonDistinctAggrTupleStartIndex + i, nonDistinctAggrFunctions[i].terminate(nonDistinctAggrContexts[i]));
    }
  }

  private Tuple getKeyTuple(int distinctSeq, Tuple tuple) {
    int[] columnIndexes = distinctKeyIndexes[distinctSeq];

    Tuple keyTuple = new VTuple(numGroupingColumns + columnIndexes.length + 1);
    keyTuple.put(0, tuple.get(0));
    for (int i = 0; i < numGroupingColumns; i++) {
      keyTuple.put(i + 1, tuple.get(i + 1));
    }
    for (int i = 0; i < columnIndexes.length; i++) {
      keyTuple.put(i + 1 + numGroupingColumns, tuple.get(columnIndexes[i]));
    }

    return keyTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    prevKeyTuple = null;
    prevTuple = null;
    finished = false;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
