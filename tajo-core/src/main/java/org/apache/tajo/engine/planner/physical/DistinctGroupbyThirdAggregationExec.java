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
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 *  This class aggregates the output of DistinctGroupbySecondAggregationExec.
 *
 */
public class DistinctGroupbyThirdAggregationExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(DistinctGroupbyThirdAggregationExec.class);
  private DistinctGroupbyNode plan;

  private boolean finished = false;

  private DistinctFinalAggregator[] aggregators;
  private DistinctFinalAggregator nonDistinctAggr;

  private int resultTupleLength;
  private int numGroupingColumns;

  private int[] resultTupleIndexes;

  private Tuple outTuple;
  private Tuple keyTuple;
  private Tuple prevKeyTuple = null;
  private Tuple prevTuple = null;

  public DistinctGroupbyThirdAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, SortExec sortExec)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), sortExec);
    this.plan = plan;
  }

  @Override
  public void init() throws IOException {
    super.init();

    numGroupingColumns = plan.getGroupingColumns().length;
    resultTupleLength = numGroupingColumns;
    keyTuple = new VTuple(numGroupingColumns);

    List<GroupbyNode> groupbyNodes = plan.getSubPlans();

    List<DistinctFinalAggregator> aggregatorList = new ArrayList<>();
    int inTupleIndex = 1 + numGroupingColumns;
    int outTupleIndex = numGroupingColumns;
    int distinctSeq = 0;

    for (GroupbyNode eachGroupby : groupbyNodes) {
      if (eachGroupby.isDistinct()) {
        aggregatorList.add(new DistinctFinalAggregator(distinctSeq, inTupleIndex, outTupleIndex, eachGroupby));
        distinctSeq++;

        Column[] distinctGroupingColumns = eachGroupby.getGroupingColumns();
        inTupleIndex += distinctGroupingColumns.length;
        outTupleIndex += eachGroupby.getAggFunctions().size();
      } else {
        nonDistinctAggr = new DistinctFinalAggregator(-1, inTupleIndex, outTupleIndex, eachGroupby);
        outTupleIndex += eachGroupby.getAggFunctions().size();
      }
      resultTupleLength += eachGroupby.getAggFunctions().size();
    }
    aggregators = aggregatorList.toArray(new DistinctFinalAggregator[aggregatorList.size()]);
    outTuple = new VTuple(resultTupleLength);

    // make output schema mapping index
    resultTupleIndexes = new int[outSchema.size()];
    Map<Column, Integer> groupbyResultTupleIndex = new HashMap<>();
    int resultTupleIndex = 0;
    for (Column eachColumn: plan.getGroupingColumns()) {
      groupbyResultTupleIndex.put(eachColumn, resultTupleIndex);
      resultTupleIndex++;
    }
    for (GroupbyNode eachGroupby : groupbyNodes) {
      Set<Column> groupingColumnSet = new HashSet<>();
      Collections.addAll(groupingColumnSet, eachGroupby.getGroupingColumns());
      for (Target eachTarget: eachGroupby.getTargets()) {
        if (!groupingColumnSet.contains(eachTarget.getNamedColumn())) {
          //aggr function
          groupbyResultTupleIndex.put(eachTarget.getNamedColumn(), resultTupleIndex);
          resultTupleIndex++;
        }
      }
    }

    int index = 0;
    for (Column eachOutputColumn: outSchema.getRootColumns()) {
      // If column is avg aggregation function, outschema's column type is float
      // but groupbyResultTupleIndex's column type is protobuf

      int matchedIndex = -1;
      for (Map.Entry<Column, Integer> entry: groupbyResultTupleIndex.entrySet()) {
        if (entry.getKey().getQualifiedName().equals(eachOutputColumn.getQualifiedName())) {
          matchedIndex = entry.getValue();
          break;
        }
      }
      if (matchedIndex < 0) {
        throw new IOException("Can't find proper output column mapping: " + eachOutputColumn);
      }
      resultTupleIndexes[matchedIndex] = index++;
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (finished) {
      return null;
    }

    while (!context.isStopped()) {
      Tuple tuple = child.next();
      // Last tuple
      if (tuple == null) {
        finished = true;

        if (prevTuple == null) {
          // Empty case
          if (numGroupingColumns == 0) {
            // No grouping column, return null tuple
            return makeEmptyTuple();
          } else {
            return null;
          }
        }

        for (int i = 0; i < numGroupingColumns; i++) {
          outTuple.put(resultTupleIndexes[i], prevTuple.asDatum(i + 1));
        }
        for (DistinctFinalAggregator eachAggr: aggregators) {
          eachAggr.terminate(outTuple);
        }

        return outTuple;
      }

      int distinctSeq = tuple.getInt2(0);
      Tuple keyTuple = getGroupingKeyTuple(tuple);

      // First tuple
      if (prevKeyTuple == null) {
        prevKeyTuple = new VTuple(keyTuple.getValues());
        prevTuple = new VTuple(tuple.getValues());

        aggregators[distinctSeq].merge(tuple);
        continue;
      }

      if (!prevKeyTuple.equals(keyTuple)) {
        // new grouping key
        for (int i = 0; i < numGroupingColumns; i++) {
          outTuple.put(resultTupleIndexes[i], prevTuple.asDatum(i + 1));
        }
        for (DistinctFinalAggregator eachAggr: aggregators) {
          eachAggr.terminate(outTuple);
        }

        prevKeyTuple.put(keyTuple.getValues());
        prevTuple.put(tuple.getValues());

        aggregators[distinctSeq].merge(tuple);
        return outTuple;
      } else {
        prevKeyTuple.put(keyTuple.getValues());
        prevTuple.put(tuple.getValues());
        aggregators[distinctSeq].merge(tuple);
      }
    }

    return null;
  }

  private Tuple makeEmptyTuple() {
    for (DistinctFinalAggregator eachAggr: aggregators) {
      eachAggr.terminateEmpty(outTuple);
    }

    return outTuple;
  }

  private Tuple getGroupingKeyTuple(Tuple tuple) {
    for (int i = 0; i < numGroupingColumns; i++) {
      keyTuple.put(i, tuple.asDatum(i + 1));
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

  class DistinctFinalAggregator {
    private FunctionContext[] functionContexts;
    private List<AggregationFunctionCallEval> aggrFunctions;
    private int seq;
    private int inTupleIndex;
    private int outTupleIndex;
    public DistinctFinalAggregator(int seq, int inTupleIndex, int outTupleIndex, GroupbyNode groupbyNode) {
      this.seq = seq;
      this.inTupleIndex = inTupleIndex;
      this.outTupleIndex = outTupleIndex;

      aggrFunctions = groupbyNode.getAggFunctions();
      if (aggrFunctions != null) {
        for (AggregationFunctionCallEval eachFunction: aggrFunctions) {
          eachFunction.bind(context.getEvalContext(), inSchema);
          eachFunction.setLastPhase();
        }
      }
      newFunctionContext();
    }

    private void newFunctionContext() {
      functionContexts = new FunctionContext[aggrFunctions.size()];
      for (int i = 0; i < aggrFunctions.size(); i++) {
        functionContexts[i] = aggrFunctions.get(i).newContext();
      }
    }

    public void merge(Tuple tuple) {
      for (int i = 0; i < aggrFunctions.size(); i++) {
        aggrFunctions.get(i).merge(functionContexts[i], tuple);
      }

      if (seq == 0 && nonDistinctAggr != null) {
        nonDistinctAggr.merge(tuple);
      }
    }

    public void terminate(Tuple resultTuple) {
      for (int i = 0; i < aggrFunctions.size(); i++) {
        resultTuple.put(resultTupleIndexes[outTupleIndex + i], aggrFunctions.get(i).terminate(functionContexts[i]));
      }
      newFunctionContext();

      if (seq == 0 && nonDistinctAggr != null) {
        nonDistinctAggr.terminate(resultTuple);
      }
    }

    public void terminateEmpty(Tuple resultTuple) {
      newFunctionContext();
      for (int i = 0; i < aggrFunctions.size(); i++) {
        resultTuple.put(resultTupleIndexes[outTupleIndex + i], aggrFunctions.get(i).terminate(functionContexts[i]));
      }
      if (seq == 0 && nonDistinctAggr != null) {
        nonDistinctAggr.terminateEmpty(resultTuple);
      }
    }
  }
}
