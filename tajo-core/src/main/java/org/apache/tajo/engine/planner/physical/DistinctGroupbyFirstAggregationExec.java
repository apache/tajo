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
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Int2Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class DistinctGroupbyFirstAggregationExec extends PhysicalExec {
  private static Log LOG = LogFactory.getLog(DistinctGroupbyFirstAggregationExec.class);

  private DistinctGroupbyNode plan;
  private boolean finished = false;
  private boolean preparedData = false;
  private PhysicalExec child;

  private long totalNumRows;
  private int fetchedRows;
  private float progress;

  private int[] groupingKeyIndexes;
  private NonDistinctHashAggregator nonDistinctHashAggregator;
  private DistinctHashAggregator[] distinctAggregators;

  private int resultTupleLength;

  public DistinctGroupbyFirstAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.child = subOp;
    this.plan = plan;
  }

  @Override
  public void init() throws IOException {
    super.init();
    child.init();

    // finding grouping column index
    Column[] groupingColumns = plan.getGroupingColumns();
    groupingKeyIndexes = new int[groupingColumns.length];

    int index = 0;
    for (Column col: groupingColumns) {
      int keyIndex;
      if (col.hasQualifier()) {
        keyIndex = inSchema.getColumnId(col.getQualifiedName());
      } else {
        keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
      }
      groupingKeyIndexes[index++] = keyIndex;
    }
    resultTupleLength = groupingKeyIndexes.length + 1;  //1 is Sequence Datum which indicates sequence of DistinctNode.

    List<GroupbyNode> groupbyNodes = plan.getGroupByNodes();

    List<DistinctHashAggregator> distinctAggrList = new ArrayList<DistinctHashAggregator>();
    int distinctSeq = 0;
    for (GroupbyNode eachGroupby: groupbyNodes) {
      if (eachGroupby.isDistinct()) {
        DistinctHashAggregator aggregator = new DistinctHashAggregator(eachGroupby);
        aggregator.setNodeSequence(distinctSeq++);
        distinctAggrList.add(aggregator);
        resultTupleLength += aggregator.getTupleLength();
      } else {
        nonDistinctHashAggregator = new NonDistinctHashAggregator(eachGroupby);
        resultTupleLength += nonDistinctHashAggregator.getTupleLength();
      }
    }
    distinctAggregators = distinctAggrList.toArray(new DistinctHashAggregator[]{});
  }

  private int currentAggregatorIndex = 0;

  @Override
  public Tuple next() throws IOException {
    if (!preparedData) {
      prepareInputData();
    }

    int prevIndex = currentAggregatorIndex;
    while (!context.isStopped()) {
      DistinctHashAggregator aggregator = distinctAggregators[currentAggregatorIndex];
      Tuple result = aggregator.next();
      if (result != null) {
        return result;
      }
      currentAggregatorIndex++;
      currentAggregatorIndex = currentAggregatorIndex % distinctAggregators.length;
      if (currentAggregatorIndex == prevIndex) {
        finished = true;
        return null;
      }
    }

    return null;
  }

  private void prepareInputData() throws IOException {
    Tuple tuple = null;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      Tuple groupingKey = new VTuple(groupingKeyIndexes.length);
      for (int i = 0; i < groupingKeyIndexes.length; i++) {
        groupingKey.put(i, tuple.get(groupingKeyIndexes[i]));
      }
      for (int i = 0; i < distinctAggregators.length; i++) {
        distinctAggregators[i].compute(groupingKey, tuple);
      }
      if (nonDistinctHashAggregator != null) {
        nonDistinctHashAggregator.compute(groupingKey, tuple);
      }
    }
    for (int i = 0; i < distinctAggregators.length; i++) {
      distinctAggregators[i].rescan();
    }

    totalNumRows = distinctAggregators[0].distinctAggrDatas.size();
    preparedData = true;
  }

  @Override
  public void close() throws IOException {
    child.close();
  }

  @Override
  public TableStats getInputStats() {
    if (child != null) {
      return child.getInputStats();
    } else {
      return null;
    }
  }

  @Override
  public float getProgress() {
    if (finished) {
      return progress;
    } else {
      if (totalNumRows > 0) {
        return progress + ((float)fetchedRows / (float)totalNumRows) * 0.5f;
      } else {
        return progress;
      }
    }
  }

  @Override
  public void rescan() {
    finished = false;
    currentAggregatorIndex = 0;
    for (int i = 0; i < distinctAggregators.length; i++) {
      distinctAggregators[i].rescan();
    }
  }

  class NonDistinctHashAggregator {
    private GroupbyNode groupbyNode;
    private int aggFunctionsNum;
    private final AggregationFunctionCallEval aggFunctions[];

    // GroupingKey -> FunctionContext[]
    private Map<Tuple, FunctionContext[]> nonDistinctAggrDatas;
    private int tupleLength;

    private Tuple dummyTuple;
    private NonDistinctHashAggregator(GroupbyNode groupbyNode) throws IOException {
      this.groupbyNode = groupbyNode;

      nonDistinctAggrDatas = new HashMap<Tuple, FunctionContext[]>();

      if (groupbyNode.hasAggFunctions()) {
        aggFunctions = groupbyNode.getAggFunctions();
        aggFunctionsNum = aggFunctions.length;
        for (AggregationFunctionCallEval eachFunction: aggFunctions) {
          eachFunction.setFirstPhase();
        }
      } else {
        aggFunctions = new AggregationFunctionCallEval[0];
        aggFunctionsNum = 0;
      }

      dummyTuple = new VTuple(aggFunctionsNum);
      for (int i = 0; i < aggFunctionsNum; i++) {
        dummyTuple.put(i, NullDatum.get());
      }
      tupleLength = aggFunctionsNum;
    }

    public void compute(Tuple groupingKeyTuple, Tuple tuple) {
      FunctionContext[] contexts = nonDistinctAggrDatas.get(groupingKeyTuple);
      if (contexts != null) {
        for (int i = 0; i < aggFunctions.length; i++) {
          aggFunctions[i].merge(contexts[i], inSchema, tuple);
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for (int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions[i].newContext();
          aggFunctions[i].merge(contexts[i], inSchema, tuple);
        }
        nonDistinctAggrDatas.put(groupingKeyTuple, contexts);
      }
    }

    public Tuple aggregate(Tuple groupingKey) {
      FunctionContext[] contexts = nonDistinctAggrDatas.get(groupingKey);
      if (contexts == null) {
        return null;
      }
      Tuple tuple = new VTuple(aggFunctionsNum);

      for (int i = 0; i < aggFunctionsNum; i++) {
        tuple.put(i, aggFunctions[i].terminate(contexts[i]));
      }

      return tuple;
    }

    public int getTupleLength() {
      return tupleLength;
    }

    public Tuple getDummyTuple() {
      return dummyTuple;
    }
  }

  class DistinctHashAggregator {
    private GroupbyNode groupbyNode;

    // GroupingKey -> DistinctKey
    private Map<Tuple, Set<Tuple>> distinctAggrDatas;
    private Iterator<Entry<Tuple, Set<Tuple>>> iterator = null;

    private int nodeSequence;
    private Int2Datum nodeSequenceDatum;

    private int[] distinctKeyIndexes;

    private int tupleLength;
    private Tuple dummyTuple;
    private boolean aggregatorFinished = false;

    public DistinctHashAggregator(GroupbyNode groupbyNode) throws IOException {
      this.groupbyNode = groupbyNode;

      Set<Integer> groupingKeyIndexSet = new HashSet<Integer>();
      for (Integer eachIndex: groupingKeyIndexes) {
        groupingKeyIndexSet.add(eachIndex);
      }

      List<Integer> distinctGroupingKeyIndexSet = new ArrayList<Integer>();
      Column[] groupingColumns = groupbyNode.getGroupingColumns();
      for (int idx = 0; idx < groupingColumns.length; idx++) {
        Column col = groupingColumns[idx];
        int keyIndex;
        if (col.hasQualifier()) {
          keyIndex = inSchema.getColumnId(col.getQualifiedName());
        } else {
          keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
        }
        if (!groupingKeyIndexSet.contains(keyIndex)) {
          distinctGroupingKeyIndexSet.add(keyIndex);
        }
      }
      int index = 0;
      this.distinctKeyIndexes = new int[distinctGroupingKeyIndexSet.size()];
      this.dummyTuple = new VTuple(distinctGroupingKeyIndexSet.size());
      for (Integer eachId : distinctGroupingKeyIndexSet) {
        this.dummyTuple.put(index, NullDatum.get());
        this.distinctKeyIndexes[index++] = eachId;
      }

      this.distinctAggrDatas = new HashMap<Tuple, Set<Tuple>>();
      this.tupleLength = distinctKeyIndexes.length;
    }

    public void setNodeSequence(int nodeSequence) {
      this.nodeSequence = nodeSequence;
      this.nodeSequenceDatum = new Int2Datum((short)nodeSequence);
    }

    public int getTupleLength() {
      return tupleLength;
    }

    public void compute(Tuple groupingKey, Tuple tuple) throws IOException {
      Tuple distinctKeyTuple = new VTuple(distinctKeyIndexes.length);
      for (int i = 0; i < distinctKeyIndexes.length; i++) {
        distinctKeyTuple.put(i, tuple.get(distinctKeyIndexes[i]));
      }

      Set<Tuple> distinctEntry = distinctAggrDatas.get(groupingKey);
      if (distinctEntry == null) {
        distinctEntry = new HashSet<Tuple>();
        distinctAggrDatas.put(groupingKey, distinctEntry);
      }
      distinctEntry.add(distinctKeyTuple);
    }

    public void rescan() {
      iterator = distinctAggrDatas.entrySet().iterator();
      currentGroupingTuples = null;
      groupingKeyChanged = false;
      aggregatorFinished = false;
    }

    public void close() throws IOException {
      distinctAggrDatas.clear();
      distinctAggrDatas = null;
      currentGroupingTuples = null;
      iterator = null;
    }

    Entry<Tuple, Set<Tuple>> currentGroupingTuples;
    Iterator<Tuple> distinctKeyIterator;
    boolean groupingKeyChanged = false;

    public Tuple next() {
      if (aggregatorFinished) {
        return null;
      }
      if (currentGroupingTuples == null) {
        // first
        if (!iterator.hasNext()) {
          // Empty case
          aggregatorFinished = true;
          return null;
        }
        currentGroupingTuples = iterator.next();
        groupingKeyChanged = true;
        distinctKeyIterator = currentGroupingTuples.getValue().iterator();
      }
      if (!distinctKeyIterator.hasNext()) {
        if (!iterator.hasNext()) {
          aggregatorFinished = true;
          return null;
        }
        currentGroupingTuples = iterator.next();
        groupingKeyChanged = true;
        distinctKeyIterator = currentGroupingTuples.getValue().iterator();
      }
      // node sequence, groupingKeys, 1'st distinctKeys, 2'st distinctKeys, ...
      // If n'st == this.nodeSequence set with real data, otherwise set with NullDatum
      Tuple tuple = new VTuple(resultTupleLength);
      int tupleIndex = 0;
      tuple.put(tupleIndex++, nodeSequenceDatum);

      // merge grouping key
      Tuple groupingKeyTuple = currentGroupingTuples.getKey();
      int groupingKeyLength = groupingKeyTuple.size();
      for (int i = 0; i < groupingKeyLength; i++, tupleIndex++) {
        tuple.put(tupleIndex, groupingKeyTuple.get(i));
      }

      // merge distinctKey
      for (int i = 0; i < distinctAggregators.length; i++) {
        if (i == nodeSequence) {
          Tuple distinctKeyTuple = distinctKeyIterator.next();
          int distinctKeyLength = distinctKeyTuple.size();
          for (int j = 0; j < distinctKeyLength; j++, tupleIndex++) {
            tuple.put(tupleIndex, distinctKeyTuple.get(j));
          }
        } else {
          Tuple dummyTuple = distinctAggregators[i].getDummyTuple();
          int dummyTupleSize = dummyTuple.size();
          for (int j = 0; j < dummyTupleSize; j++, tupleIndex++) {
            tuple.put(tupleIndex, dummyTuple.get(j));
          }
        }
      }

      // merge non distinct aggregation tuple
      if (nonDistinctHashAggregator != null) {
        Tuple nonDistinctTuple;
        if (nodeSequence == 0 && groupingKeyChanged) {
          groupingKeyChanged = false;
          nonDistinctTuple = nonDistinctHashAggregator.aggregate(groupingKeyTuple);
          if (nonDistinctTuple == null) {
            nonDistinctTuple = nonDistinctHashAggregator.getDummyTuple();
          }
        } else {
          nonDistinctTuple = nonDistinctHashAggregator.getDummyTuple();
        }
        int tupleSize = nonDistinctTuple.size();
        for (int j = 0; j < tupleSize; j++, tupleIndex++) {
          tuple.put(tupleIndex, nonDistinctTuple.get(j));
        }
      }
      return tuple;
    }

    public Tuple getDummyTuple() {
      return dummyTuple;
    }
  }
}
