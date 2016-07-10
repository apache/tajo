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
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class incremented each row to more rows by grouping columns. In addition, the operator must creates each row
 * because of aggregation non-distinct columns.
 *
 * For example, there is a query as follows:
 *  select sum(distinct l_orderkey), l_linenumber, l_returnflag, l_linestatus, l_shipdate,
 *         count(distinct l_partkey), sum(l_orderkey)
 *  from lineitem
 *  group by l_linenumber, l_returnflag, l_linestatus, l_shipdate;
 *
 *  If you execute above query on tajo, FileScanner makes tuples after scanning raw data as follows:
 *
 *  -----------------------------------------------------------------------------
 *  l_linenumber, l_returnflag, l_linestatus, l_shipdate, l_orderkey, l_partkey
 *  -----------------------------------------------------------------------------
 *  1, N, O, 1996-03-13, 1, 1
 *  2, N, O, 1996-04-12, 1, 1
 *  1, N, O, 1997-01-28, 2, 2
 *  1, R, F, 1994-02-02, 3, 2
 *  2, R, F, 1993-11-09, 3, 3
 *  
 *  And then the scanner will push it as input data to this class. After then, this class will makes output data as
 *  follows:
 *
 *  -------------------------------------------------------------------------------------------------------------------
 *  NodeSequence, l_linenumber, l_returnflag, l_linestatus, l_shipdate, l_partkey for distinct,
 *  l_orderkey for distinct, l_orderkey for nondistinct
 *  -------------------------------------------------------------------------------------------------------------------
 *  0, 2, R, F, 1993-11-09, 3, NULL, 3
 *  0, 2, N, O, 1996-04-12, 1, NULL, 1
 *  0, 1, N, O, 1997-01-28, 2, NULL, 2
 *  0, 1, R, F, 1994-02-02, 2, NULL, 3
 *  0, 1, N, O, 1996-03-13, 1, NULL, 1
 *  1, 2, R, F, 1993-11-09, NULL, 3, NULL
 *  1, 2, N, O, 1996-04-12, NULL, 1, NULL
 *  1, 1, N, O, 1997-01-28, NULL, 2, NULL
 *  1, 1, R, F, 1994-02-02, NULL, 3, NULL
 *  1, 1, N, O, 1996-03-13, NULL, 1, NULL
 *
 *  For reference, NodeSequence means GroupByNode sequence. In this case, there are two GroupByNode. And it consist
 *  of lineitem.l_partkey and lineitem.l_orderkey. The NodeSequence of lineitem.l_partkey is zero and the sequence of
 *  lineitem.l_orderkey is one. As above output data, If there are uncomfortable column for DistinctGroupBy, 
 *  inner aggregator makes it to NullDataTum.
 *  
 *  In addition, columns for NonDistinctGroupBy only can contains real value at first NodeSequence.
 *
 */

public class DistinctGroupbyFirstAggregationExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(DistinctGroupbyFirstAggregationExec.class);

  private DistinctGroupbyNode plan;
  private boolean finished = false;
  private boolean preparedData = false;

  private long totalNumRows;
  private int fetchedRows;

  private NonDistinctHashAggregator nonDistinctHashAggregator;
  private Map<Integer, DistinctHashAggregator> nodeSeqToDistinctAggregators = new HashMap<>();

  private KeyProjector nonDistinctGroupingKeyProjector;
  private Map<Integer, KeyProjector> distinctGroupbyKeyProjectors = new HashMap<>();

  private int resultTupleLength;

  public DistinctGroupbyFirstAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), subOp);
    this.plan = plan;
  }

  @Override
  public void init() throws IOException {
    super.init();

    // finding grouping column index
    Column[] groupingKeyColumns = plan.getGroupingColumns();
    nonDistinctGroupingKeyProjector = new KeyProjector(inSchema, plan.getGroupingColumns());
    resultTupleLength = groupingKeyColumns.length + 1;  //1 is Sequence Datum which indicates sequence of DistinctNode.

    List<GroupbyNode> groupbyNodes = plan.getSubPlans();

    int distinctSeq = 0;
    for (GroupbyNode eachGroupby: groupbyNodes) {
      if (eachGroupby.isDistinct()) {
        DistinctHashAggregator aggregator = new DistinctHashAggregator(eachGroupby, distinctSeq);
        nodeSeqToDistinctAggregators.put(distinctSeq++, aggregator);
        resultTupleLength += aggregator.getTupleLength();
      } else {
        nonDistinctHashAggregator = new NonDistinctHashAggregator(eachGroupby);
        resultTupleLength += nonDistinctHashAggregator.getTupleLength();
      }
    }
  }

  private int currentAggregatorIndex = 0;

  @Override
  public Tuple next() throws IOException {
    if (!preparedData) {
      prepareInputData();
    }

    int prevIndex = currentAggregatorIndex;
    while (!context.isStopped()) {
      DistinctHashAggregator aggregator = nodeSeqToDistinctAggregators.get(currentAggregatorIndex);
      Tuple result = aggregator.next();
      if (result != null) {
        return result;
      }
      currentAggregatorIndex++;
      currentAggregatorIndex = currentAggregatorIndex % nodeSeqToDistinctAggregators.size();
      if (currentAggregatorIndex == prevIndex) {
        finished = true;
        return null;
      }
    }

    return null;
  }

  private void prepareInputData() throws IOException {
    Tuple tuple;

    while(!context.isStopped() && (tuple = child.next()) != null) {

      KeyTuple groupingKey = nonDistinctGroupingKeyProjector.project(tuple);
      for (int i = 0; i < nodeSeqToDistinctAggregators.size(); i++) {
        nodeSeqToDistinctAggregators.get(i).compute(groupingKey, tuple);
      }
      if (nonDistinctHashAggregator != null) {
        nonDistinctHashAggregator.compute(groupingKey, tuple);
      }
    }
    for (int i = 0; i < nodeSeqToDistinctAggregators.size(); i++) {
      nodeSeqToDistinctAggregators.get(i).rescan();
    }

    totalNumRows = nodeSeqToDistinctAggregators.get(0).distinctAggrDatas.size();
    preparedData = true;
  }

  @Override
  public void close() throws IOException {
    if (nonDistinctHashAggregator != null) {
      nonDistinctHashAggregator.close();
      nonDistinctHashAggregator = null;
    }
    for (DistinctHashAggregator aggregator : nodeSeqToDistinctAggregators.values()) {
      aggregator.close();
    }
    nodeSeqToDistinctAggregators.clear();
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
    for (int i = 0; i < nodeSeqToDistinctAggregators.size(); i++) {
      nodeSeqToDistinctAggregators.get(i).rescan();
    }
  }

  class NonDistinctHashAggregator {
    private int aggFunctionsNum;
    private final List<AggregationFunctionCallEval> aggFunctions;

    // GroupingKey -> FunctionContext[]
    private TupleMap<FunctionContext[]> nonDistinctAggrDatas;
    private int tupleLength;

    private final Tuple dummyTuple;
    private final Tuple outTuple;
    private NonDistinctHashAggregator(GroupbyNode groupbyNode) throws IOException {

      nonDistinctAggrDatas = new TupleMap<>();

      if (groupbyNode.hasAggFunctions()) {
        aggFunctions = groupbyNode.getAggFunctions();
        aggFunctionsNum = aggFunctions.size();
      } else {
        aggFunctions = new ArrayList<>();
        aggFunctionsNum = 0;
      }

      for (AggregationFunctionCallEval eachFunction: aggFunctions) {
        eachFunction.bind(context.getEvalContext(), inSchema);
        eachFunction.setFirstPhase();
      }

      outTuple = new VTuple(aggFunctionsNum);
      dummyTuple = NullTuple.create(aggFunctionsNum);
      tupleLength = aggFunctionsNum;
    }

    public void compute(KeyTuple groupingKeyTuple, Tuple tuple) {
      FunctionContext[] contexts = nonDistinctAggrDatas.get(groupingKeyTuple);
      if (contexts != null) {
        for (int i = 0; i < aggFunctions.size(); i++) {
          aggFunctions.get(i).merge(contexts[i], tuple);
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for (int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions.get(i).newContext();
          aggFunctions.get(i).merge(contexts[i], tuple);
        }
        nonDistinctAggrDatas.put(groupingKeyTuple, contexts);
      }
    }

    public Tuple aggregate(Tuple groupingKey) {
      FunctionContext[] contexts = nonDistinctAggrDatas.get(groupingKey);
      if (contexts == null) {
        return null;
      }

      for (int i = 0; i < aggFunctionsNum; i++) {
        outTuple.put(i, aggFunctions.get(i).terminate(contexts[i]));
      }

      return outTuple;
    }

    public int getTupleLength() {
      return tupleLength;
    }

    public Tuple getDummyTuple() {
      return dummyTuple;
    }

    public void close() {
      nonDistinctAggrDatas.clear();
      nonDistinctAggrDatas = null;
    }
  }

  class DistinctHashAggregator {

    // GroupingKey -> DistinctKey
    private TupleMap<TupleSet> distinctAggrDatas;
    private Iterator<Entry<KeyTuple, TupleSet>> iterator = null;

    private int nodeSequence;
    private Int2Datum nodeSequenceDatum;

    private int tupleLength;
    private final Tuple dummyTuple;
    private Tuple outTuple;
    private boolean aggregatorFinished = false;

    public DistinctHashAggregator(GroupbyNode groupbyNode, int nodeSequence) throws IOException {

      Set<Column> groupingKeySet = new HashSet<>(Arrays.asList(plan.getGroupingColumns()));

      List<Column> distinctGroupingKeyIndexSet = new ArrayList<>();
      Column[] groupingColumns = groupbyNode.getGroupingColumns();
      for (Column col : groupingColumns) {
        if (!groupingKeySet.contains(col)) {
          distinctGroupingKeyIndexSet.add(col);
        }
      }
      Column[] distinctKeyColumns = new Column[distinctGroupingKeyIndexSet.size()];
      distinctKeyColumns = distinctGroupingKeyIndexSet.toArray(distinctKeyColumns);
      this.dummyTuple = NullTuple.create(distinctGroupingKeyIndexSet.size());

      this.distinctAggrDatas = new TupleMap<>();
      distinctGroupbyKeyProjectors.put(nodeSequence, new KeyProjector(inSchema, distinctKeyColumns));
      this.tupleLength = distinctKeyColumns.length;

      setNodeSequence(nodeSequence);
    }

    private void setNodeSequence(int nodeSequence) {
      this.nodeSequence = nodeSequence;
      this.nodeSequenceDatum = new Int2Datum((short)nodeSequence);
    }

    public int getTupleLength() {
      return tupleLength;
    }

    public void compute(KeyTuple groupingKey, Tuple tuple) throws IOException {
      KeyTuple distinctKeyTuple = distinctGroupbyKeyProjectors.get(nodeSequence).project(tuple);

      TupleSet distinctEntry = distinctAggrDatas.get(groupingKey);
      if (distinctEntry == null) {
        distinctEntry = new TupleSet();
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
      distinctAggrDatas.values().forEach(TupleSet::clear);
      distinctAggrDatas.clear();
      distinctAggrDatas = null;
      currentGroupingTuples = null;
      iterator = null;
    }

    Entry<KeyTuple, TupleSet> currentGroupingTuples;
    Iterator<KeyTuple> distinctKeyIterator;
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
      int tupleIndex = 0;
      if (outTuple == null) {
        outTuple = new VTuple(resultTupleLength);
      }
      outTuple.put(tupleIndex++, nodeSequenceDatum);

      // merge grouping key
      Tuple groupingKeyTuple = currentGroupingTuples.getKey();
      int groupingKeyLength = groupingKeyTuple.size();
      for (int i = 0; i < groupingKeyLength; i++, tupleIndex++) {
        outTuple.put(tupleIndex, groupingKeyTuple.asDatum(i));
      }

      // merge distinctKey
      for (int i = 0; i < nodeSeqToDistinctAggregators.size(); i++) {
        if (i == nodeSequence) {
          Tuple distinctKeyTuple = distinctKeyIterator.next();
          int distinctKeyLength = distinctKeyTuple.size();
          for (int j = 0; j < distinctKeyLength; j++, tupleIndex++) {
            outTuple.put(tupleIndex, distinctKeyTuple.asDatum(j));
          }
        } else {
          Tuple dummyTuple = nodeSeqToDistinctAggregators.get(i).getDummyTuple();
          int dummyTupleSize = dummyTuple.size();
          for (int j = 0; j < dummyTupleSize; j++, tupleIndex++) {
            outTuple.put(tupleIndex, dummyTuple.asDatum(j));
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
          outTuple.put(tupleIndex, nonDistinctTuple.asDatum(j));
        }
      }
      return outTuple;
    }

    public Tuple getDummyTuple() {
      return dummyTuple;
    }
  }
}
