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
import org.apache.tajo.engine.planner.physical.ComparableVector.ComparableTuple;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
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

    List<GroupbyNode> groupbyNodes = plan.getSubPlans();

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
    distinctAggregators = distinctAggrList.toArray(new DistinctHashAggregator[distinctAggrList.size()]);
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
    Tuple tuple;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      for (int i = 0; i < distinctAggregators.length; i++) {
        distinctAggregators[i].compute(tuple);
      }
      if (nonDistinctHashAggregator != null) {
        nonDistinctHashAggregator.compute(tuple);
      }
    }
    for (int i = 0; i < distinctAggregators.length; i++) {
      distinctAggregators[i].rescan();
    }

    totalNumRows = distinctAggregators[0].distinctAggrData.size();
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
    private int aggFunctionsNum;
    private final AggregationFunctionCallEval aggFunctions[];

    // GroupingKey -> FunctionContext[]
    private Map<ComparableTuple, FunctionContext[]> nonDistinctAggrData;
    private int tupleLength;

    private Tuple dummyTuple;

    private transient ComparableTuple groupingKey;

    private NonDistinctHashAggregator(GroupbyNode groupbyNode) throws IOException {

      nonDistinctAggrData = new HashMap<ComparableTuple, FunctionContext[]>();

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
      groupingKey = new ComparableTuple(inSchema, groupingKeyIndexes);
    }

    public void compute(Tuple tuple) {
      groupingKey.set(tuple);
      FunctionContext[] contexts = nonDistinctAggrData.get(groupingKey);
      if (contexts == null) {
        // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for (int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions[i].newContext();
        }
        nonDistinctAggrData.put(groupingKey.copy(), contexts);
      }
      for (int i = 0; i < aggFunctionsNum; i++) {
        aggFunctions[i].merge(contexts[i], inSchema, tuple);
      }
    }

    public Tuple aggregate(ComparableTuple groupingKey) {
      FunctionContext[] contexts = nonDistinctAggrData.get(groupingKey);
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

    // GroupingKey -> DistinctKey
    private Map<ComparableTuple, Set<ComparableTuple>> distinctAggrData;
    private Iterator<Entry<ComparableTuple, Set<ComparableTuple>>> iterator;

    private int nodeSequence;
    private Int2Datum nodeSequenceDatum;

    private int[] distinctKeyIndexes;

    private transient ComparableTuple groupingKey;
    private transient ComparableTuple distinctKey;

    private int tupleLength;
    private Tuple dummyTuple;
    private boolean aggregatorFinished = false;

    public DistinctHashAggregator(GroupbyNode groupbyNode) throws IOException {

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

      this.distinctAggrData = new HashMap<ComparableTuple, Set<ComparableTuple>>();
      this.tupleLength = distinctKeyIndexes.length;

      this.groupingKey = new ComparableTuple(inSchema, groupingKeyIndexes);
      this.distinctKey = new ComparableTuple(inSchema, distinctKeyIndexes);
    }

    public void setNodeSequence(int nodeSequence) {
      this.nodeSequence = nodeSequence;
      this.nodeSequenceDatum = new Int2Datum((short)nodeSequence);
    }

    public int getTupleLength() {
      return tupleLength;
    }

    public void compute(Tuple tuple) throws IOException {
      groupingKey.set(tuple);
      Set<ComparableTuple> distinctEntry = distinctAggrData.get(groupingKey);
      if (distinctEntry == null) {
        distinctEntry = new HashSet<ComparableTuple>();
        distinctAggrData.put(groupingKey.copy(), distinctEntry);
      }
      distinctKey.set(tuple);
      if (distinctEntry.add(distinctKey)) {
        distinctKey = distinctKey.emptyCopy();
      }
    }

    public void rescan() {
      iterator = distinctAggrData.entrySet().iterator();
      currentGroupingTuples = null;
      groupingKeyChanged = false;
      aggregatorFinished = false;
    }

    public void close() throws IOException {
      distinctAggrData.clear();
      distinctAggrData = null;
      currentGroupingTuples = null;
      iterator = null;
    }

    Entry<ComparableTuple, Set<ComparableTuple>> currentGroupingTuples;
    Iterator<ComparableTuple> distinctKeyIterator;
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
      ComparableTuple groupingKey = currentGroupingTuples.getKey();
      int groupingKeyLength = groupingKey.size();
      for (int i = 0; i < groupingKeyLength; i++, tupleIndex++) {
        tuple.put(tupleIndex, groupingKey.toDatum(i));
      }

      // merge distinctKey
      for (int i = 0; i < distinctAggregators.length; i++) {
        if (i == nodeSequence) {
          ComparableTuple distinctKeyTuple = distinctKeyIterator.next();
          int distinctKeyLength = distinctKeyTuple.size();
          for (int j = 0; j < distinctKeyLength; j++, tupleIndex++) {
            tuple.put(tupleIndex, distinctKeyTuple.toDatum(j));
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
          nonDistinctTuple = nonDistinctHashAggregator.aggregate(groupingKey);
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
