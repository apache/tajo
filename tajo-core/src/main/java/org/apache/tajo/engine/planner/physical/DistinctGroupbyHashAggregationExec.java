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

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DistinctGroupbyHashAggregationExec extends UnaryPhysicalExec {

  private boolean finished = false;

  private final DistinctGroupbyNode plan;
  private HashAggregator[] hashAggregators;

  private List<Column> distinctGroupingKeyColumnSet;
  private boolean first = true;
  private int groupbyNodeNum;
  private int outputColumnNum;
  private int totalNumRows;
  private int fetchedRows;

  private int[] resultColumnIdIndexes;

  private Tuple outTuple;

  private KeyProjector outerKeyProjector;

  public DistinctGroupbyHashAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), subOp);
    this.plan = plan;
  }

  @Override
  public void init() throws IOException {
    super.init();

    distinctGroupingKeyColumnSet = new ArrayList<>();
    for (Column col : plan.getGroupingColumns()) {
      if (!distinctGroupingKeyColumnSet.contains(col)) {
        distinctGroupingKeyColumnSet.add(col);
      }
    }
    Column[] distinctGroupingKeyColumns = new Column[distinctGroupingKeyColumnSet.size()];
    distinctGroupingKeyColumns = distinctGroupingKeyColumnSet.toArray(distinctGroupingKeyColumns);

    outerKeyProjector = new KeyProjector(inSchema, distinctGroupingKeyColumns);

    List<GroupbyNode> groupbyNodes = plan.getSubPlans();
    groupbyNodeNum = groupbyNodes.size();
    this.hashAggregators = new HashAggregator[groupbyNodeNum];

    int index = 0;
    for (GroupbyNode eachGroupby: groupbyNodes) {
      hashAggregators[index++] = new HashAggregator(eachGroupby, inSchema);
    }

    outputColumnNum = plan.getOutSchema().size();
    outTuple = new VTuple(outputColumnNum);

    int allGroupbyOutColNum = 0;
    for (GroupbyNode eachGroupby: plan.getSubPlans()) {
      allGroupbyOutColNum += eachGroupby.getOutSchema().size();
    }

    resultColumnIdIndexes = new int[allGroupbyOutColNum];
    for (int i = 0; i < allGroupbyOutColNum; i++) {
      resultColumnIdIndexes[i] = -1;
    }

    int[] resultColumnIds = plan.getResultColumnIds();
    for(int i = 0; i < resultColumnIds.length; i++) {
      resultColumnIdIndexes[resultColumnIds[i]] = i;
    }
  }

  TupleList currentAggregatedTuples = null;
  int currentAggregatedTupleIndex = 0;
  int currentAggregatedTupleSize = 0;

  @Override
  public Tuple next() throws IOException {
    if (finished) {
      return null;
    }
    if (first) {
      loadChildHashTable();

      progress = 0.5f;
      first = false;
    }

    if (currentAggregatedTuples != null && currentAggregatedTupleIndex < currentAggregatedTupleSize) {
      return currentAggregatedTuples.get(currentAggregatedTupleIndex++);
    }

    Tuple distinctGroupingKey = null;
    int nullCount = 0;

    //--------------------------------------------------------------------------------------
    // Output tuple
    //--------------------------------------------------------------------------------------
    //                hashAggregators[0]    hashAggregators[1]    hashAggregators[2]
    //--------------------------------------------------------------------------------------
    // Groupby_Key1 | Distinct1_Column_V1 | Distinct2_Column_Va | Other_Aggregation_Result |
    // Groupby_Key1 | Distinct1_Column_V2 | Distinct2_Column_Vb |                          |
    // Groupby_Key1 |                     | Distinct2_Column_Vc |                          |
    // Groupby_Key1 |                     | Distinct2_Column_Vd |                          |
    //--------------------------------------------------------------------------------------
    // Groupby_Key2 | Distinct1_Column_V1 | Distinct2_Column_Vk | Other_Aggregation_Result |
    // Groupby_Key2 | Distinct1_Column_V2 | Distinct2_Column_Vn |                          |
    // Groupby_Key2 | Distinct1_Column_V3 |                     |                          |
    //--------------------------------------------------------------------------------------

    List<TupleList> tupleSlots = new ArrayList<>();

    // aggregation with single grouping key
    for (HashAggregator hashAggregator : hashAggregators) {
      if (!hashAggregator.iterator.hasNext()) {
        nullCount++;
        tupleSlots.add(new TupleList());
        continue;
      }
      Entry<KeyTuple, TupleMap<FunctionContext[]>> entry = hashAggregator.iterator.next();
      if (distinctGroupingKey == null) {
        distinctGroupingKey = entry.getKey();
      }
      TupleList aggregatedTuples = hashAggregator.aggregate(entry.getValue());
      tupleSlots.add(aggregatedTuples);
    }

    if (nullCount == hashAggregators.length) {
      finished = true;
      progress = 1.0f;

      // If DistinctGroupbyHashAggregationExec does not have any rows,
      // it should return NullDatum.
      if (totalNumRows == 0 && groupbyNodeNum == 0) {
        return NullTuple.create(outputColumnNum);
      } else {
        return null;
      }
    }


    /*
    Tuple materialization example
    =============================

    Output Tuple Index: 0(l_orderkey), 1(l_partkey), 2(default.lineitem.l_suppkey), 5(default.lineitem.
    l_partkey), 8(sum)

              select
                  lineitem.l_orderkey as l_orderkey,
                  lineitem.l_partkey as l_partkey,
                  count(distinct lineitem.l_partkey) as cnt1,
                  count(distinct lineitem.l_suppkey) as cnt2,
                  sum(lineitem.l_quantity) as sum1
              from
                  lineitem
              group by
                  lineitem.l_orderkey, lineitem.l_partkey

    The above case will result in the following materialization
    ------------------------------------------------------------

    l_orderkey  l_partkey  default.lineitem.l_suppkey  l_orderkey  l_partkey  default.lineitem.l_partkey  l_orderkey  l_partkey  sum
        1            1              7311                   1            1                1                    1           1      53.0
        1            1              7706

    */

    // currentAggregatedTuples has tuples which has same group key.
    if (currentAggregatedTuples == null) {
      currentAggregatedTuples = new TupleList();
    } else {
      currentAggregatedTuples.clear();
    }
    int listIndex = 0;
    while (true) {
      Tuple[] tuples = new Tuple[hashAggregators.length];
      // Each item in tuples is VTuple. So the tuples variable is two dimensions(tuple[aggregator][datum]).
      for (int i = 0; i < hashAggregators.length; i++) {
        TupleList aggregatedTuples = tupleSlots.get(i);
        if (aggregatedTuples.size() > listIndex) {
          tuples[i] = tupleSlots.get(i).get(listIndex);
        }
      }

      //merge
      int resultColumnIdx = 0;

      boolean allNull = true;
      for (int i = 0; i < hashAggregators.length; i++) {
        if (tuples[i] != null) {
          allNull = false;
        }

        int tupleSize = hashAggregators[i].getTupleSize();
        for (int j = 0; j < tupleSize; j++) {
          int mergeTupleIndex = resultColumnIdIndexes[resultColumnIdx];
          if (mergeTupleIndex >= 0) {
            if (mergeTupleIndex < distinctGroupingKey.size()) {
              // set group key tuple
              // Because each hashAggregator has different number of tuples,
              // sometimes getting group key from each hashAggregator will be null value.
              outTuple.put(mergeTupleIndex, distinctGroupingKey.asDatum(mergeTupleIndex));
            } else {
              if (tuples[i] != null) {
                outTuple.put(mergeTupleIndex, tuples[i].asDatum(j));
              } else {
                outTuple.put(mergeTupleIndex, NullDatum.get());
              }
            }
          }
          resultColumnIdx++;
        }
      }

      if (allNull) {
        break;
      }

      currentAggregatedTuples.add(outTuple);
      listIndex++;
    }

    tupleSlots.forEach(TupleList::clear);
    tupleSlots.clear();

    currentAggregatedTupleIndex = 0;
    currentAggregatedTupleSize = currentAggregatedTuples.size();

    if (currentAggregatedTupleSize == 0) {
      finished = true;
      progress = 1.0f;
      return null;
    }

    fetchedRows++;
    return currentAggregatedTuples.get(currentAggregatedTupleIndex++);
  }

  private void loadChildHashTable() throws IOException {
    Tuple tuple;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      for (HashAggregator hashAggregator : hashAggregators) {
        hashAggregator.compute(tuple);
      }
    }
    for (HashAggregator hashAggregator : hashAggregators) {
      hashAggregator.initFetch();
    }

    totalNumRows = hashAggregators[0].hashTable.size();
  }

  @Override
  public void close() throws IOException {
    if (hashAggregators != null) {
      for (HashAggregator hashAggregator : hashAggregators) {
        hashAggregator.close();
      }
    }
    if (child != null) {
      child.close();
    }
    if (currentAggregatedTuples != null) {
      currentAggregatedTuples.clear();
      currentAggregatedTuples = null;
    }
    if (distinctGroupingKeyColumnSet != null) {
      distinctGroupingKeyColumnSet.clear();
    }
  }

  public void rescan() throws IOException {
    finished = false;
    for (HashAggregator hashAggregator : hashAggregators) {
      hashAggregator.initFetch();
    }
    if (currentAggregatedTuples != null) {
      currentAggregatedTuples.clear();
    }
  }

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

  public TableStats getInputStats() {
    if (child != null) {
      return child.getInputStats();
    } else {
      return null;
    }
  }

  class HashAggregator {
    // Outer's GroupBy Key -> Each GroupByNode's Key -> FunctionContext
    private TupleMap<TupleMap<FunctionContext[]>> hashTable;
    private Iterator<Entry<KeyTuple, TupleMap<FunctionContext[]>>> iterator = null;

    private final KeyProjector innerKeyProjector;

    private final int aggFunctionsNum;
    private final List<AggregationFunctionCallEval> aggFunctions;

    private final Tuple aggregatedTuple;

    private final int tupleSize;

    public HashAggregator(GroupbyNode groupbyNode, Schema schema) throws IOException {

      hashTable = new TupleMap<>(context.getQueryContext().getInt(SessionVars.AGG_HASH_TABLE_SIZE));

      List<Column> groupingKeyColumnList = new ArrayList<>(distinctGroupingKeyColumnSet);

      Column[] keyColumns = groupbyNode.getGroupingColumns();
      for (Column keyColumn : keyColumns) {
        if (!distinctGroupingKeyColumnSet.contains(keyColumn)) {
          groupingKeyColumnList.add(keyColumn);
        }
      }
      Column[] groupingKeyColumns = groupingKeyColumnList.toArray(new Column[groupingKeyColumnList.size()]);

      if (groupbyNode.hasAggFunctions()) {
        aggFunctions = groupbyNode.getAggFunctions();
        aggFunctionsNum = aggFunctions.size();
      } else {
        aggFunctions = new ArrayList<>();
        aggFunctionsNum = 0;
      }

      for (EvalNode aggFunction : aggFunctions) {
        aggFunction.bind(context.getEvalContext(), schema);
      }

      tupleSize = groupingKeyColumns.length + aggFunctionsNum;
      aggregatedTuple = new VTuple(groupingKeyColumns.length + aggFunctionsNum);
      innerKeyProjector = new KeyProjector(inSchema, groupingKeyColumns);
    }

    public int getTupleSize() {
      return tupleSize;
    }

    public void compute(Tuple tuple) throws IOException {
      KeyTuple outerKeyTuple = outerKeyProjector.project(tuple);
      TupleMap<FunctionContext[]> distinctEntry = hashTable.get(outerKeyTuple);

      if (distinctEntry == null) {
        distinctEntry = new TupleMap<>();
        hashTable.put(outerKeyTuple, distinctEntry);
      }

      KeyTuple innerKeyTuple = innerKeyProjector.project(tuple);
      FunctionContext[] contexts = distinctEntry.get(innerKeyTuple);
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
        distinctEntry.put(innerKeyTuple, contexts);
      }
    }

    public void initFetch() {
      iterator = hashTable.entrySet().iterator();
    }

    public TupleList aggregate(Map<KeyTuple, FunctionContext[]> groupTuples) {
      TupleList aggregatedTuples = new TupleList();

      for (Entry<KeyTuple, FunctionContext[]> entry : groupTuples.entrySet()) {
        Tuple groupbyKey = entry.getKey();
        int index = 0;
        for (; index < groupbyKey.size(); index++) {
          aggregatedTuple.put(index, groupbyKey.asDatum(index));
        }

        FunctionContext[] contexts = entry.getValue();
        for (int i = 0; i < aggFunctionsNum; i++, index++) {
          aggregatedTuple.put(index, aggFunctions.get(i).terminate(contexts[i]));
        }
        aggregatedTuples.add(aggregatedTuple);
      }
      return aggregatedTuples;
    }

    public void close() throws IOException {
      hashTable.values().forEach(TupleMap<FunctionContext[]>::clear);
      hashTable.clear();
      hashTable = null;
      iterator = null;
    }
  }
}
