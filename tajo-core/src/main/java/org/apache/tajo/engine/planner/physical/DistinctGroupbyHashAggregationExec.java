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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class DistinctGroupbyHashAggregationExec extends UnaryPhysicalExec {
  private boolean finished = false;

  private DistinctGroupbyNode plan;
  private HashAggregator[] hashAggregators;
  private int distinctGroupingKeyIds[];
  private boolean first = true;
  private int groupbyNodeNum;
  private int outputColumnNum;
  private int totalNumRows;
  private int fetchedRows;

  private int[] resultColumnIdIndexes;

  public DistinctGroupbyHashAggregationExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), subOp);
    this.plan = plan;
  }

  @Override
  public void init() throws IOException {
    super.init();

    List<Integer> distinctGroupingKeyIdList = new ArrayList<Integer>();
    for (Column col: plan.getGroupingColumns()) {
      int keyIndex;
      if (col.hasQualifier()) {
        keyIndex = inSchema.getColumnId(col.getQualifiedName());
      } else {
        keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
      }
      if (!distinctGroupingKeyIdList.contains(keyIndex)) {
        distinctGroupingKeyIdList.add(keyIndex);
      }
    }
    int idx = 0;
    distinctGroupingKeyIds = new int[distinctGroupingKeyIdList.size()];
    for (Integer intVal: distinctGroupingKeyIdList) {
      distinctGroupingKeyIds[idx++] = intVal;
    }

    List<GroupbyNode> groupbyNodes = plan.getSubPlans();
    groupbyNodeNum = groupbyNodes.size();
    this.hashAggregators = new HashAggregator[groupbyNodeNum];

    int index = 0;
    for (GroupbyNode eachGroupby: groupbyNodes) {
      hashAggregators[index++] = new HashAggregator(eachGroupby, inSchema);
    }

    outputColumnNum = plan.getOutSchema().size();

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

  List<Tuple> currentAggregatedTuples = null;
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

    List<List<Tuple>> tupleSlots = new ArrayList<List<Tuple>>();

    // aggregation with single grouping key
    for (int i = 0; i < hashAggregators.length; i++) {
      if (!hashAggregators[i].iterator.hasNext()) {
        nullCount++;
        tupleSlots.add(new ArrayList<Tuple>());
        continue;
      }
      Entry<Tuple, Map<Tuple, FunctionContext[]>> entry = hashAggregators[i].iterator.next();
      if (distinctGroupingKey == null) {
        distinctGroupingKey = entry.getKey();
      }
      List<Tuple> aggregatedTuples = hashAggregators[i].aggregate(entry.getValue());
      tupleSlots.add(aggregatedTuples);
    }

    if (nullCount == hashAggregators.length) {
      finished = true;
      progress = 1.0f;

      // If DistinctGroupbyHashAggregationExec does not have any rows,
      // it should return NullDatum.
      if (totalNumRows == 0 && groupbyNodeNum == 0) {
        Tuple tuple = new VTuple(outputColumnNum);
        for (int i = 0; i < tuple.size(); i++) {
          tuple.put(i, DatumFactory.createNullDatum());
        }
        return tuple;
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
    currentAggregatedTuples = new ArrayList<Tuple>();
    int listIndex = 0;
    while (true) {
      // Each item in tuples is VTuple. So the tuples variable is two dimensions(tuple[aggregator][datum]).
      Tuple[] tuples = new Tuple[hashAggregators.length];
      for (int i = 0; i < hashAggregators.length; i++) {
        List<Tuple> aggregatedTuples = tupleSlots.get(i);
        if (aggregatedTuples.size() > listIndex) {
          tuples[i] = tupleSlots.get(i).get(listIndex);
        }
      }

      //merge
      Tuple mergedTuple = new VTuple(outputColumnNum);
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
              mergedTuple.put(mergeTupleIndex, distinctGroupingKey.get(mergeTupleIndex));
            } else {
              if (tuples[i] != null) {
                mergedTuple.put(mergeTupleIndex, tuples[i].get(j));
              } else {
                mergedTuple.put(mergeTupleIndex, NullDatum.get());
              }
            }
          }
          resultColumnIdx++;
        }
      }

      if (allNull) {
        break;
      }

      currentAggregatedTuples.add(mergedTuple);
      listIndex++;
    }

    currentAggregatedTupleIndex = 0;
    currentAggregatedTupleSize = currentAggregatedTuples.size();

    if (currentAggregatedTupleSize == 0) {
      finished = true;
      progress = 1.0f;
      return null;
    }

    fetchedRows++;
    Tuple tuple = currentAggregatedTuples.get(currentAggregatedTupleIndex++);

    return tuple;
  }

  private void loadChildHashTable() throws IOException {
    Tuple tuple = null;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      for (int i = 0; i < hashAggregators.length; i++) {
        hashAggregators[i].compute(tuple);
      }
    }
    for (int i = 0; i < hashAggregators.length; i++) {
      hashAggregators[i].initFetch();
    }

    totalNumRows = hashAggregators[0].hashTable.size();
  }

  @Override
  public void close() throws IOException {
    if (hashAggregators != null) {
      for (int i = 0; i < hashAggregators.length; i++) {
        hashAggregators[i].close();
      }
    }
    if (child != null) {
      child.close();
    }
  }

  public void rescan() throws IOException {
    finished = false;
    for (int i = 0; i < hashAggregators.length; i++) {
      hashAggregators[i].initFetch();
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
    private Map<Tuple, Map<Tuple, FunctionContext[]>> hashTable;
    private Iterator<Entry<Tuple, Map<Tuple, FunctionContext[]>>> iterator = null;

    private int groupingKeyIds[];
    private final int aggFunctionsNum;
    private final AggregationFunctionCallEval aggFunctions[];

    int tupleSize;

    public HashAggregator(GroupbyNode groupbyNode, Schema schema) throws IOException {

      hashTable = new HashMap<Tuple, Map<Tuple, FunctionContext[]>>(10000);

      List<Integer> distinctGroupingKeyIdSet = new ArrayList<Integer>();
      for (int i = 0; i < distinctGroupingKeyIds.length; i++) {
        distinctGroupingKeyIdSet.add(distinctGroupingKeyIds[i]);
      }

      List<Integer> groupingKeyIdList = new ArrayList<Integer>(distinctGroupingKeyIdSet);
      Column[] keyColumns = groupbyNode.getGroupingColumns();
      Column col;
      for (int idx = 0; idx < keyColumns.length; idx++) {
        col = keyColumns[idx];
        int keyIndex;
        if (col.hasQualifier()) {
          keyIndex = inSchema.getColumnId(col.getQualifiedName());
        } else {
          keyIndex = inSchema.getColumnIdByName(col.getSimpleName());
        }
        if (!distinctGroupingKeyIdSet.contains(keyIndex)) {
          groupingKeyIdList.add(keyIndex);
        }
      }
      int index = 0;
      groupingKeyIds = new int[groupingKeyIdList.size()];
      for (Integer eachId : groupingKeyIdList) {
        groupingKeyIds[index++] = eachId;
      }

      if (groupbyNode.hasAggFunctions()) {
        aggFunctions = groupbyNode.getAggFunctions();
        aggFunctionsNum = aggFunctions.length;
      } else {
        aggFunctions = new AggregationFunctionCallEval[0];
        aggFunctionsNum = 0;
      }

      for (EvalNode aggFunction : aggFunctions) {
        aggFunction.bind(context.getEvalContext(), schema);
      }

      tupleSize = groupingKeyIds.length + aggFunctionsNum;
    }

    public int getTupleSize() {
      return tupleSize;
    }

    public void compute(Tuple tuple) throws IOException {
      Tuple outerKeyTuple = new VTuple(distinctGroupingKeyIds.length);
      for (int i = 0; i < distinctGroupingKeyIds.length; i++) {
        outerKeyTuple.put(i, tuple.get(distinctGroupingKeyIds[i]));
      }

      Tuple keyTuple = new VTuple(groupingKeyIds.length);
      for (int i = 0; i < groupingKeyIds.length; i++) {
        keyTuple.put(i, tuple.get(groupingKeyIds[i]));
      }

      Map<Tuple, FunctionContext[]> distinctEntry = hashTable.get(outerKeyTuple);
      if (distinctEntry == null) {
        distinctEntry = new HashMap<Tuple, FunctionContext[]>();
        hashTable.put(outerKeyTuple, distinctEntry);
      }
      FunctionContext[] contexts = distinctEntry.get(keyTuple);
      if (contexts != null) {
        for (int i = 0; i < aggFunctions.length; i++) {
          aggFunctions[i].merge(contexts[i], tuple);
        }
      } else { // if the key occurs firstly
        contexts = new FunctionContext[aggFunctionsNum];
        for (int i = 0; i < aggFunctionsNum; i++) {
          contexts[i] = aggFunctions[i].newContext();
          aggFunctions[i].merge(contexts[i], tuple);
        }
        distinctEntry.put(keyTuple, contexts);
      }
    }

    public void initFetch() {
      iterator = hashTable.entrySet().iterator();
    }

    public List<Tuple> aggregate(Map<Tuple, FunctionContext[]> groupTuples) {
      List<Tuple> aggregatedTuples = new ArrayList<Tuple>();

      for (Entry<Tuple, FunctionContext[]> entry : groupTuples.entrySet()) {
        Tuple tuple = new VTuple(groupingKeyIds.length + aggFunctionsNum);
        Tuple groupbyKey = entry.getKey();
        int index = 0;
        for (; index < groupbyKey.size(); index++) {
          tuple.put(index, groupbyKey.get(index));
        }

        FunctionContext[] contexts = entry.getValue();
        for (int i = 0; i < aggFunctionsNum; i++, index++) {
          tuple.put(index, aggFunctions[i].terminate(contexts[i]));
        }
        aggregatedTuples.add(tuple);
      }
      return aggregatedTuples;
    }

    public void close() throws IOException {
      hashTable.clear();
      hashTable = null;
      iterator = null;
    }
  }
}
