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

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 * Tajo supports various options for count distinct. First is to execute a count distinct query with two
 * execution blocks. It made by DistinctGroupbyBuilder::buildPlan. Second is to execute the query with three
 * execution blocks. You can use this option for set SessionVars.COUNT_DISTINCT_ALGORITHM to three_stages.
 *
 *  - In first stage, tajo operator incremented each row to more rows by grouping columns. In addition,
 * the operator must creates each row because of aggregation non-distinct columns.
 *  - In second stage, tajo operator aggregates the output of the first stage. For reference,
 * it shuffled by grouping columns and aggregation columns.
 *  - In third stage, tajo operator merges the output of the second stage. For reference,
 * it shuffled by just grouping columns.
 *
 * This class serves on the operator for the first stage.
 *
 */
public class DistinctGroupbyFirstWriterExec extends UnaryPhysicalExec {
  private DistinctGroupbyNode plan;
  private PhysicalExec child;
  private List<Tuple> tupleList;
  private boolean computed = false;
  private Iterator<Tuple> iterator = null;

  protected final int groupingKeyNum;
  protected int groupingKeyIds[];
  protected final int aggFunctionsNum;
  protected final AggregationFunctionCallEval aggFunctions[];

  protected TableStats inputStats;

  public DistinctGroupbyFirstWriterExec(TaskAttemptContext context, DistinctGroupbyNode plan, PhysicalExec subOp)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), subOp);

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

    tupleList = new ArrayList<Tuple>(100000);
    aggFunctions = plan.getAggFunctions();
    aggFunctionsNum = aggFunctions.length;
  }

  @Override
  public Tuple next() throws IOException {
    if(!computed) {
      compute();
      iterator = tupleList.iterator();
      computed = true;
    }

    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return null;
    }
  }

  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    Tuple outputTuple;
    boolean matched = false;
    List<Column> originalGroupingColumns = Lists.newArrayList(plan.getGroupingColumns());

    while((tuple = child.next()) != null && !context.isStopped()) {
      // If there is no grouping columns, we don't need to set NullDatum.
      if (groupingKeyIds.length == 0) {
        tupleList.add(tuple);
      } else {
        matched = false;
        keyTuple = new VTuple(groupingKeyIds.length);
        // build one key tuple
        for(int i = 0; i < groupingKeyIds.length; i++) {
          keyTuple.put(i, tuple.get(groupingKeyIds[i]));
        }

        outputTuple = tuple;
        tupleList.add(outputTuple);

        for (int i = 0; i < child.outColumnNum; i++) {
          matched = false;
          outputTuple = new VTuple(outColumnNum);
          for (int j = 0; j < child.outColumnNum; j++) {
            if (i == j) {
              outputTuple.put(j, tuple.get(j));
            } else {
              outputTuple.put(j, DatumFactory.createNullDatum());
              Column column = child.outSchema.getColumn(j);
              if (originalGroupingColumns.contains(column)) {
                matched = true;
              }
            }
          }

          if (!matched) {
            tupleList.add(outputTuple);
          }
        }
      }
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    iterator = tupleList.iterator();
  }

  @Override
  public void close() throws IOException {
    super.close();
    tupleList.clear();
    tupleList = null;
    iterator = null;
  }

}
