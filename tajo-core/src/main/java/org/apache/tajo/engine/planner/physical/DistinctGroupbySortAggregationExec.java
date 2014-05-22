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

import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class DistinctGroupbySortAggregationExec extends PhysicalExec {
  private DistinctGroupbyNode plan;
  private SortAggregateExec[] aggregateExecs;

  private boolean finished = false;

  private Tuple[] currentTuples;
  private int outColumnNum;
  private int groupbyNodeNum;

  private int[] resultColumnIdIndexes;

  public DistinctGroupbySortAggregationExec(final TaskAttemptContext context, DistinctGroupbyNode plan,
                                            SortAggregateExec[] aggregateExecs) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.plan = plan;
    this.aggregateExecs = aggregateExecs;
    this.groupbyNodeNum = plan.getGroupByNodes().size();

    currentTuples = new Tuple[groupbyNodeNum];
    outColumnNum = outSchema.size();

    int allGroupbyOutColNum = 0;
    for (GroupbyNode eachGroupby: plan.getGroupByNodes()) {
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

    for (SortAggregateExec eachExec: aggregateExecs) {
      eachExec.init();
    }
  }

  boolean first = true;

  @Override
  public Tuple next() throws IOException {
    if (finished) {
      return null;
    }

    boolean allNull = true;
    for (int i = 0; i < groupbyNodeNum; i++) {
      if (first && i > 0) {
        // All SortAggregateExec uses same SeqScanExec object.
        // After running sort, rescan() should be called.
        aggregateExecs[i].rescan();
      }
      currentTuples[i] = aggregateExecs[i].next();

      if (currentTuples[i] != null) {
        allNull = false;
      }
    }
    first = false;

    if (allNull) {
      finished = true;
      return null;
    }

    Tuple mergedTuple = new VTuple(outColumnNum);

    int mergeTupleIndex = 0;
    for (int i = 0; i < currentTuples.length; i++) {
      int tupleSize = currentTuples[i].size();
      for (int j = 0; j < tupleSize; j++) {
        if (resultColumnIdIndexes[mergeTupleIndex] >= 0) {
          mergedTuple.put(resultColumnIdIndexes[mergeTupleIndex], currentTuples[i].get(j));
        }
        mergeTupleIndex++;
      }
    }
    return mergedTuple;
  }

  @Override
  public void close() throws IOException {
    plan = null;
    if (aggregateExecs != null) {
      for (SortAggregateExec eachExec: aggregateExecs) {
        eachExec.close();
      }
    }
  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void rescan() throws IOException {
    finished = false;
    for (int i = 0; i < groupbyNodeNum; i++) {
      aggregateExecs[i].rescan();
    }
  }

  @Override
  public float getProgress() {
    if (finished) {
      return 1.0f;
    } else {
      return aggregateExecs[aggregateExecs.length - 1].getProgress();
    }
  }

  @Override
  public TableStats getInputStats() {
    return aggregateExecs[aggregateExecs.length - 1].getInputStats();
  }
}
