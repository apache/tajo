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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
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
        if (currentTuples[i-1] != null) {
          aggregateExecs[i].rescan();
        }
      }
      currentTuples[i] = aggregateExecs[i].next();

      if (currentTuples[i] != null) {
        allNull = false;
      }
    }

    // If DistinctGroupbySortAggregationExec received NullDatum and didn't has any grouping keys,
    // it should return primitive values for NullDatum.
    if (allNull && aggregateExecs[0].groupingKeyNum == 0 && first)   {
      return getEmptyTuple();
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

  private Tuple getEmptyTuple() {
    Tuple tuple = new VTuple(outColumnNum);
    NullDatum nullDatum = DatumFactory.createNullDatum();

    for (int i = 0; i < outColumnNum; i++) {
      TajoDataTypes.Type type = outSchema.getColumn(i).getDataType().getType();
      if (type == TajoDataTypes.Type.INT8) {
        tuple.put(i, DatumFactory.createInt8(nullDatum.asInt8()));
      } else if (type == TajoDataTypes.Type.INT4) {
        tuple.put(i, DatumFactory.createInt4(nullDatum.asInt4()));
      } else if (type == TajoDataTypes.Type.INT2) {
        tuple.put(i, DatumFactory.createInt2(nullDatum.asInt2()));
      } else if (type == TajoDataTypes.Type.FLOAT4) {
        tuple.put(i, DatumFactory.createFloat4(nullDatum.asFloat4()));
      } else if (type == TajoDataTypes.Type.FLOAT8) {
        tuple.put(i, DatumFactory.createFloat8(nullDatum.asFloat8()));
      } else {
        tuple.put(i, DatumFactory.createNullDatum());
      }
    }

    finished = true;
    first = false;

    return tuple;
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
