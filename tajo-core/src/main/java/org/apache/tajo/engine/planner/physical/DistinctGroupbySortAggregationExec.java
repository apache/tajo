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
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class DistinctGroupbySortAggregationExec extends PhysicalExec {
  private SortAggregateExec[] aggregateExecs;

  private boolean finished = false;

  private Tuple[] currentTuples;
  private int outColumnNum;
  private int groupbyNodeNum;

  private int[] resultColumnIdIndexes;

  private final Tuple outTuple;

  public DistinctGroupbySortAggregationExec(final TaskAttemptContext context, DistinctGroupbyNode plan,
                                            SortAggregateExec[] aggregateExecs) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.aggregateExecs = aggregateExecs;
    this.groupbyNodeNum = plan.getSubPlans().size();

    currentTuples = new Tuple[groupbyNodeNum];
    outColumnNum = outSchema.size();
    outTuple = new VTuple(outColumnNum);

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

    int mergeTupleIndex = 0;
    for (int i = 0; i < currentTuples.length; i++) {
      int tupleSize = currentTuples[i].size();
      for (int j = 0; j < tupleSize; j++) {
        if (resultColumnIdIndexes[mergeTupleIndex] >= 0) {
          outTuple.put(resultColumnIdIndexes[mergeTupleIndex], currentTuples[i].asDatum(j));
        }
        mergeTupleIndex++;
      }
    }
    return outTuple;
  }

  private Tuple getEmptyTuple() {
    NullDatum nullDatum = DatumFactory.createNullDatum();

    int tupleIndex = 0;
    for (SortAggregateExec aggExec: aggregateExecs) {
      for (int i = 0; i < aggExec.aggFunctionsNum; i++, tupleIndex++) {
        String funcName = aggExec.aggFunctions.get(i).getName();
        if ("min".equals(funcName) || "max".equals(funcName) || "avg".equals(funcName) || "sum".equals(funcName)) {
          outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createNullDatum());
        }
        else
        {
          TajoDataTypes.Type type = outSchema.getColumn(resultColumnIdIndexes[tupleIndex]).getDataType().getType();
          if (type == TajoDataTypes.Type.INT8) {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createInt8(nullDatum.asInt8()));
          } else if (type == TajoDataTypes.Type.INT4) {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createInt4(nullDatum.asInt4()));
          } else if (type == TajoDataTypes.Type.INT2) {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createInt2(nullDatum.asInt2()));
          } else if (type == TajoDataTypes.Type.FLOAT4) {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createFloat4(nullDatum.asFloat4()));
          } else if (type == TajoDataTypes.Type.FLOAT8) {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createFloat8(nullDatum.asFloat8()));
          } else {
            outTuple.put(resultColumnIdIndexes[tupleIndex], DatumFactory.createNullDatum());
          }
        }
      }
    }

    finished = true;
    first = false;

    return outTuple;
  }

  @Override
  public void close() throws IOException {
    if (aggregateExecs != null) {
      for (SortAggregateExec eachExec: aggregateExecs) {
        eachExec.close();
      }
    }
  }

  @Override
  public void init() throws IOException {
    super.init();

    if (aggregateExecs != null) {
      for (SortAggregateExec eachExec: aggregateExecs) {
        eachExec.init();
      }
    }
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
