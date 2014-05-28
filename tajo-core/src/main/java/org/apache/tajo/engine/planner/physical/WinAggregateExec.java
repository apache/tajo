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
import org.apache.tajo.algebra.WindowSpecExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.WindowFunctionEval;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.WindowAggNode;
import org.apache.tajo.engine.planner.logical.WindowSpec;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameStartBoundType;
import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;

/**
 * This is the sort-based window operator.
 */
public class WinAggregateExec extends UnaryPhysicalExec {
  // plan information
  protected final int outputColumnNum;
  protected final int nonFunctionColumnNum;
  protected final int nonFunctionColumns[];

  protected final int functionNum;
  protected final WindowFunctionEval functions[];

  protected Schema schemaForOrderBy;
  protected int sortKeyColumns[];
  protected final boolean hasPartitionKeys;
  protected final int partitionKeyNum;
  protected final int partitionKeyIds[];

  // for evaluation
  protected FunctionContext contexts [];
  protected Tuple lastKey = null;
  protected boolean noMoreTuples = false;
  private boolean [] orderedFuncFlags;
  private boolean [] aggFuncFlags;
  private boolean [] windowFuncFlags;
  private boolean [] endUnboundedFollowingFlags;
  private boolean [] endCurrentRowFlags;

  private boolean endCurrentRow = false;

  // operator state
  enum WindowState {
    NEW_WINDOW,
    ACCUMULATING_WINDOW,
    EVALUATION,
    RETRIEVING_FROM_WINDOW,
    END_OF_TUPLE
  }

  // Transient state
  boolean firstTime = true;
  List<Tuple> evaluatedTuples = null;
  List<Tuple> accumulatedInTuples = null;
  List<Tuple> nextAccumulatedProjected = null;
  List<Tuple> nextAccumulatedInTuples = null;
  WindowState state = WindowState.NEW_WINDOW;
  Iterator<Tuple> tupleInFrameIterator = null;

  public WinAggregateExec(TaskAttemptContext context, WindowAggNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);

    if (plan.hasPartitionKeys()) {
      final Column[] keyColumns = plan.getPartitionKeys();
      partitionKeyNum = keyColumns.length;
      partitionKeyIds = new int[partitionKeyNum];
      Column col;
      for (int idx = 0; idx < plan.getPartitionKeys().length; idx++) {
        col = keyColumns[idx];
        partitionKeyIds[idx] = inSchema.getColumnId(col.getQualifiedName());
      }
      hasPartitionKeys = true;
    } else {
      partitionKeyNum = 0;
      partitionKeyIds = null;
      hasPartitionKeys = false;
    }

    if (plan.hasAggFunctions()) {
      functions = plan.getWindowFunctions();
      functionNum = functions.length;

      orderedFuncFlags = new boolean[functions.length];
      windowFuncFlags = new boolean[functions.length];
      aggFuncFlags = new boolean[functions.length];

      endUnboundedFollowingFlags = new boolean[functions.length];
      endCurrentRowFlags = new boolean[functions.length];

      List<Column> additionalSortKeyColumns = Lists.newArrayList();
      Schema rewrittenSchema = new Schema(outSchema);
      for (int i = 0; i < functions.length; i++) {
        WindowSpec.WindowEndBound endBound = functions[i].getWindowFrame().getEndBound();
        switch (endBound.getBoundType()) {
        case CURRENT_ROW:
          endCurrentRowFlags[i] = true; break;
        case UNBOUNDED_FOLLOWING:
          endUnboundedFollowingFlags[i] = true; break;
        default:
        }

        switch (functions[i].getFuncDesc().getFuncType()) {
        case AGGREGATION:
        case DISTINCT_AGGREGATION:
          aggFuncFlags[i] = true; break;
        case WINDOW:
          windowFuncFlags[i] = true; break;
        default:
        }

        if (functions[i].hasSortSpecs()) {
          orderedFuncFlags[i] = true;

          for (SortSpec sortSpec : functions[i].getSortSpecs()) {
            if (!rewrittenSchema.contains(sortSpec.getSortKey())) {
              additionalSortKeyColumns.add(sortSpec.getSortKey());
            }
          }
        }
      }

      sortKeyColumns = new int[additionalSortKeyColumns.size()];
      schemaForOrderBy = new Schema(outSchema);
      for (int i = 0; i < additionalSortKeyColumns.size(); i++) {
        sortKeyColumns[i] = i;
        schemaForOrderBy.addColumn(additionalSortKeyColumns.get(i));
      }
    } else {
      functions = new WindowFunctionEval[0];
      functionNum = 0;
      schemaForOrderBy = outSchema;
    }


    nonFunctionColumnNum = plan.getTargets().length - functionNum;
    nonFunctionColumns = new int[nonFunctionColumnNum];
    for (int idx = 0; idx < plan.getTargets().length - functionNum; idx++) {
      nonFunctionColumns[idx] = inSchema.getColumnId(plan.getTargets()[idx].getCanonicalName());
    }

    outputColumnNum = nonFunctionColumnNum + functionNum;
  }

  private void transition(WindowState state) {
    this.state = state;
  }

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey = null;
    Tuple readTuple = null;

    while(!context.isStopped() && state != WindowState.END_OF_TUPLE) {

      if (state == WindowState.NEW_WINDOW) {
        initWindow();
        transition(WindowState.ACCUMULATING_WINDOW);
      }

      if (state != WindowState.RETRIEVING_FROM_WINDOW) { // read an input tuple and build a partition key
        readTuple = child.next();

        if (readTuple == null) { // the end of tuple
          noMoreTuples = true;
          transition(WindowState.EVALUATION);
        }

        if (readTuple != null && hasPartitionKeys) { // get a key tuple
          currentKey = new VTuple(partitionKeyIds.length);
          for (int i = 0; i < partitionKeyIds.length; i++) {
            currentKey.put(i, readTuple.get(partitionKeyIds[i]));
          }
        }
      }

      if (state == WindowState.ACCUMULATING_WINDOW) {
        accumulatingWindow(currentKey, readTuple);
      }

      if (state == WindowState.EVALUATION) {
        evaluationWindowFrame();

        tupleInFrameIterator = evaluatedTuples.iterator();
        transition(WindowState.RETRIEVING_FROM_WINDOW);
      }

      if (state == WindowState.RETRIEVING_FROM_WINDOW) {
        if (tupleInFrameIterator.hasNext()) {
          return tupleInFrameIterator.next();
        } else {
          finalizeWindow();
        }
      }
    }

    return null;
  }

  private void initWindow() {
    if (firstTime) {
      accumulatedInTuples = Lists.newArrayList();

      contexts = new FunctionContext[functionNum];
      for(int evalIdx = 0; evalIdx < functionNum; evalIdx++) {
        contexts[evalIdx] = functions[evalIdx].newContext();
      }
      firstTime = false;
    }
  }

  private void accumulatingWindow(Tuple currentKey, Tuple inTuple) {
    if (lastKey == null || lastKey.equals(currentKey)) {
      accumulatedInTuples.add(new VTuple(inTuple));

    } else {
      preAccumulatingNextWindow(inTuple);
      transition(WindowState.EVALUATION);
    }

    lastKey = currentKey;
  }

  private void preAccumulatingNextWindow(Tuple inTuple) {
    Tuple projectedTuple = new VTuple(outSchema.size());
    for(int idx = 0; idx < nonFunctionColumnNum; idx++) {
      projectedTuple.put(idx, inTuple.get(nonFunctionColumns[idx]));
    }
    nextAccumulatedProjected = Lists.newArrayList();
    nextAccumulatedProjected.add(projectedTuple);
    nextAccumulatedInTuples = Lists.newArrayList();
    nextAccumulatedInTuples.add(new VTuple(inTuple));
  }

  private void evaluationWindowFrame() {
    TupleComparator comp;

    evaluatedTuples = new ArrayList<Tuple>();

    for (int i = 0; i <accumulatedInTuples.size(); i++) {
      Tuple inTuple = accumulatedInTuples.get(i);

      Tuple projectedTuple = new VTuple(schemaForOrderBy.size());
      for (int c = 0; c < nonFunctionColumnNum; c++) {
        projectedTuple.put(c, inTuple.get(nonFunctionColumns[c]));
      }
      for (int c = 0; c < sortKeyColumns.length; c++) {
        projectedTuple.put(outputColumnNum + c, inTuple.get(sortKeyColumns[c]));
      }

      evaluatedTuples.add(projectedTuple);
    }

    for (int idx = 0; idx < functions.length; idx++) {
      if (orderedFuncFlags[idx]) {
        comp = new TupleComparator(inSchema, functions[idx].getSortSpecs());
        Collections.sort(accumulatedInTuples, comp);
        comp = new TupleComparator(schemaForOrderBy, functions[idx].getSortSpecs());
        Collections.sort(evaluatedTuples, comp);
      }

      for (int i = 0; i < accumulatedInTuples.size(); i++) {
        Tuple inTuple = accumulatedInTuples.get(i);
        Tuple outTuple = evaluatedTuples.get(i);

        functions[idx].merge(contexts[idx], inSchema, inTuple);

        if (windowFuncFlags[idx]) {
          Datum result = functions[idx].terminate(contexts[idx]);
          outTuple.put(nonFunctionColumnNum + idx, result);
        }
      }

      if (aggFuncFlags[idx]) {
        Datum result = functions[idx].terminate(contexts[idx]);
        for (int i = 0; i < evaluatedTuples.size(); i++) {
          Tuple outTuple = evaluatedTuples.get(i);
          outTuple.put(nonFunctionColumnNum + idx, result);
        }
      }
    }
  }

  private void finalizeWindow() {
    evaluatedTuples.clear();
    accumulatedInTuples.clear();

    if (noMoreTuples) {
      transition(WindowState.END_OF_TUPLE);
    } else {
      accumulatedInTuples = nextAccumulatedInTuples;

      contexts = new FunctionContext[functionNum];
      for(int evalIdx = 0; evalIdx < functionNum; evalIdx++) {
        contexts[evalIdx] = functions[evalIdx].newContext();
      }
      transition(WindowState.NEW_WINDOW);
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    noMoreTuples = false;
  }
}
