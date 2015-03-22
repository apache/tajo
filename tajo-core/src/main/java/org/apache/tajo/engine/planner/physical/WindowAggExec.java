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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.expr.ConstEval;
import org.apache.tajo.plan.expr.WindowFunctionEval;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.LogicalWindowSpec;
import org.apache.tajo.plan.logical.WindowAggNode;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The sort-based window aggregation operator
 */
public class WindowAggExec extends UnaryPhysicalExec {
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
  private int [] startOffset;
  private int [] endOffset;

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

  public WindowAggExec(TaskAttemptContext context, WindowAggNode plan, PhysicalExec child) throws IOException {
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

      startOffset = new int[functions.length];
      endOffset = new int[functions.length];

      List<Column> additionalSortKeyColumns = Lists.newArrayList();
      Schema rewrittenSchema = new Schema(outSchema);
      for (int i = 0; i < functions.length; i++) {
        switch (functions[i].getLogicalWindowFrame().getStartBound().getBoundType()) {
          case PRECEDING:
          case FOLLOWING:
            startOffset[i] = functions[i].getLogicalWindowFrame().getStartBound().getNumber(); break;
          default:
        }

        switch (functions[i].getLogicalWindowFrame().getEndBound().getBoundType()) {
          case PRECEDING:
          case FOLLOWING:
            endOffset[i] = functions[i].getLogicalWindowFrame().getStartBound().getNumber(); break;
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
              // check if additionalSortKeyColumns already has that sort key
              if (!additionalSortKeyColumns.contains(sortSpec.getSortKey())) {
                additionalSortKeyColumns.add(sortSpec.getSortKey());
              }
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

    if (lastKey == null || lastKey.equals(currentKey)) { // if the current key is same to the previous key
      accumulatedInTuples.add(new VTuple(inTuple));

    } else {
      // if the current key is different from the previous key,
      // the current key belongs to the next window frame. preaccumulatingNextWindow() will
      // aggregate the current key for next window frame.
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
        comp = new BaseTupleComparator(inSchema, functions[idx].getSortSpecs());
        Collections.sort(accumulatedInTuples, comp);
        comp = new BaseTupleComparator(schemaForOrderBy, functions[idx].getSortSpecs());
        Collections.sort(evaluatedTuples, comp);
      }

      LogicalWindowSpec.LogicalWindowFrame.WindowFrameType windowFrameType = functions[idx].getLogicalWindowFrame().getFrameType();
      int frameStart = 0, frameEnd = accumulatedInTuples.size() - 1;
      int startOffset = functions[idx].getLogicalWindowFrame().getStartBound().getNumber();
      int endOffset = functions[idx].getLogicalWindowFrame().getEndBound().getNumber();

      switch (functions[idx].getFunctionType()) {
        case NONFRAMABLE:
        {
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
            for (int i = 0; i < evaluatedTuples.size(); i++) {
              Datum result = functions[idx].terminate(contexts[idx]);
              Tuple outTuple = evaluatedTuples.get(i);
              outTuple.put(nonFunctionColumnNum + idx, result);
            }
          }
          break;
        }
        case FRAMABLE:
        {
          String funcName = functions[idx].getName();

          for (int i = 0; i < accumulatedInTuples.size(); i++) {
            switch(windowFrameType) {
              case TO_CURRENT_ROW:
                frameEnd = i + endOffset; break;
              case FROM_CURRENT_ROW:
                frameStart = i + startOffset; break;
              case SLIDING_WINDOW:
                frameStart = i + startOffset;
                frameEnd = i + endOffset;
                break;
            }

            // As the number of built-in window functions that support window frame is small,
            // special treatment for each function seems to be reasonable
            Tuple outTuple = evaluatedTuples.get(i);
            Tuple inTuple = null;
            Datum result = NullDatum.get();
            if (funcName.equals("first_value")) {
              if (frameStart <= frameEnd && frameStart >= 0 && frameStart < accumulatedInTuples.size()) {
                inTuple = accumulatedInTuples.get(frameStart);
              }
            } else if (funcName.equals("last_value")) {
              if (frameStart <= frameEnd && frameEnd >= 0 && frameEnd < accumulatedInTuples.size()) {
                inTuple = accumulatedInTuples.get(frameEnd);
              }
            }

            if (inTuple != null) {
              functions[idx].merge(contexts[idx], inSchema, inTuple);
              result = functions[idx].terminate(contexts[idx]);
            }
            outTuple.put(nonFunctionColumnNum + idx, result);
          }
          break;
        }
        case AGGREGATION:
        {
          switch(windowFrameType) {
            case ENTIRE_PARTITION:
            {
              for (int i = 0; i < accumulatedInTuples.size(); i++) {
                Tuple inTuple = accumulatedInTuples.get(i);

                functions[idx].merge(contexts[idx], inSchema, inTuple);
              }

              Datum result = functions[idx].terminate(contexts[idx]);
              for (int j = 0; j < evaluatedTuples.size(); j++) {
                Tuple outTuple = evaluatedTuples.get(j);

                outTuple.put(nonFunctionColumnNum + idx, result);
              }
              break;
            }
            case TO_CURRENT_ROW:
            {
              if (endOffset > 0) {
                int i =0; int j = 0;
                for (; i < Math.min(accumulatedInTuples.size(), endOffset); i++) {
                  Tuple inTuple = accumulatedInTuples.get(i);

                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                }

                for (; i < accumulatedInTuples.size(); i++, j++) {
                  Tuple inTuple = accumulatedInTuples.get(i);
                  Tuple outTuple = evaluatedTuples.get(j);

                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                  Datum result = functions[idx].terminate(contexts[idx]);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }

                Datum result = functions[idx].terminate(contexts[idx]);
                for (; j < evaluatedTuples.size(); j++) {
                  Tuple outTuple = evaluatedTuples.get(j);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }
              } else {
                int i = endOffset; int j = 0;
                Datum result = functions[idx].terminate(contexts[idx]);
                for (; i < 0 && j < evaluatedTuples.size(); i++, j++) {
                  Tuple outTuple = evaluatedTuples.get(j);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }
                for (; j < evaluatedTuples.size(); i++, j++) {
                  Tuple inTuple = accumulatedInTuples.get(i);
                  Tuple outTuple = evaluatedTuples.get(j);

                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                  result = functions[idx].terminate(contexts[idx]);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }
              }
              break;
            }
            case FROM_CURRENT_ROW:
            {
              if (startOffset > 0) {
                int i = startOffset; int j = evaluatedTuples.size();
                for (; i > 0 && j > 0; i--, j--) {
                  Tuple outTuple = evaluatedTuples.get(j-1);
                  Datum result = functions[idx].terminate(contexts[idx]);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }

                for (i = accumulatedInTuples.size(); j > 0; i--, j--) {
                  Tuple inTuple = accumulatedInTuples.get(i-1);
                  functions[idx].merge(contexts[idx], inSchema, inTuple);

                  Tuple outTuple = evaluatedTuples.get(j-1);
                  Datum result = functions[idx].terminate(contexts[idx]);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }
              } else {
                int i = accumulatedInTuples.size(); int j = evaluatedTuples.size();
                for (; i > Math.max(0, accumulatedInTuples.size() + startOffset); i--) {
                  Tuple inTuple = accumulatedInTuples.get(i-1);
                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                }

                for (; i > 0; i--, j--) {
                  Tuple inTuple = accumulatedInTuples.get(i-1);
                  Tuple outTuple = evaluatedTuples.get(j-1);

                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                  Datum result = functions[idx].terminate(contexts[idx]);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }

                Datum result = functions[idx].terminate(contexts[idx]);
                for (; j > 0; j--) {
                  Tuple outTuple = evaluatedTuples.get(j-1);
                  outTuple.put(nonFunctionColumnNum + idx, result);
                }
              }
            }
            break;
            case SLIDING_WINDOW:
            {
              for (int i = 0; i < accumulatedInTuples.size(); i++) {
                frameStart = i + startOffset;
                frameEnd = i + endOffset;
                contexts[idx] = functions[idx].newContext();
                for (int j = Math.max(frameStart, 0); j < Math.min(frameEnd + 1, accumulatedInTuples.size()); j++) {
                  Tuple inTuple = accumulatedInTuples.get(j);
                  functions[idx].merge(contexts[idx], inSchema, inTuple);
                }
                Tuple outTuple = evaluatedTuples.get(i);
                Datum result = functions[idx].terminate(contexts[idx]);
                outTuple.put(nonFunctionColumnNum + idx, result);
              }
            }
            break;
          }
          break;
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
