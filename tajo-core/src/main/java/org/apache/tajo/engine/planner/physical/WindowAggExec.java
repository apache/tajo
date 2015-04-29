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
import org.apache.tajo.algebra.WindowSpec;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.WindowFunctionEval;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.logical.LogicalWindowSpec;
import org.apache.tajo.plan.logical.WindowAggNode;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.Tuple;
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

      List<Column> additionalSortKeyColumns = Lists.newArrayList();
      Schema rewrittenSchema = new Schema(outSchema);
      for (int i = 0; i < functions.length; i++) {
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

  @Override
  public void init() throws IOException {
    super.init();
    for (EvalNode functionEval : functions) {
      functionEval.bind(context.getEvalContext(), inSchema);
    }
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
    BaseTupleComparator comp;

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
      comp = null;
      if (orderedFuncFlags[idx]) {
        SortSpec[] sortSpecs = functions[idx].getSortSpecs();
        comp = new BaseTupleComparator(schemaForOrderBy, sortSpecs);
        Collections.sort(evaluatedTuples, comp);
        // following comparator is used later when RANGE unit is handled to check whether order by value is changed or not
        comp = new BaseTupleComparator(inSchema, sortSpecs);
        Collections.sort(accumulatedInTuples, comp);
      }

      LogicalWindowSpec.LogicalWindowFrame windowFrame = functions[idx].getLogicalWindowFrame();

      /*
         Following code handles evaluation of window functions with two nested switch statements
         Basically, ROWS handling has more cases then RANGE handling
         First switch distinguishes among
            1) built-in window functions without window frame support
            2) buiit-in window functions with window frame support
            3) aggregation functions, where window frame is supported

         In window frame support case, there exists four types of window frame which is also handled by switch statement
            a) Entire window partition
            b) From the start of window partition to the moving end point relative to current row position
            c) From the moving start point relative to current row position to the end of window partition
            d) Both start point and end point of window frame are moving relative to the current row position

         In the case of RANGE, there can be three window frame type
            i) From the start of window partition to the last row that has the same order by key as the current row
            ii) From the first row that has the same order by key as the current row to the end of window partition
            iii) For all rows that has the same order by key as the current row
       */
      switch (functions[idx].getFunctionType()) {
        case NONFRAMABLE:   // 1) built-in window functions without window frame support
        {
          evaluateNonframableWindowFunction(idx);
          break;
        }
        case FRAMABLE:  // 2) built-in window functions with window frame support
        {
          evaluateFramableWindowFunction(idx, windowFrame, comp);
          break;
        }
        case AGGREGATION:     // 3) aggregation functions, where window frame is supported
        {
          evaluateAggregationFunction(idx, windowFrame, comp);
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


  private void evaluateNonframableWindowFunction(int functionIndex) {
    for (int i = 0; i < accumulatedInTuples.size(); i++) {
      Tuple inTuple = accumulatedInTuples.get(i);
      Tuple outTuple = evaluatedTuples.get(i);

      functions[functionIndex].merge(contexts[functionIndex], inTuple);

      if (windowFuncFlags[functionIndex]) {
        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    }

    if (aggFuncFlags[functionIndex]) {
      for (int i = 0; i < evaluatedTuples.size(); i++) {
        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        Tuple outTuple = evaluatedTuples.get(i);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    }
  }

  private void evaluateFramableWindowFunction(int functionIndex, LogicalWindowSpec.LogicalWindowFrame windowFrame,
                                              BaseTupleComparator comp) {
    LogicalWindowSpec.LogicalWindowFrame.WindowFrameType windowFrameType = windowFrame.getFrameType();
    WindowSpec.WindowFrameUnit windowFrameUnit = windowFrame.getFrameUnit();
    int windowFrameStartOffset = windowFrame.getStartBound().getNumber();
    int windowFrameEndOffset = windowFrame.getEndBound().getNumber();

    String funcName = functions[functionIndex].getName();
    int sameStartRange = 0; int sameEndRange = 0;
    int frameStart = 0, frameEnd = accumulatedInTuples.size() - 1;
    int actualStart = 0;

    for (int i = 0; i < accumulatedInTuples.size(); i++) {
      switch(windowFrameType) {
        case TO_CURRENT_ROW:
          frameEnd = i + windowFrameEndOffset; break;
        case FROM_CURRENT_ROW:
          frameStart = i + windowFrameStartOffset; break;
        case SLIDING_WINDOW:
          frameStart = i + windowFrameStartOffset;
          frameEnd = i + windowFrameEndOffset;
          break;
      }

      // RANGE window frame SHOULD include all the rows that has the same order by value with the current row
      //   if comp == null, there is no order by and window frame is set as the entire partition
      if (comp != null && windowFrameUnit == WindowSpec.WindowFrameUnit.RANGE) {
        // move frame end point to the last row of the same order by value
        if (sameEndRange == 0) {
          sameEndRange = numOfTuplesWithTheSameKeyValue(frameEnd, comp, true);
        }
        sameEndRange --;
        frameEnd += sameEndRange;

        // move frame start point to the first row of the same order by value
        if (sameStartRange == 0) {
          sameStartRange = numOfTuplesWithTheSameKeyValue(frameStart, comp, true);
          actualStart = frameStart;
        }
        sameStartRange --;
        frameStart = actualStart;
      }
      // As the number of built-in window functions that support window frame is only three,
      // special treatment for each function seems to be reasonable.
      // There are three functions, first_value(), last_value(), and nth_value(), and
      // only nth_value is not implemented yet.
      Tuple inTuple = getFunctionSpecificInput(funcName, frameStart, frameEnd);
      Datum result = NullDatum.get();

      if (inTuple != null) {
        functions[functionIndex].merge(contexts[functionIndex], inTuple);
        result = functions[functionIndex].terminate(contexts[functionIndex]);
      }
      Tuple outTuple = evaluatedTuples.get(i);
      outTuple.put(nonFunctionColumnNum + functionIndex, result);
    }
  }

  // it returns the number of Tuples with the same order by key value
  // if only one Tuple for the given order by key exists, it returns 1
  private int numOfTuplesWithTheSameKeyValue(int startOffset, BaseTupleComparator comp, boolean isForward) {
    int numberOfTuples = 0;

    Tuple inTuple = accumulatedInTuples.get(startOffset);
    if (isForward) {
      do {
        numberOfTuples++;
        startOffset++;
      } while (startOffset < accumulatedInTuples.size() && comp.compare(accumulatedInTuples.get(startOffset), inTuple) == 0);
    } else {      // backward direction
      do {
        numberOfTuples++;
        startOffset--;
      } while (startOffset >= 0 && comp.compare(accumulatedInTuples.get(startOffset), inTuple) == 0);
    }

    return numberOfTuples;
  }

  /*
   * NOTE: Built-in window functions with frame support SHOULD be added in this function.
   * TODO: add support for nth_value()
   */
  private Tuple getFunctionSpecificInput(String funcName, int dataStart, int dataEnd) {
    if (funcName.equals("first_value")) {
      // adjust dataStart not to go beyond the partition
      dataStart = Math.max(dataStart, 0);
      // check the frame start is within the partition
      if (dataStart <= dataEnd && dataStart < accumulatedInTuples.size()) {
        return accumulatedInTuples.get(dataStart);
      }
    } else if (funcName.equals("last_value")) {
      // adjust dataEnd not to exceed the partition
      dataEnd = Math.min(dataEnd, accumulatedInTuples.size() - 1);
      // check the frame end is within the partition
      if (dataStart <= dataEnd && dataEnd >= 0) {
        return accumulatedInTuples.get(dataEnd);
      }
    }
    return null;
  }

  private void evaluateAggregationFunction(int functionIndex, LogicalWindowSpec.LogicalWindowFrame windowFrame,
                                           BaseTupleComparator comp) {
    LogicalWindowSpec.LogicalWindowFrame.WindowFrameType windowFrameType = windowFrame.getFrameType();
    WindowSpec.WindowFrameUnit windowFrameUnit = windowFrame.getFrameUnit();
    int windowFrameStartOffset = windowFrame.getStartBound().getNumber();
    int windowFrameEndOffset = windowFrame.getEndBound().getNumber();

    switch(windowFrameType) {
      case ENTIRE_PARTITION:
      {
        Datum result = aggregateInRange(functionIndex, 0, accumulatedInTuples.size());

        for (int j = 0; j < evaluatedTuples.size(); j++) {
          Tuple outTuple = evaluatedTuples.get(j);

          outTuple.put(nonFunctionColumnNum + functionIndex, result);
        }
        break;
      }
      case TO_CURRENT_ROW:
      {
        if (windowFrameUnit == WindowSpec.WindowFrameUnit.RANGE) {
          aggregateRangeFromStartToCurrent(functionIndex, comp);
        } else {
          aggregateRowsFromStartToCurrent(functionIndex, windowFrameEndOffset);
        }
        break;
      }
      /* For current_row to partition end case,
       * we calculate aggregation by feeding from last rows to current row, i.e., in reverse order.
       * Because it can exploit the incremental aggregation of function
       * Following code is almost the same as TO_CURRENT_ROW case except the row feeding order is reversed.
       **/
      case FROM_CURRENT_ROW:
      {
        if (windowFrameUnit == WindowSpec.WindowFrameUnit.RANGE) {
          aggregateRangeFromCurrentToEnd(functionIndex, comp);
        } else {
          aggregateRowsFromCurrentToEnd(functionIndex, windowFrameStartOffset);
        }
        break;
      }
      case SLIDING_WINDOW:
      {
        if (windowFrameUnit == WindowSpec.WindowFrameUnit.RANGE) {
          aggregateRangeInSlidingFrame(functionIndex, comp);
        } else {
          aggregateRowsInSlidingFrame(functionIndex, windowFrameStartOffset, windowFrameEndOffset);
        }
        break;
      }
    }
  }

  private void aggregateRangeFromStartToCurrent(int functionIndex, BaseTupleComparator comparator) {
    // If RANGE is used, it is guaranteed that the frame ends to current row with no offset
    int sameEndRange = 0;
    for (int i = 0; i < accumulatedInTuples.size(); i++) {
      // including all rows that has the same order by value
      if (sameEndRange == 0) {
        sameEndRange = aggregateTuplesWithSameKey(functionIndex, i, comparator, true);
      }
      sameEndRange --;

      Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
      Tuple outTuple = evaluatedTuples.get(i);
      outTuple.put(nonFunctionColumnNum + functionIndex, result);
    }
  }

  private void aggregateRowsFromStartToCurrent(int functionIndex, int offsetFromCurrentRow) {
    if (offsetFromCurrentRow > 0) {
      int inputTupleIndex = 0; int outputTupleIndex = 0;

      // input range: [0, offsetFromCurrentRow)
      // aggregated value would be reflected on every resulting rows of window function
      for (; inputTupleIndex < Math.min(accumulatedInTuples.size(), offsetFromCurrentRow); inputTupleIndex++) {
        Tuple inTuple = accumulatedInTuples.get(inputTupleIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);
      }

      // input range: [offsetFromCurrentRow, partition end)
      // output range: [0, partition end - offsetFromCurrentRow)
      // Each output is the result of partial aggregation up to corresponding input Tuple
      for (; inputTupleIndex < accumulatedInTuples.size(); inputTupleIndex++, outputTupleIndex++) {
        Tuple inTuple = accumulatedInTuples.get(inputTupleIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }

      // output range: (partition end - offsetFromCurrentRow, partition end)
      // output remain unchanged in this range
      Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
      for (; outputTupleIndex < evaluatedTuples.size(); outputTupleIndex++) {
        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    } else {
      int inputTupleIndex = offsetFromCurrentRow; int outputTupleIndex = 0;

      // output range: [0, -offsetFromCurrentRow)   --- note that offsetFromCurrentRow <= 0
      // As no corresponding input exists, aggregation result is the return value of function with no input
      Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
      for (; inputTupleIndex < 0 && outputTupleIndex < evaluatedTuples.size(); inputTupleIndex++, outputTupleIndex++) {
        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }

      // input range: [0, partition end + offsetFromCurrentRow)
      // output range: (-offsetFromCurrentRow, partition end]
      // Each output is the result of partial aggregation up to corresponding input Tuple
      for (; outputTupleIndex < evaluatedTuples.size(); inputTupleIndex++, outputTupleIndex++) {
        Tuple inTuple = accumulatedInTuples.get(inputTupleIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        result = functions[functionIndex].terminate(contexts[functionIndex]);
        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    }
  }

  private void aggregateRangeFromCurrentToEnd(int functionIndex, BaseTupleComparator comparator) {
    // If RANGE is used, it is guaranteed that the frame starts from current row with no offset
    int sameStartRange = 0;
    for (int i = accumulatedInTuples.size() - 1; i >= 0; i--) {
      // including all rows that has the same order by value
      if (sameStartRange == 0) {
        sameStartRange = aggregateTuplesWithSameKey(functionIndex, i, comparator, false);
      }
      sameStartRange --;

      Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
      Tuple outTuple = evaluatedTuples.get(i);
      outTuple.put(nonFunctionColumnNum + functionIndex, result);
    }
  }

  private void aggregateRowsFromCurrentToEnd(int functionIndex, int offsetFromCurrentRow) {
    if (offsetFromCurrentRow > 0) {
      int inputTupleIndex = offsetFromCurrentRow; int outputTupleIndex = evaluatedTuples.size() - 1;

      // output range: (partition end - offsetFromCurrentRow, partition end)
      // there is no input corresponding this output range,
      // As no corresponding input exists, aggregation result is the return value of function with no input
      for (; inputTupleIndex > 0 && outputTupleIndex >= 0; inputTupleIndex--, outputTupleIndex--) {
        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }

      // input range: [offsetFromCurrentRow, partition end)
      // output range: [0, partition end - offsetFromCurrentRow)
      // Each output is the result of partial aggregation up to corresponding input Tuple (in reverse order)
      for (inputTupleIndex = accumulatedInTuples.size() - 1; outputTupleIndex >= 0; inputTupleIndex--, outputTupleIndex--) {
        Tuple inTuple = accumulatedInTuples.get(inputTupleIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        Tuple outTuple = evaluatedTuples.get(outputTupleIndex);
        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    } else {
      // input range: (partition end + offsetFromCurrentRow, partition end)
      // aggregated value would be reflected on every resulting rows of window function
      int inputIndex = accumulatedInTuples.size() -  1; int outputIndex = evaluatedTuples.size() - 1;
      for (; inputIndex >= Math.max(0, accumulatedInTuples.size() + offsetFromCurrentRow); inputIndex--) {
        Tuple inTuple = accumulatedInTuples.get(inputIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);
      }

      // input range: [0, partition end + offsetFromCurrentRow)
      // output range: [-offsetFromCurrentRow, partition end)
      // Each output is the result of partial aggregation up to corresponding input Tuple
      for (; inputIndex >= 0; inputIndex--, outputIndex--) {
        Tuple inTuple = accumulatedInTuples.get(inputIndex);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
        Tuple outTuple = evaluatedTuples.get(outputIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }

      // output range: [0, -offsetFromCurrentRow)
      // output remain unchanged in this range
      Datum result = functions[functionIndex].terminate(contexts[functionIndex]);
      for (; outputIndex >= 0; outputIndex--) {
        Tuple outTuple = evaluatedTuples.get(outputIndex);
        outTuple.put(nonFunctionColumnNum + functionIndex, result);
      }
    }
  }

  private void aggregateRangeInSlidingFrame(int functionIndex, BaseTupleComparator comparator) {
    // the only case is RANGE BETWEEN CURRENT_ROW AND CURRENT_ROW
    int actualStart = 0; int actualEnd = 0;
    for (int i = 0; i < accumulatedInTuples.size(); i++) {
      // When the same key range is over,
      // re-calculate the start and end point of the same order by key range.
      // range is [actualStart, actualEnd), which means does not contain actualEnd in it.
      if (i == actualEnd) {
        actualStart = i;
        actualEnd = i + numOfTuplesWithTheSameKeyValue(i, comparator, true);
      }

      contexts[functionIndex] = functions[functionIndex].newContext();

      Datum result = aggregateInRange(functionIndex, actualStart, actualEnd);
      Tuple outTuple = evaluatedTuples.get(i);
      outTuple.put(nonFunctionColumnNum + functionIndex, result);
    }
  }

  private void aggregateRowsInSlidingFrame(int functionIndex, int startOffset, int endOffset) {
    int frameStart, frameEnd;
    for (int i = 0; i < accumulatedInTuples.size(); i++) {
      frameStart = i + startOffset;
      frameEnd = i + endOffset;
      contexts[functionIndex] = functions[functionIndex].newContext();

      // sliding frame should stay inside the partition, whose range is [0, accumulatedInTuples.size())
      Datum result = aggregateInRange(functionIndex, Math.max(frameStart, 0),
          Math.min(frameEnd + 1, accumulatedInTuples.size()));
      Tuple outTuple = evaluatedTuples.get(i);
      outTuple.put(nonFunctionColumnNum + functionIndex, result);
    }
  }

  // it calculates aggregation on [start, end)
  // and returns resulting Datum
  private Datum aggregateInRange(int functionIndex, int start, int end) {
    for (int i = start; i < end; i++) {
      Tuple inTuple = accumulatedInTuples.get(i);
      functions[functionIndex].merge(contexts[functionIndex], inTuple);
    }

    return functions[functionIndex].terminate(contexts[functionIndex]);
  }

  // it aggregates the Tuples of the same order by key value for the given direction
  //  and returns number of aggregated Tuples
  private int aggregateTuplesWithSameKey(int functionIndex, int startOffset,
                                         BaseTupleComparator comp, boolean isForward) {
    int numberOfBackwardTuples = 0;
    int numberOfForwardTuples = 0;
    int backwardOffset = startOffset;
    int forwardOffset = startOffset;

    Tuple inTuple;
    if (isForward) {
      do {
        inTuple = accumulatedInTuples.get(forwardOffset);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        numberOfForwardTuples++;
        forwardOffset++;
      } while (forwardOffset < accumulatedInTuples.size() &&
          comp.compare(accumulatedInTuples.get(forwardOffset), inTuple) == 0);
    } else {    // backward
      do {
        inTuple = accumulatedInTuples.get(backwardOffset);
        functions[functionIndex].merge(contexts[functionIndex], inTuple);

        numberOfBackwardTuples++;
        backwardOffset--;
      } while (backwardOffset >= 0 && comp.compare(accumulatedInTuples.get(backwardOffset), inTuple) == 0);

    }

    return isForward ? numberOfForwardTuples : numberOfBackwardTuples;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    noMoreTuples = false;
  }
}
