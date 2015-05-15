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

import org.apache.tajo.engine.planner.PhysicalPlanningException;

import java.util.Stack;

public class BasicPhysicalExecutorVisitor<CONTEXT, RESULT> implements PhysicalExecutorVisitor<CONTEXT, RESULT> {

  public RESULT visit(PhysicalExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {

    // Please keep all physical executors except for abstract class.
    // They should be ordered in an lexicography order of their names for easy code maintenance.
    if (exec instanceof BNLJoinExec) {
      return visitBNLJoin(context, (BNLJoinExec) exec, stack);
    } else if (exec instanceof BSTIndexScanExec) {
      return visitBSTIndexScan(context, (BSTIndexScanExec) exec, stack);
    } else if (exec instanceof EvalExprExec) {
      return visitEvalExpr(context, (EvalExprExec) exec, stack);
    } else if (exec instanceof ExternalSortExec) {
      return visitExternalSort(context, (ExternalSortExec) exec, stack);
    } else if (exec instanceof HashAggregateExec) {
      return visitHashAggregate(context, (HashAggregateExec) exec, stack);
    } else if (exec instanceof HashBasedColPartitionStoreExec) {
      return visitHashBasedColPartitionStore(context, (HashBasedColPartitionStoreExec) exec, stack);
    } else if (exec instanceof HashFullOuterJoinExec) {
      return visitHashFullOuterJoin(context, (HashFullOuterJoinExec) exec, stack);
    } else if (exec instanceof HashJoinExec) {
      return visitHashJoin(context, (HashJoinExec) exec, stack);
    } else if (exec instanceof HashLeftAntiJoinExec) {
      return visitHashLeftAntiJoin(context, (HashLeftAntiJoinExec) exec, stack);
    } else if (exec instanceof HashLeftOuterJoinExec) {
      return visitHashLeftOuterJoin(context, (HashLeftOuterJoinExec) exec, stack);
    } else if (exec instanceof HashLeftSemiJoinExec) {
      return visitLeftHashSemiJoin(context, (HashLeftSemiJoinExec) exec, stack);
    } else if (exec instanceof HashShuffleFileWriteExec) {
      return visitHashShuffleFileWrite(context, (HashShuffleFileWriteExec) exec, stack);
    } else if (exec instanceof HavingExec) {
      return visitHaving(context, (HavingExec) exec, stack);
    } else if (exec instanceof LimitExec) {
      return visitLimit(context, (LimitExec) exec, stack);
    } else if (exec instanceof MemSortExec) {
      return visitMemSort(context, (MemSortExec) exec, stack);
    } else if (exec instanceof MergeFullOuterJoinExec) {
      return visitMergeFullOuterJoin(context, (MergeFullOuterJoinExec) exec, stack);
    } else if (exec instanceof MergeJoinExec) {
      return visitMergeJoin(context, (MergeJoinExec) exec, stack);
    } else if (exec instanceof NLJoinExec) {
      return visitNLJoin(context, (NLJoinExec) exec, stack);
    } else if (exec instanceof ProjectionExec) {
      return visitProjection(context, (ProjectionExec) exec, stack);
    } else if (exec instanceof RangeShuffleFileWriteExec) {
      return visitRangeShuffleFileWrite(context, (RangeShuffleFileWriteExec) exec, stack);
    } else if (exec instanceof RightOuterMergeJoinExec) {
      return visitRightOuterMergeJoin(context, (RightOuterMergeJoinExec) exec, stack);
    } else if (exec instanceof SelectionExec) {
      return visitSelection(context, (SelectionExec) exec, stack);
    } else if (exec instanceof SeqScanExec) {
      return visitSeqScan(context, (SeqScanExec) exec, stack);
    } else if (exec instanceof SortAggregateExec) {
      return visitSortAggregate(context, (SortAggregateExec) exec, stack);
    } else if (exec instanceof SortBasedColPartitionStoreExec) {
      return visitSortBasedColPartitionStore(context, (SortBasedColPartitionStoreExec) exec, stack);
    } else if (exec instanceof StoreTableExec) {
      return visitStoreTable(context, (StoreTableExec) exec, stack);
    }

    throw new PhysicalPlanningException("Unsupported Type: " + exec.getClass().getSimpleName());
  }

  private RESULT visitUnaryExecutor(CONTEXT context, UnaryPhysicalExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    stack.push(exec);
    RESULT r = visit(exec.getChild(), stack, context);
    stack.pop();
    return r;
  }

  private RESULT visitBinaryExecutor(CONTEXT context, BinaryPhysicalExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    stack.push(exec);
    RESULT r = visit(exec.getLeftChild(), stack, context);
    visit(exec.getRightChild(), stack, context);
    stack.pop();
    return r;
  }

  @Override
  public RESULT visitBNLJoin(CONTEXT context, BNLJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitBSTIndexScan(CONTEXT context, BSTIndexScanExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return null;
  }

  @Override
  public RESULT visitEvalExpr(CONTEXT context, EvalExprExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return null;
  }

  @Override
  public RESULT visitExternalSort(CONTEXT context, ExternalSortExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashAggregate(CONTEXT context, HashAggregateExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashBasedColPartitionStore(CONTEXT context, HashBasedColPartitionStoreExec exec,
                                                Stack<PhysicalExec> stack) throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashFullOuterJoin(CONTEXT context, HashFullOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashJoin(CONTEXT context, HashJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashLeftAntiJoin(CONTEXT context, HashLeftAntiJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashLeftOuterJoin(CONTEXT context, HashLeftOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitLeftHashSemiJoin(CONTEXT context, HashLeftSemiJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHashShuffleFileWrite(CONTEXT context, HashShuffleFileWriteExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitHaving(CONTEXT context, HavingExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitLimit(CONTEXT context, LimitExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitMemSort(CONTEXT context, MemSortExec exec, Stack<PhysicalExec> stack) throws
      PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitMergeFullOuterJoin(CONTEXT context, MergeFullOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitMergeJoin(CONTEXT context, MergeJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitNLJoin(CONTEXT context, NLJoinExec exec, Stack<PhysicalExec> stack) throws
      PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitProjection(CONTEXT context, ProjectionExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitRangeShuffleFileWrite(CONTEXT context, RangeShuffleFileWriteExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitRightOuterMergeJoin(CONTEXT context, RightOuterMergeJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitSelection(CONTEXT context, SelectionExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitSeqScan(CONTEXT context, SeqScanExec exec, Stack<PhysicalExec> stack) {
    return null;
  }

  @Override
  public RESULT visitSortAggregate(CONTEXT context, SortAggregateExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitSortBasedColPartitionStore(CONTEXT context, SortBasedColPartitionStoreExec exec,
                                                Stack<PhysicalExec> stack) throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }

  @Override
  public RESULT visitStoreTable(CONTEXT context, StoreTableExec exec, Stack<PhysicalExec> stack) throws PhysicalPlanningException {
    return visitUnaryExecutor(context, exec, stack);
  }
}
