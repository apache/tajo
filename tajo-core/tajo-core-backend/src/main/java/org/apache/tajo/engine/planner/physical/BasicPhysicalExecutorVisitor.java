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

import java.util.Stack;

public class BasicPhysicalExecutorVisitor<CONTEXT, RESULT> implements PhysicalExecutorVisitor<CONTEXT, RESULT> {

  @Override
  public RESULT visitChild(PhysicalExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {

    if (exec instanceof SeqScanExec) {
      return visitSeqScan((SeqScanExec) exec, stack, context);
    } else if (exec instanceof SelectionExec) {
      return visitSelection((SelectionExec) exec, stack, context);
    } else if (exec instanceof SortExec) {
      return visitSort((SortExec) exec, stack, context);
    } else if (exec instanceof SortAggregateExec) {
      return visitSortAggregation((SortAggregateExec) exec, stack, context);
    } else if (exec instanceof ProjectionExec) {
      return visitProjection((ProjectionExec) exec, stack, context);
    } else if (exec instanceof HashJoinExec) {
      return visitHashJoin((HashJoinExec) exec, stack, context);
    } else if (exec instanceof HashAntiJoinExec) {
      return visitHashAntiJoin((HashAntiJoinExec) exec, stack, context);
    } else if (exec instanceof HashSemiJoinExec) {
      return visitHashSemiJoin((HashSemiJoinExec) exec, stack, context);
    } else if (exec instanceof LimitExec) {
      return visitLimit((LimitExec) exec, stack, context);
    } else {
      throw new PhysicalPlanningException("Unsupported Type: " + exec.getClass().getSimpleName());
    }
  }

  private RESULT visitUnaryExecutor(UnaryPhysicalExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    stack.push(exec);
    RESULT r = visitChild(exec.getChild(), stack, context);
    stack.pop();
    return r;
  }

  private RESULT visitBinaryExecutor(BinaryPhysicalExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    stack.push(exec);
    RESULT r = visitChild(exec.getLeftChild(), stack, context);
    visitChild(exec.getRightChild(), stack, context);
    stack.pop();
    return r;
  }

  @Override
  public RESULT visitSortAggregation(SortAggregateExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitSeqScan(SeqScanExec exec, Stack<PhysicalExec> stack, CONTEXT context) {
    return null;
  }

  @Override
  public RESULT visitSort(SortExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitMergeJoin(MergeJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitSelection(SelectionExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitProjection(ProjectionExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitHashJoin(HashJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitHashSemiJoin(HashSemiJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitHashAntiJoin(HashAntiJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitBinaryExecutor(exec, stack, context);
  }

  @Override
  public RESULT visitLimit(LimitExec exec, Stack<PhysicalExec> stack, CONTEXT context)
      throws PhysicalPlanningException {
    return visitUnaryExecutor(exec, stack, context);
  }
}
