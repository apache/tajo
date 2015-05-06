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

public interface PhysicalExecutorVisitor<CONTEXT, RESULT> {

  RESULT visitBNLJoin(CONTEXT context, BNLJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitBSTIndexScan(CONTEXT context, BSTIndexScanExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitEvalExpr(CONTEXT context, EvalExprExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitExternalSort(CONTEXT context, ExternalSortExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashAggregate(CONTEXT context, HashAggregateExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashBasedColPartitionStore(CONTEXT context, HashBasedColPartitionStoreExec exec,
                                         Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashFullOuterJoin(CONTEXT context, HashFullOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashJoin(CONTEXT context, HashJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashLeftAntiJoin(CONTEXT context, HashLeftAntiJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashLeftOuterJoin(CONTEXT context, HashLeftOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitLeftHashSemiJoin(CONTEXT context, HashLeftSemiJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHashShuffleFileWrite(CONTEXT context, HashShuffleFileWriteExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitHaving(CONTEXT context, HavingExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitLimit(CONTEXT context, LimitExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitMemSort(CONTEXT context, MemSortExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitMergeFullOuterJoin(CONTEXT context, MergeFullOuterJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitMergeJoin(CONTEXT context, MergeJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitNLJoin(CONTEXT context, NLJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitProjection(CONTEXT context, ProjectionExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitRangeShuffleFileWrite(CONTEXT context, RangeShuffleFileWriteExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitRightOuterMergeJoin(CONTEXT context, RightOuterMergeJoinExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitSelection(CONTEXT context, SelectionExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitSeqScan(CONTEXT context, SeqScanExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitSortAggregate(CONTEXT context, SortAggregateExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitSortBasedColPartitionStore(CONTEXT context, SortBasedColPartitionStoreExec exec,
                                         Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;

  RESULT visitStoreTable(CONTEXT context, StoreTableExec exec, Stack<PhysicalExec> stack)
      throws PhysicalPlanningException;
}
