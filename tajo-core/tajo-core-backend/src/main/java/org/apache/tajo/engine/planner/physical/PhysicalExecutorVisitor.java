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

import org.apache.tajo.engine.planner.physical.*;

import java.util.Stack;

public interface PhysicalExecutorVisitor<CONTEXT, RESULT> {
  RESULT visitChild(PhysicalExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitSeqScan(SeqScanExec exec, Stack<PhysicalExec> stack, CONTEXT context);
  RESULT visitSort(SortExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitSortAggregation(SortAggregateExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitMergeJoin(MergeJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitSelection(SelectionExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitProjection(ProjectionExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitHashJoin(HashJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitHashSemiJoin(HashSemiJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitHashAntiJoin(HashAntiJoinExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
  RESULT visitLimit(LimitExec exec, Stack<PhysicalExec> stack, CONTEXT context) throws PhysicalPlanningException;
}
