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

package org.apache.tajo.engine.planner;

import org.apache.tajo.engine.planner.logical.*;

import java.util.Stack;

public interface LogicalPlanVisitor <CONTEXT, RESULT> {
  RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitProjection(CONTEXT context, LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitLimit(CONTEXT context, LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitSort(CONTEXT context, LogicalPlan plan, SortNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitFilter(CONTEXT context, LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitJoin(CONTEXT context, LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitUnion(CONTEXT context, LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitExcept(CONTEXT context, LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitIntersect(CONTEXT context, LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, TableSubQueryNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitScan(CONTEXT context, LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitInsert(CONTEXT context, LogicalPlan plan, InsertNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, CreateTableNode node, Stack<LogicalNode> stack)
      throws PlanningException;
  RESULT visitDropTable(CONTEXT context, LogicalPlan plan, DropTableNode node, Stack<LogicalNode> stack)
      throws PlanningException;
}
