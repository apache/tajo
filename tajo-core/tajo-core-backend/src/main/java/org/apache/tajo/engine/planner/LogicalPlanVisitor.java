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

public interface LogicalPlanVisitor <T> {
  LogicalNode visitRoot(LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitProjection(LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitLimit(LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitSort(LogicalPlan plan, SortNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitGroupBy(LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitFilter(LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitJoin(LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitUnion(LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitExcept(LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitIntersect(LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitScan(LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
  LogicalNode visitStoreTable(LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException;
}
