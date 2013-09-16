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

import org.apache.tajo.algebra.*;

import java.util.Stack;

public interface AlgebraVisitor<T1, T2> {
  T2 visitProjection(T1 ctx, Stack<OpType> stack, Projection expr) throws PlanningException;
  T2 visitLimit(T1 ctx, Stack<OpType> stack, Limit expr) throws PlanningException;
  T2 visitSort(T1 ctx, Stack<OpType> stack, Sort expr) throws PlanningException;
  T2 visitGroupBy(T1 ctx, Stack<OpType> stack, Aggregation expr) throws PlanningException;
  T2 visitJoin(T1 ctx, Stack<OpType> stack, Join expr) throws PlanningException;
  T2 visitFilter(T1 ctx, Stack<OpType> stack, Selection expr) throws PlanningException;
  T2 visitUnion(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  T2 visitExcept(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  T2 visitIntersect(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  T2 visitRelationList(T1 ctx, Stack<OpType> stack, RelationList expr) throws PlanningException;
  T2 visitTableSubQuery(T1 ctx, Stack<OpType> stack, TableSubQuery expr) throws PlanningException;
  T2 visitRelation(T1 ctx, Stack<OpType> stack, Relation expr) throws PlanningException;
  T2 visitCreateTable(T1 ctx, Stack<OpType> stack, CreateTable expr) throws PlanningException;
  T2 visitDropTable(T1 ctx, Stack<OpType> stack, DropTable expr) throws PlanningException;
}
