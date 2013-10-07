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

public interface AlgebraVisitor<CONTEXT, RESULT> {
  RESULT visitProjection(CONTEXT ctx, Stack<OpType> stack, Projection expr) throws PlanningException;
  RESULT visitLimit(CONTEXT ctx, Stack<OpType> stack, Limit expr) throws PlanningException;
  RESULT visitSort(CONTEXT ctx, Stack<OpType> stack, Sort expr) throws PlanningException;
  RESULT visitHaving(CONTEXT ctx, Stack<OpType> stack, Having expr) throws PlanningException;
  RESULT visitGroupBy(CONTEXT ctx, Stack<OpType> stack, Aggregation expr) throws PlanningException;
  RESULT visitJoin(CONTEXT ctx, Stack<OpType> stack, Join expr) throws PlanningException;
  RESULT visitFilter(CONTEXT ctx, Stack<OpType> stack, Selection expr) throws PlanningException;
  RESULT visitUnion(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitExcept(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitIntersect(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitRelationList(CONTEXT ctx, Stack<OpType> stack, RelationList expr) throws PlanningException;
  RESULT visitTableSubQuery(CONTEXT ctx, Stack<OpType> stack, TablePrimarySubQuery expr) throws PlanningException;
  RESULT visitRelation(CONTEXT ctx, Stack<OpType> stack, Relation expr) throws PlanningException;
  RESULT visitCreateTable(CONTEXT ctx, Stack<OpType> stack, CreateTable expr) throws PlanningException;
  RESULT visitDropTable(CONTEXT ctx, Stack<OpType> stack, DropTable expr) throws PlanningException;
}
