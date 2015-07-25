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

package org.apache.tajo.plan.visitor;

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;

import java.util.Stack;

public interface LogicalPlanVisitor<CONTEXT, RESULT> {

  RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalRootNode node,
                   Stack<LogicalNode> stack) throws TajoException;

  RESULT visitSetSession(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, SetSessionNode node,
                         Stack<LogicalNode> stack) throws TajoException;

  RESULT visitEvalExpr(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, EvalExprNode node,
                       Stack<LogicalNode> stack) throws TajoException;

  RESULT visitProjection(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ProjectionNode node,
                         Stack<LogicalNode> stack) throws TajoException;

  RESULT visitLimit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                    Stack<LogicalNode> stack) throws TajoException;

  RESULT visitSort(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, SortNode node,
                   Stack<LogicalNode> stack) throws TajoException;

  RESULT visitHaving(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                      Stack<LogicalNode> stack) throws TajoException;

  RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                      Stack<LogicalNode> stack) throws TajoException;
  RESULT visitWindowAgg(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, WindowAggNode node,
                      Stack<LogicalNode> stack) throws TajoException;
  RESULT visitDistinctGroupby(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DistinctGroupbyNode node,
                              Stack<LogicalNode> stack) throws TajoException;

  RESULT visitFilter(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                     Stack<LogicalNode> stack) throws TajoException;

  RESULT visitJoin(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                   Stack<LogicalNode> stack) throws TajoException;

  RESULT visitUnion(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                    Stack<LogicalNode> stack) throws TajoException;

  RESULT visitExcept(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ExceptNode node,
                     Stack<LogicalNode> stack) throws TajoException;

  RESULT visitIntersect(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, IntersectNode node,
                        Stack<LogicalNode> stack) throws TajoException;

  RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, TableSubQueryNode node,
                            Stack<LogicalNode> stack) throws TajoException;

  RESULT visitScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                   Stack<LogicalNode> stack) throws TajoException;

  RESULT visitIndexScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, IndexScanNode node,
                        Stack<LogicalNode> stack) throws TajoException;

  RESULT visitPartitionedTableScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   PartitionedTableScanNode node, Stack<LogicalNode> stack) throws TajoException;

  RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, StoreTableNode node,
                         Stack<LogicalNode> stack) throws TajoException;

  RESULT visitInsert(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, InsertNode node,
                     Stack<LogicalNode> stack) throws TajoException;

  RESULT visitCreateDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, CreateDatabaseNode node,
                          Stack<LogicalNode> stack) throws TajoException;

  RESULT visitDropDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DropDatabaseNode node,
                             Stack<LogicalNode> stack) throws TajoException;

  RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, CreateTableNode node,
                          Stack<LogicalNode> stack) throws TajoException;

  RESULT visitDropTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DropTableNode node,
                        Stack<LogicalNode> stack) throws TajoException;

  RESULT visitAlterTablespace(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, AlterTablespaceNode node,
                          Stack<LogicalNode> stack) throws TajoException;

  RESULT visitAlterTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, AlterTableNode node,
                         Stack<LogicalNode> stack) throws TajoException;

  RESULT visitCreateIndex(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, CreateIndexNode node,
                               Stack<LogicalNode> stack) throws TajoException;

  RESULT visitDropIndex(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DropIndexNode node,
                        Stack<LogicalNode> stack) throws TajoException;

  RESULT visitTruncateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, TruncateTableNode node,
                         Stack<LogicalNode> stack) throws TajoException;
}
