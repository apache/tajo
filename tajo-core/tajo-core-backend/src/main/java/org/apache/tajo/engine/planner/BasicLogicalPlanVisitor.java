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

public class BasicLogicalPlanVisitor<CONTEXT, RESULT> implements LogicalPlanVisitor<CONTEXT, RESULT> {

  /**
   * The prehook is called before each node is visited.
   */
  @SuppressWarnings("unused")
  public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data) throws PlanningException {
  }

  /**
   * The posthook is called after each node is visited.
   */
  @SuppressWarnings("unused")
  public void postHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data)
      throws PlanningException {
  }

  public CONTEXT visit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block) throws PlanningException {
    visitChild(context, plan, block.getRoot(), new Stack<LogicalNode>());
    return context;
  }

  public CONTEXT visit(CONTEXT context, LogicalPlan plan, LogicalNode node) throws PlanningException {
    visitChild(context, plan, node, new Stack<LogicalNode>());
    return context;
  }

  /**
   * visit visits each logicalNode recursively.
   */
  public RESULT visitChild(CONTEXT context, LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    RESULT current;
    switch (node.getType()) {
      case ROOT:
        current = visitRoot(context, plan, (LogicalRootNode) node, stack);
        break;
      case PROJECTION:
        current = visitProjection(context, plan, (ProjectionNode) node, stack);
        break;
      case LIMIT:
        current = visitLimit(context, plan, (LimitNode) node, stack);
        break;
      case SORT:
        current = visitSort(context, plan, (SortNode) node, stack);
        break;
      case GROUP_BY:
        current = visitGroupBy(context, plan, (GroupbyNode) node, stack);
        break;
      case SELECTION:
        current = visitFilter(context, plan, (SelectionNode) node, stack);
        break;
      case JOIN:
        current = visitJoin(context, plan, (JoinNode) node, stack);
        break;
      case UNION:
        current = visitUnion(context, plan, (UnionNode) node, stack);
        break;
      case EXCEPT:
        current = visitExcept(context, plan, (ExceptNode) node, stack);
        break;
      case INTERSECT:
        current = visitIntersect(context, plan, (IntersectNode) node, stack);
        break;
      case TABLE_SUBQUERY:
        current = visitTableSubQuery(context, plan, (TableSubQueryNode) node, stack);
        break;
      case SCAN:
        current = visitScan(context, plan, (ScanNode) node, stack);
        break;
      case PARTITIONS_SCAN:
        current = visitScan(context, plan, (ScanNode) node, stack);
        break;
      case STORE:
        current = visitStoreTable(context, plan, (StoreTableNode) node, stack);
        break;
      case INSERT:
        current = visitInsert(context, plan, (InsertNode) node, stack);
        break;
      case CREATE_TABLE:
        current = visitCreateTable(context, plan, (CreateTableNode) node, stack);
        break;
      case DROP_TABLE:
        current = visitDropTable(context, plan, (DropTableNode) node, stack);
        break;
      default:
        throw new PlanningException("Unknown logical node type: " + node.getType());
    }

    return current;
  }

  @Override
  public RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitProjection(CONTEXT context, LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitLimit(CONTEXT context, LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitSort(CONTEXT context, LogicalPlan plan, SortNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitFilter(CONTEXT context, LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitJoin(CONTEXT context, LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getLeftChild(), stack);
    visitChild(context, plan, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitUnion(CONTEXT context, LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getLeftChild(), stack);
    visitChild(context, plan, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitExcept(CONTEXT context, LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getLeftChild(), stack);
    visitChild(context, plan, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIntersect(CONTEXT context, LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getLeftChild(), stack);
    visitChild(context, plan, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, TableSubQueryNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getSubQuery(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitScan(CONTEXT context, LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitPartitionedTableScan(CONTEXT context, LogicalPlan plan, PartitionedTableScanNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitInsert(CONTEXT context, LogicalPlan plan, InsertNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visitChild(context, plan, node.getSubQuery(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, CreateTableNode node, Stack<LogicalNode> stack) {
    return null;
  }

  @Override
  public RESULT visitDropTable(CONTEXT context, LogicalPlan plan, DropTableNode node, Stack<LogicalNode> stack) {
    return null;
  }
}
