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
  public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data)
      throws PlanningException {
  }

  /**
   * The posthook is called after each node is visited.
   */
  @SuppressWarnings("unused")
  public void postHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data)
      throws PlanningException {
  }

  public CONTEXT visit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block)
      throws PlanningException {
    visit(context, plan, block, block.getRoot(), new Stack<LogicalNode>());
    return context;
  }

  /**
   * visit visits each logicalNode recursively.
   */
  public RESULT visit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode node,
                      Stack<LogicalNode> stack)
      throws PlanningException {
    RESULT current;
    switch (node.getType()) {
      case ROOT:
        current = visitRoot(context, plan, block, (LogicalRootNode) node, stack);
        break;
      case EXPRS:
        return null;
      case PROJECTION:
        current = visitProjection(context, plan, block, (ProjectionNode) node, stack);
        break;
      case LIMIT:
        current = visitLimit(context, plan, block, (LimitNode) node, stack);
        break;
      case SORT:
        current = visitSort(context, plan, block, (SortNode) node, stack);
        break;
      case HAVING:
        current = visitHaving(context, plan, block, (HavingNode) node, stack);
        break;
      case GROUP_BY:
        current = visitGroupBy(context, plan, block, (GroupbyNode) node, stack);
        break;
      case WINDOW_AGG:
        current = visitWindowAgg(context, plan, block, (WindowAggNode) node, stack);
        break;
      case DISTINCT_GROUP_BY:
        current = visitDistinct(context, plan, block, (DistinctGroupbyNode) node, stack);
        break;
      case SELECTION:
        current = visitFilter(context, plan, block, (SelectionNode) node, stack);
        break;
      case JOIN:
        current = visitJoin(context, plan, block, (JoinNode) node, stack);
        break;
      case UNION:
        current = visitUnion(context, plan, block, (UnionNode) node, stack);
        break;
      case EXCEPT:
        current = visitExcept(context, plan, block, (ExceptNode) node, stack);
        break;
      case INTERSECT:
        current = visitIntersect(context, plan, block, (IntersectNode) node, stack);
        break;
      case TABLE_SUBQUERY:
        current = visitTableSubQuery(context, plan, block, (TableSubQueryNode) node, stack);
        break;
      case SCAN:
        current = visitScan(context, plan, block, (ScanNode) node, stack);
        break;
      case PARTITIONS_SCAN:
        current = visitPartitionedTableScan(context, plan, block, (PartitionedTableScanNode) node, stack);
        break;
      case STORE:
        current = visitStoreTable(context, plan, block, (StoreTableNode) node, stack);
        break;
      case INSERT:
        current = visitInsert(context, plan, block, (InsertNode) node, stack);
        break;
      case CREATE_DATABASE:
        current = visitCreateDatabase(context, plan, block, (CreateDatabaseNode) node, stack);
        break;
      case DROP_DATABASE:
        current = visitDropDatabase(context, plan, block, (DropDatabaseNode) node, stack);
        break;
      case CREATE_TABLE:
        current = visitCreateTable(context, plan, block, (CreateTableNode) node, stack);
        break;
      case DROP_TABLE:
        current = visitDropTable(context, plan, block, (DropTableNode) node, stack);
        break;
      case ALTER_TABLESPACE:
        current = visitAlterTablespace(context, plan, block, (AlterTablespaceNode) node, stack);
        break;
      case ALTER_TABLE:
        current = visitAlterTable(context, plan, block, (AlterTableNode) node, stack);
        break;
      case TRUNCATE_TABLE:
        current = visitTruncateTable(context, plan, block, (TruncateTableNode) node, stack);
        break;
      default:
        throw new PlanningException("Unknown logical node type: " + node.getType());
    }

    return current;
  }

  @Override
  public RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalRootNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitProjection(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ProjectionNode node,
                                Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitLimit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitSort(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, SortNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitHaving(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitWindowAgg(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, WindowAggNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  public RESULT visitDistinct(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DistinctGroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitFilter(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitJoin(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getLeftChild(), stack);
    visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitUnion(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    LogicalPlan.QueryBlock leftBlock = plan.getBlock(node.getLeftChild());
    RESULT result = visit(context, plan, leftBlock, leftBlock.getRoot(), stack);
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(node.getRightChild());
    visit(context, plan, rightBlock, rightBlock.getRoot(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitExcept(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ExceptNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getLeftChild(), stack);
    visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIntersect(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, IntersectNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getLeftChild(), stack);
    visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    LogicalPlan.QueryBlock childBlock = plan.getBlock(node.getSubQuery());
    RESULT result = visit(context, plan, childBlock, childBlock.getRoot(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitPartitionedTableScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, StoreTableNode node,
                                Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitInsert(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, InsertNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitCreateDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    CreateDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitDropDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DropDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, CreateTableNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    RESULT result = null;
    stack.push(node);
    if (node.hasSubQuery()) {
      result = visit(context, plan, block, node.getChild(), stack);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitDropTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, DropTableNode node,
                               Stack<LogicalNode> stack) {
    return null;
  }

  @Override
  public RESULT visitAlterTablespace(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     AlterTablespaceNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitAlterTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, AlterTableNode node,
                                 Stack<LogicalNode> stack) {
        return null;
    }

  @Override
  public RESULT visitTruncateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TruncateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }
}
