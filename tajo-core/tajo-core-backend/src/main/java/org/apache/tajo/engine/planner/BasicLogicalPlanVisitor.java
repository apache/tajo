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

public class BasicLogicalPlanVisitor<T> implements LogicalPlanVisitor<T> {

  /**
   * The prehook is called before each node is visited.
   */
  @SuppressWarnings("unused")
  public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, T data) throws PlanningException {
  }

  /**
   * The posthook is called after each node is visited.
   */
  @SuppressWarnings("unused")
  public void postHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
  }

  /**
   * visitChild visits each logicalNode recursively.
   */
  public LogicalNode visitChild(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    LogicalNode current;
    switch (node.getType()) {
      case ROOT:
        current = visitRoot(plan, (LogicalRootNode) node, stack, data);
        break;
      case PROJECTION:
        current = visitProjection(plan, (ProjectionNode) node, stack, data);
        break;
      case LIMIT:
        current = visitLimit(plan, (LimitNode) node, stack, data);
        break;
      case SORT:
        current = visitSort(plan, (SortNode) node, stack, data);
        break;
      case GROUP_BY:
        current = visitGroupBy(plan, (GroupbyNode) node, stack, data);
        break;
      case SELECTION:
        current = visitFilter(plan, (SelectionNode) node, stack, data);
        break;
      case JOIN:
        current = visitJoin(plan, (JoinNode) node, stack, data);
        break;
      case UNION:
        current = visitUnion(plan, (UnionNode) node, stack, data);
        break;
      case EXCEPT:
        current = visitExcept(plan, (ExceptNode) node, stack, data);
        break;
      case INTERSECT:
        current = visitIntersect(plan, (IntersectNode) node, stack, data);
        break;
      case SCAN:
        current = visitScan(plan, (ScanNode) node, stack, data);
        break;
      case STORE:
        current = visitStoreTable(plan, (StoreTableNode) node, stack, data);
        break;
      default:
        current = node; // nothing to do for others
    }

    return current;
  }

  @Override
  public LogicalNode visitRoot(LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitProjection(LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitLimit(LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitSort(LogicalPlan plan, SortNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitGroupBy(LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitFilter(LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitJoin(LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getLeftChild(), stack, data);
    visitChild(plan, node.getRightChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitUnion(LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getLeftChild(), stack, data);
    visitChild(plan, node.getRightChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitExcept(LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getLeftChild(), stack, data);
    visitChild(plan, node.getRightChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitIntersect(LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getLeftChild(), stack, data);
    visitChild(plan, node.getRightChild(), stack, data);
    stack.pop();
    return node;
  }

  @Override
  public LogicalNode visitScan(LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    return node;
  }

  @Override
  public LogicalNode visitStoreTable(LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack, T data)
      throws PlanningException {
    stack.push(node);
    visitChild(plan, node.getChild(), stack, data);
    stack.pop();
    return node;
  }
}
