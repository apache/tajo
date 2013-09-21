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

/**
 * It returns a list of node plan strings.
 */
public class ExplainLogicalPlanVisitor extends BasicLogicalPlanVisitor<ExplainLogicalPlanVisitor.Context> {

  public static class Context {
    public int maxDepth  = -1;
    public int depth = 0;
    public Stack<DepthString> explains = new Stack<DepthString>();

    public void add(int depth, PlanString planString) {
      maxDepth = Math.max(maxDepth, depth);
      explains.push(new DepthString(depth, planString));
    }

    public int getMaxDepth() {
      return this.maxDepth;
    }

    public Stack<DepthString> getExplains() {
      return explains;
    }
  }

  public static class DepthString {
    private int depth;
    private PlanString planStr;

    DepthString(int depth, PlanString planStr) {
      this.depth = depth;
      this.planStr = planStr;
    }

    public int getDepth() {
      return depth;
    }

    public PlanString getPlanString() {
      return planStr;
    }
  }

  public Context getBlockPlanStrings(LogicalPlan plan, String block) throws PlanningException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context explainContext = new Context();
    visitChild(plan, plan.getBlock(block).getRoot(), stack, explainContext);
    return explainContext;
  }

  @Override
  public LogicalNode visitRoot(LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    return visitChild(plan, node.getChild(), stack, context);
  }

  @Override
  public LogicalNode visitProjection(LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack,
                                     Context context) throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitLimit(LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitSort(LogicalPlan plan, SortNode node, Stack<LogicalNode> stack,
                               Context context) throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitGroupBy(LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack,
                                  Context context) throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  private LogicalNode visitUnaryNode(LogicalPlan plan, UnaryNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    context.depth++;
    stack.push(node);
    visitChild(plan, node.getChild(), stack, context);
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  private LogicalNode visitBinaryNode(LogicalPlan plan, BinaryNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    context.depth++;
    stack.push(node);
    visitChild(plan, node.getLeftChild(), stack, context);
    visitChild(plan, node.getRightChild(), stack, context);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitFilter(LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack,
                                 Context context) throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitJoin(LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    return visitBinaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitUnion(LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack,
                                Context context) throws PlanningException {
    return visitBinaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitExcept(LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack,
                                 Context context) throws PlanningException {
    return visitBinaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitIntersect(LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack,
                                    Context context) throws PlanningException {
    return visitBinaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitTableSubQuery(LogicalPlan plan, TableSubQueryNode node, Stack<LogicalNode> stack,
                                              Context context) throws PlanningException {
    context.depth++;
    stack.push(node);
    super.visitTableSubQuery(plan, node, stack, context);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());

    return node;
  }

  @Override
  public LogicalNode visitScan(LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack, Context context)
      throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitStoreTable(LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack, Context context) throws PlanningException {
    return visitUnaryNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitInsert(LogicalPlan plan, InsertNode node, Stack<LogicalNode> stack, Context context) throws PlanningException {
    context.depth++;
    stack.push(node);
    visitChild(plan, node.getSubQuery(), stack, context);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  public static String printDepthString(int maxDepth, DepthString planStr) {
    StringBuilder output = new StringBuilder();
    String pad = new String(new char[planStr.getDepth() * 3]).replace('\0', ' ');
    output.append(pad + planStr.getPlanString().getTitle()).append("\n");

    for (String str : planStr.getPlanString().getExplanations()) {
      output.append(pad).append("  => ").append(str).append("\n");
    }

    for (String str : planStr.getPlanString().getDetails()) {
      output.append(pad).append("  => ").append(str).append("\n");
    }
    return output.toString();
  }
}
