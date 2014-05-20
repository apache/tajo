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

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.planner.logical.*;

import java.util.Stack;

/**
 * It returns a list of node plan strings.
 */
public class ExplainLogicalPlanVisitor extends BasicLogicalPlanVisitor<ExplainLogicalPlanVisitor.Context, LogicalNode> {

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

  public Context getBlockPlanStrings(@Nullable LogicalPlan plan, LogicalNode node) throws PlanningException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context explainContext = new Context();
    visit(explainContext, plan, null, node, stack);
    return explainContext;
  }

  @Override
  public LogicalNode visitRoot(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalRootNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return visit(context, plan, block, node.getChild(), stack);
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, SortNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                                  Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                                  Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitWindowAgg(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, WindowAggNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  public LogicalNode visitDistinct(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, DistinctGroupbyNode node,
                                  Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  private LogicalNode visitUnaryNode(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     UnaryNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.depth++;
    stack.push(node);
    visit(context, plan, block, node.getChild(), stack);
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  private LogicalNode visitBinaryNode(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, BinaryNode node,
                                      Stack<LogicalNode> stack)
      throws PlanningException {
    context.depth++;
    stack.push(node);
    visit(context, plan, block, node.getLeftChild(), stack);
    visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                                Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitExcept(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ExceptNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitIntersect(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, IntersectNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, node, stack);
  }

  @Override
  public LogicalNode visitTableSubQuery(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.depth++;
    stack.push(node);
    visit(context, plan, block, node.getSubQuery(), new Stack<LogicalNode>());
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());

    return node;
  }

  @Override
  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitPartitionedTableScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitStoreTable(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, node, stack);
  }

  public LogicalNode visitCreateDatabase(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                         CreateDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  public LogicalNode visitDropDatabase(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                         DropDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitInsert(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, InsertNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    context.depth++;
    stack.push(node);
    super.visitInsert(context, plan, block, node, stack);
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
