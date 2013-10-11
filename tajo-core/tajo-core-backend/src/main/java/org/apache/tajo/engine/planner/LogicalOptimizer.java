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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.graph.DirectedGraphCursor;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.logical.join.FoundJoinOrder;
import org.apache.tajo.engine.planner.logical.join.GreedyHeuristicJoinOrderAlgorithm;
import org.apache.tajo.engine.planner.logical.join.JoinGraph;
import org.apache.tajo.engine.planner.logical.join.JoinOrderAlgorithm;
import org.apache.tajo.engine.planner.rewrite.BasicQueryRewriteEngine;
import org.apache.tajo.engine.planner.rewrite.FilterPushDownRule;
import org.apache.tajo.engine.planner.rewrite.ProjectionPushDownRule;

import java.util.Collection;
import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.engine.planner.LogicalPlan.BlockEdge;
import static org.apache.tajo.engine.planner.logical.join.GreedyHeuristicJoinOrderAlgorithm.getCost;

/**
 * This class optimizes a logical plan.
 */
@InterfaceStability.Evolving
public class LogicalOptimizer {
  private BasicQueryRewriteEngine rulesBeforeJoinOpt;
  private BasicQueryRewriteEngine rulesAfterToJoinOpt;
  private JoinOrderAlgorithm joinOrderAlgorithm = new GreedyHeuristicJoinOrderAlgorithm();

  public LogicalOptimizer() {
    rulesBeforeJoinOpt = new BasicQueryRewriteEngine();
    rulesBeforeJoinOpt.addRewriteRule(new FilterPushDownRule());

    rulesAfterToJoinOpt = new BasicQueryRewriteEngine();
    rulesAfterToJoinOpt.addRewriteRule(new ProjectionPushDownRule());
  }

  public LogicalNode optimize(LogicalPlan plan) throws PlanningException {
    rulesBeforeJoinOpt.rewrite(plan);

    DirectedGraphCursor<String, BlockEdge> blockCursor =
        new DirectedGraphCursor<String, BlockEdge>(plan.getQueryBlockGraph(), plan.getRootBlock().getName());

    while(blockCursor.hasNext()) {
      optimizeJoinOrder(plan, blockCursor.nextBlock());
    }

    rulesAfterToJoinOpt.rewrite(plan);
    return plan.getRootBlock().getRoot();
  }

  private void optimizeJoinOrder(LogicalPlan plan, String blockName) throws PlanningException {
    LogicalPlan.QueryBlock block = plan.getBlock(blockName);

    if (block.hasJoinNode()) {
      String originalOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);
      double nonOptimizedJoinCost = JoinCostComputer.computeCost(plan, block);

      // finding relations and filter expressions
      JoinGraphContext joinGraphContext = JoinGraphBuilder.buildJoinGraph(plan, block);

      // finding join order and restore remain filter order
      FoundJoinOrder order = joinOrderAlgorithm.findBestOrder(plan, block,
          joinGraphContext.joinGraph, joinGraphContext.quals, joinGraphContext.relationsWithoutQual);
      block.setJoinNode(order.getOrderedJoin());

      String optimizedOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);

      block.addHistory("Non-optimized join order: " + originalOrder + " (cost: " + nonOptimizedJoinCost + ")");
      block.addHistory("Optimized join order    : " + optimizedOrder + " (cost: " + order.getCost() + ")");
    }
  }

  private static class JoinGraphContext {
    JoinGraph joinGraph = new JoinGraph();
    Set<EvalNode> quals = Sets.newHashSet();
    Set<String> relationsWithoutQual = Sets.newHashSet();
  }

  private static class JoinGraphBuilder extends BasicLogicalPlanVisitor<JoinGraphContext, LogicalNode> {
    private final static JoinGraphBuilder instance;

    static {
      instance = new JoinGraphBuilder();
    }

    public static JoinGraphContext buildJoinGraph(LogicalPlan plan, LogicalPlan.QueryBlock block)
        throws PlanningException {
      JoinGraphContext joinGraphContext = new JoinGraphContext();
      instance.visit(joinGraphContext, plan, block);
      return joinGraphContext;
    }

    public LogicalNode visitFilter(JoinGraphContext context, LogicalPlan plan, SelectionNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      super.visitFilter(context, plan, node, stack);
      context.quals.addAll(Lists.newArrayList(EvalTreeUtil.getConjNormalForm(node.getQual())));
      return node;
    }

    @Override
    public LogicalNode visitJoin(JoinGraphContext joinGraphContext, LogicalPlan plan, JoinNode joinNode,
                                 Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(joinGraphContext, plan, joinNode, stack);
      if (joinNode.hasJoinQual()) {
        Collection<EvalNode> nonJoinQual =
            joinGraphContext.joinGraph.addJoin(joinNode.getJoinType(), joinNode.getJoinQual());
        joinGraphContext.quals.addAll(nonJoinQual);
      } else {
        LogicalNode leftChild = joinNode.getLeftChild();
        if (leftChild instanceof RelationNode) {
          RelationNode rel = (RelationNode) leftChild;
          joinGraphContext.relationsWithoutQual.add(rel.getCanonicalName());
        }
      }
      return joinNode;
    }
  }

  public static class JoinOrderStringBuilder extends BasicLogicalPlanVisitor<StringBuilder, LogicalNode> {
    private static final JoinOrderStringBuilder instance;
    static {
      instance = new JoinOrderStringBuilder();
    }

    public static JoinOrderStringBuilder getInstance() {
      return instance;
    }

    public static String buildJoinOrderString(LogicalPlan plan, LogicalPlan.QueryBlock block) throws PlanningException {
      StringBuilder originalOrder = new StringBuilder();
      instance.visit(originalOrder, plan, block);
      return originalOrder.toString();
    }

    @Override
    public LogicalNode visitJoin(StringBuilder sb, LogicalPlan plan, JoinNode joinNode,
                                 Stack<LogicalNode> stack)
        throws PlanningException {
      stack.push(joinNode);
      sb.append("(");
      visitChild(sb, plan, joinNode.getLeftChild(), stack);
      sb.append(",");
      visitChild(sb, plan, joinNode.getRightChild(), stack);
      sb.append(")");
      stack.pop();
      return joinNode;
    }

    @Override
    public LogicalNode visitTableSubQuery(StringBuilder sb, LogicalPlan plan, TableSubQueryNode node,
                                          Stack<LogicalNode> stack) {
      sb.append(node.getTableName());
      return node;
    }

    public LogicalNode visitScan(StringBuilder sb, LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack) {
      sb.append(node.getTableName());
      return node;
    }
  }

  private static class CostContext {
    double accumulatedCost = 0;
  }

  public static class JoinCostComputer extends BasicLogicalPlanVisitor<CostContext, LogicalNode> {
    private static final JoinCostComputer instance;

    static {
      instance = new JoinCostComputer();
    }

    public static double computeCost(LogicalPlan plan, LogicalPlan.QueryBlock block) throws PlanningException {
      CostContext costContext = new CostContext();
      instance.visit(costContext, plan, block);
      return costContext.accumulatedCost;
    }

    @Override
    public LogicalNode visitJoin(CostContext joinGraphContext, LogicalPlan plan, JoinNode joinNode,
                                 Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(joinGraphContext, plan, joinNode, stack);

      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        EvalNode [] quals = EvalTreeUtil.getConjNormalForm(joinNode.getJoinQual());
        filterFactor = Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, quals.length);
      }

      if (joinNode.getLeftChild() instanceof RelationNode) {
        joinGraphContext.accumulatedCost = getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild())
            * filterFactor;
      } else {
        joinGraphContext.accumulatedCost = joinGraphContext.accumulatedCost +
            (joinGraphContext.accumulatedCost * getCost(joinNode.getRightChild()) * filterFactor);
      }

      return joinNode;
    }
  }
}