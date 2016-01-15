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

package org.apache.tajo.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.joinorder.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.BaseLogicalPlanRewriteEngine;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleProvider;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.ReflectionUtil;
import org.apache.tajo.util.graph.DirectedGraphCursor;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tajo.plan.LogicalPlan.BlockEdge;
import static org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm.getCost;

/**
 * This class optimizes a logical plan.
 */
@InterfaceStability.Evolving
public class LogicalOptimizer {
  private static final Log LOG = LogFactory.getLog(LogicalOptimizer.class.getName());

  private final CatalogService catalog;
  private final StorageService storage;
  private BaseLogicalPlanRewriteEngine rulesBeforeJoinOpt;
  private BaseLogicalPlanRewriteEngine rulesAfterToJoinOpt;
  private JoinOrderAlgorithm joinOrderAlgorithm = new GreedyHeuristicJoinOrderAlgorithm();

  public LogicalOptimizer(TajoConf conf, CatalogService catalog, StorageService storage) {

    this.catalog = catalog;
    this.storage = storage;

    // TODO: set the catalog instance to FilterPushdownRule
    Class clazz = conf.getClassVar(ConfVars.LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS);
    LogicalPlanRewriteRuleProvider provider = (LogicalPlanRewriteRuleProvider) ReflectionUtil.newInstance(clazz, conf);

    rulesBeforeJoinOpt = new BaseLogicalPlanRewriteEngine(storage);
    rulesBeforeJoinOpt.addRewriteRule(provider.getPreRules());
    rulesAfterToJoinOpt = new BaseLogicalPlanRewriteEngine(storage);
    rulesAfterToJoinOpt.addRewriteRule(provider.getPostRules());
  }

  @VisibleForTesting
  public LogicalNode optimize(LogicalPlan plan) throws TajoException {
    OverridableConf conf = new OverridableConf(new TajoConf(),
        ConfigKey.ConfigType.SESSION, ConfigKey.ConfigType.QUERY, ConfigKey.ConfigType.SYSTEM);
    return optimize(conf, plan);
  }

  public LogicalNode optimize(OverridableConf context, LogicalPlan plan) throws TajoException {
    rulesBeforeJoinOpt.rewrite(new LogicalPlanRewriteRuleContext(context, plan, catalog, storage));

    DirectedGraphCursor<String, BlockEdge> blockCursor =
            new DirectedGraphCursor<>(plan.getQueryBlockGraph(), plan.getRootBlock().getName());

    if (context == null || context.getBool(SessionVars.TEST_JOIN_OPT_ENABLED)) {
      // default is true
      while (blockCursor.hasNext()) {
        optimizeJoinOrder(plan, blockCursor.nextBlock());
      }
    } else {
      LOG.info("Skip join order optimization");
    }
    rulesAfterToJoinOpt.rewrite(new LogicalPlanRewriteRuleContext(context, plan, catalog, storage));
    return plan.getRootBlock().getRoot();
  }

  private void optimizeJoinOrder(LogicalPlan plan, String blockName) throws TajoException {
    LogicalPlan.QueryBlock block = plan.getBlock(blockName);

    if (block.hasNode(NodeType.JOIN)) {
      String originalOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);
      double nonOptimizedJoinCost = JoinCostComputer.computeCost(plan, block);

      // finding relations and filter expressions
      JoinGraphContext joinGraphContext = JoinGraphBuilder.buildJoinGraph(plan, block);

      // finding join order and restore remaining filters
      FoundJoinOrder order = joinOrderAlgorithm.findBestOrder(plan, block, joinGraphContext);

      // replace join node with FoundJoinOrder.
      JoinNode newJoinNode = order.getOrderedJoin();
      LogicalNode newNode = handleRemainingFiltersIfNecessary(joinGraphContext, plan, block, newJoinNode);

      JoinNode old = PlannerUtil.findTopNode(block.getRoot(), NodeType.JOIN);

      JoinTargetCollector collector = new JoinTargetCollector();
      Set<Target> targets = new LinkedHashSet<>();
      collector.visitJoin(targets, plan, block, old, new Stack<>());

      if (targets.size() == 0) {
        newJoinNode.setTargets(PlannerUtil.schemaToTargets(old.getOutSchema()));
      } else {
        newJoinNode.setTargets(new ArrayList<>(targets));
      }
      PlannerUtil.replaceNode(plan, block.getRoot(), old, newNode);
      // End of replacement logic

      String optimizedOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);
      block.addPlanHistory("Non-optimized join order: " + originalOrder + " (cost: " + nonOptimizedJoinCost + ")");
      block.addPlanHistory("Optimized join order    : " + optimizedOrder + " (cost: " + order.getCost() + ")");

      joinGraphContext.clear();
    }
  }

  /**
   * During join order optimization, every condition is checked whether it is a join condition or not.
   * So, after join order is optimized, there can be remaining conditions which are not join conditions.
   * This function handles such remaining conditions. It creates a new selection node for those conditions if required
   * or add them to the existing selection node.
   *
   * @param joinGraphContext join graph context
   * @param plan logical plan
   * @param block query block
   * @param newJoinNode the top join node after join order optimization
   * @return the top logical node after handling remaining conditions
   */
  private static LogicalNode handleRemainingFiltersIfNecessary(JoinGraphContext joinGraphContext,
                                                               LogicalPlan plan,
                                                               LogicalPlan.QueryBlock block,
                                                               JoinNode newJoinNode) {
    // Gather filters from remaining join edges
    Collection<JoinEdge> joinEdges = joinGraphContext.getJoinGraph().getEdgesAll();
    Collection<EvalNode> markAsEvaluated = new HashSet<>(joinGraphContext.getEvaluatedJoinConditions());
    markAsEvaluated.addAll(joinGraphContext.getEvaluatedJoinFilters());
    Set<EvalNode> remainingQuals = new HashSet<>(joinGraphContext.getCandidateJoinFilters());
    for (JoinEdge eachEdge : joinEdges) {
      remainingQuals.addAll(eachEdge.getJoinQual().stream().filter(eachQual -> !markAsEvaluated.contains(eachQual)).collect(Collectors.toList()));
    }

    if (!remainingQuals.isEmpty()) {
      LogicalNode topParent = PlannerUtil.findTopParentNode(block.getRoot(), NodeType.JOIN);
      if (topParent.getType() == NodeType.SELECTION) {
        SelectionNode topParentSelect = (SelectionNode) topParent;
        Set<EvalNode> filters = new HashSet<>();
        filters.addAll(new HashSet<>(Arrays.asList(AlgebraicUtil.toConjunctiveNormalFormArray(topParentSelect.getQual()))));
        filters.addAll(remainingQuals);
        topParentSelect.setQual(AlgebraicUtil.createSingletonExprFromCNF(
            filters.toArray(new EvalNode[filters.size()])));
        return newJoinNode;
      } else {
        SelectionNode newSelection = plan.createNode(SelectionNode.class);
        newSelection.setQual(AlgebraicUtil.createSingletonExprFromCNF(
            remainingQuals.toArray(new EvalNode[remainingQuals.size()])));
        newSelection.setChild(newJoinNode);
        return newSelection;
      }
    }


    return newJoinNode;
  }

  private static class JoinTargetCollector extends BasicLogicalPlanVisitor<Set<Target>, LogicalNode> {
    @Override
    public LogicalNode visitJoin(Set<Target> ctx, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack)
        throws TajoException {
      super.visitJoin(ctx, plan, block, node, stack);

      if (node.hasTargets()) {
        ctx.addAll(node.getTargets().stream().collect(Collectors.toList()));
      }
      return node;
    }
  }

  /**
   * The first phase of the join order optimization is building a join graph from the given query.
   * This initial join graph forms a tree which consists of only relation vertexes in an order of their occurrences in
   * the query. For example, let me suppose the following query.
   *
   * default> select * from t1 inner join t2 left outer join t3 inner join t4;
   *
   * In this example, the initial join graph is:
   *
   * t1 - (inner join) - t2 - (left outer join) - t3 - (inner join) - t4.
   *
   * This means that the default join order is left to right. Join queries can be always processed with the
   * default join order. This join order will be optimized by {@link JoinOrderAlgorithm}.
   *
   * JoinGraphBuilder builds an initial join graph as illustrated above.
   *
   */
  private static class JoinGraphBuilder extends BasicLogicalPlanVisitor<JoinGraphContext, LogicalNode> {
    private final static JoinGraphBuilder instance;

    static {
      instance = new JoinGraphBuilder();
    }

    /**
     * This is based on the assumtion that all join and filter conditions are placed on the right join and
     * scan operators. In other words, filter push down must be performed before this method.
     * Otherwise, this method may build incorrectly a join graph.
     */
    public static JoinGraphContext buildJoinGraph(LogicalPlan plan, LogicalPlan.QueryBlock block)
        throws TajoException {
      JoinGraphContext context = new JoinGraphContext();
      instance.visit(context, plan, block);
      return context;
    }

    @Override
    public LogicalNode visit(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                             LogicalNode node, Stack<LogicalNode> stack) throws TajoException {
      if (node.getType() != NodeType.TABLE_SUBQUERY) {
        super.visit(context, plan, block, node, stack);
      }

      return node;
    }

    @Override
    public LogicalNode visitFilter(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode node, Stack<LogicalNode> stack) throws TajoException {
      // all join predicate candidates must be collected before building the join tree except non-equality conditions
      // TODO: non-equality conditions should also be considered as join conditions after TAJO-1554
      List<EvalNode> candidateJoinQuals = new ArrayList<>();
      for (EvalNode eachEval : AlgebraicUtil.toConjunctiveNormalFormArray(node.getQual())) {
        if (EvalTreeUtil.isJoinQual(eachEval, false)) {
          candidateJoinQuals.add(eachEval);
        }
      }
      context.addCandidateJoinFilters(candidateJoinQuals);
      super.visitFilter(context, plan, block, node, stack);
      return node;
    }

    @Override
    public LogicalNode visitJoin(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack)
        throws TajoException {
      super.visitJoin(context, plan, block, joinNode, stack);

      if (joinNode.getJoinType() == JoinType.LEFT_SEMI || joinNode.getJoinType() == JoinType.LEFT_ANTI) {
        // In case of in-subquery, the left vertex must be the relation of the left column of the in qual.
        // In addition, the join qual can be evaluated only at the join node for in-subquery,
        // we don't need to consider moving it to other joins.

        BinaryEval joinQual = (BinaryEval) joinNode.getJoinQual();
        Preconditions.checkArgument(joinQual.getLeftExpr().getType() == EvalType.FIELD ||
            joinQual.getLeftExpr().getType() == EvalType.CAST);
        FieldEval leftColumn = null;
        if (joinQual.getLeftExpr().getType() == EvalType.FIELD) {
          leftColumn = joinQual.getLeftExpr();
        } else if (joinQual.getLeftExpr().getType() == EvalType.CAST) {
          leftColumn = (FieldEval) ((CastEval)joinQual.getLeftExpr()).getOperand();
        }
        RelationNode leftChild = block.getRelation(leftColumn.getQualifier());
        RelationNode rightChild = joinNode.getRightChild();
        RelationVertex leftVertex = new RelationVertex(leftChild);
        RelationVertex rightVertex = new RelationVertex(rightChild);

        context.getJoinGraph().addJoin(context, joinNode.getJoinSpec(), leftVertex, rightVertex);
      } else {

        // given a join node, find the relations which are nearest to the join in the query.
        RelationNode leftChild = findMostRightRelation(plan, block, joinNode.getLeftChild());
        RelationNode rightChild = findMostLeftRelation(plan, block, joinNode.getRightChild());
        RelationVertex leftVertex = new RelationVertex(leftChild);
        RelationVertex rightVertex = new RelationVertex(rightChild);

        JoinEdge edge = context.getJoinGraph().addJoin(context, joinNode.getJoinSpec(), leftVertex, rightVertex);

        // find all possible predicates for this join edge
        Set<EvalNode> joinConditions = new HashSet<>();
        if (joinNode.hasJoinQual()) {
          Set<EvalNode> originPredicates = joinNode.getJoinSpec().getPredicates();
          for (EvalNode predicate : joinNode.getJoinSpec().getPredicates()) {
            if (EvalTreeUtil.isJoinQual(block, leftVertex.getSchema(), rightVertex.getSchema(), predicate, false)) {
              if (JoinOrderingUtil.checkIfEvaluatedAtEdge(predicate, edge, true)) {
                joinConditions.add(predicate);
              }
            } else {
              joinConditions.add(predicate);
            }
          }
          // find predicates which cannot be evaluated at this join
          originPredicates.removeAll(joinConditions);
          context.addCandidateJoinConditions(originPredicates);
          originPredicates.clear();
          originPredicates.addAll(joinConditions);
        }

        joinConditions.addAll(JoinOrderingUtil.findJoinConditionForJoinVertex(context.getCandidateJoinConditions(), edge,
            true));
        joinConditions.addAll(JoinOrderingUtil.findJoinConditionForJoinVertex(context.getCandidateJoinFilters(), edge,
            false));
        context.markAsEvaluatedJoinConditions(joinConditions);
        context.markAsEvaluatedJoinFilters(joinConditions);
        edge.addJoinPredicates(joinConditions);
        if (edge.getJoinType() == JoinType.INNER && edge.getJoinQual().isEmpty()) {
          edge.getJoinSpec().setType(JoinType.CROSS);
        }
        
        if (PlannerUtil.isCommutativeJoinType(edge.getJoinType())) {
          JoinEdge commutativeEdge = context.getCachedOrNewJoinEdge(edge.getJoinSpec(), edge.getRightVertex(),
              edge.getLeftVertex());
          commutativeEdge.addJoinPredicates(joinConditions);
          context.getJoinGraph().addEdge(commutativeEdge.getLeftVertex(), commutativeEdge.getRightVertex(),
              commutativeEdge);
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

    public static String buildJoinOrderString(LogicalPlan plan, LogicalPlan.QueryBlock block) throws TajoException {
      StringBuilder originalOrder = new StringBuilder();
      instance.visit(originalOrder, plan, block);
      return originalOrder.toString();
    }

    @Override
    public LogicalNode visitJoin(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode joinNode,
                                 Stack<LogicalNode> stack)
        throws TajoException {
      stack.push(joinNode);
      sb.append("(");
      visit(sb, plan, block, joinNode.getLeftChild(), stack);
      sb.append(" ").append(getJoinNotation(joinNode.getJoinType())).append(" ");
      visit(sb, plan, block, joinNode.getRightChild(), stack);
      sb.append(")");
      stack.pop();
      return joinNode;
    }

    private static String getJoinNotation(JoinType joinType) {
      switch (joinType) {
      case CROSS: return "⋈";
      case INNER: return "⋈θ";
      case LEFT_OUTER: return "⟕";
      case RIGHT_OUTER: return "⟖";
      case FULL_OUTER: return "⟗";
      case LEFT_SEMI: return "⋉";
      case RIGHT_SEMI: return "⋊";
      case LEFT_ANTI: return "▷";
      }
      return ",";
    }

    @Override
    public LogicalNode visitTableSubQuery(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          TableSubQueryNode node, Stack<LogicalNode> stack) {
      sb.append(node.getTableName());
      return node;
    }

    public LogicalNode visitScan(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) {
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

    public static double computeCost(LogicalPlan plan, LogicalPlan.QueryBlock block) throws TajoException {
      CostContext costContext = new CostContext();
      instance.visit(costContext, plan, block);
      return costContext.accumulatedCost;
    }

    @Override
    public LogicalNode visitJoin(CostContext joinGraphContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack)
        throws TajoException {
      super.visitJoin(joinGraphContext, plan, block, joinNode, stack);

      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        EvalNode [] quals = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());
        filterFactor = Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, quals.length);
      }

      if (joinNode.getLeftChild() instanceof RelationNode) {
        joinGraphContext.accumulatedCost = getCost((RelationNode)joinNode.getLeftChild()) *
            getCost((LogicalNode)joinNode.getRightChild())* filterFactor;
      } else {
        joinGraphContext.accumulatedCost = joinGraphContext.accumulatedCost +
            (joinGraphContext.accumulatedCost * getCost((RelationNode)joinNode.getRightChild()) * filterFactor);
      }

      return joinNode;
    }
  }

  /**
   * Find the most left relation node in the join tree. For the join tree, please refer to {@link JoinGraphBuilder}.
   *
   * @param plan logical plan
   * @param block query block in which the given join node is involved
   * @param from logical node where the search starts
   * @return found relation
   * @throws TajoException
   */
  public static RelationNode findMostLeftRelation(LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode from)
      throws TajoException {
    RelationNodeFinderContext context = new RelationNodeFinderContext();
    context.findMostLeft = true;
    RelationNodeFinder finder = new RelationNodeFinder();
    finder.visit(context, plan, block, from, new Stack<>());
    return context.founds.isEmpty() ? null : context.founds.iterator().next();
  }

  /**
   * Find the most right relation node in the join tree. For the join tree, please refer to {@link JoinGraphBuilder}.
   *
   * @param plan logical plan
   * @param block query block in which the given join node is involved
   * @param from logical node where the search starts
   * @return found relation
   * @throws TajoException
   */
  public static RelationNode findMostRightRelation(LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode from)
      throws TajoException {
    RelationNodeFinderContext context = new RelationNodeFinderContext();
    context.findMostRight = true;
    RelationNodeFinder finder = new RelationNodeFinder();
    finder.visit(context, plan, block, from, new Stack<>());
    return context.founds.isEmpty() ? null : context.founds.iterator().next();
  }

  private static class RelationNodeFinderContext {
    private Set<RelationNode> founds = new HashSet<>();
    private boolean findMostLeft;
    private boolean findMostRight;
  }

  /**
   * RelationNodeFinder finds the most left/right vertex from the given node in the join graph.
   */
  private static class RelationNodeFinder extends BasicLogicalPlanVisitor<RelationNodeFinderContext,LogicalNode> {

    @Override
    public LogicalNode visit(RelationNodeFinderContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                             LogicalNode node, Stack<LogicalNode> stack) throws TajoException {
      if (node.getType() != NodeType.TABLE_SUBQUERY) {
        super.visit(context, plan, block, node, stack);
      }

      if (node instanceof RelationNode) {
        context.founds.add((RelationNode) node);
      }

      return node;
    }

    @Override
    public LogicalNode visitJoin(RelationNodeFinderContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws TajoException {
      stack.push(node);
      LogicalNode result = null;
      if (context.findMostLeft) {
        result = visit(context, plan, block, node.getLeftChild(), stack);
      }
      if (context.findMostRight) {
        result = visit(context, plan, block, node.getRightChild(), stack);
      }
      stack.pop();
      return result;
    }
  }
}