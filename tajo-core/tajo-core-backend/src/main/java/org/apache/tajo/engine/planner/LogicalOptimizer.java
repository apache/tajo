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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.exception.InvalidQueryException;
import org.apache.tajo.engine.utils.SchemaUtil;

import java.util.*;

/**
 * This class optimizes a logical plan.
 */
public class LogicalOptimizer {
  private static Log LOG = LogFactory.getLog(LogicalOptimizer.class);

  private LogicalOptimizer() {
  }

  public static class OptimizationContext {
    TargetListManager targetListManager;

    public OptimizationContext(LogicalPlan plan, String blockName) {
      this.targetListManager = new TargetListManager(plan, blockName);
    }

    public OptimizationContext(LogicalPlan plan, Target [] targets) {
      this.targetListManager = new TargetListManager(plan, targets);
    }

    public TargetListManager getTargetListManager() {
      return this.targetListManager;
    }
  }

  public static LogicalNode optimize(LogicalPlan plan) throws PlanningException {
    LogicalNode toBeOptimized;

    toBeOptimized = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized) || !plan.getRootBlock().hasTableExpression()) {
      LOG.info("This query skips the logical optimization step.");
    } else {
      if(PlannerUtil.findTopNode(toBeOptimized, ExprType.SELECTION) != null) {
            pushSelection(toBeOptimized);
      }
      pushProjection(plan);
    }
    return toBeOptimized;
  }

  /**
   * This method pushes down the selection into the appropriate sub 
   * logical operators.
   * <br />
   * 
   * There are three operators that can have search conditions.
   * Selection, Join, and GroupBy clause.
   * However, the search conditions of Join and GroupBy cannot be pushed down 
   * into child operators because they can be used when the data layout change
   * caused by join and grouping relations.
   * <br />
   * 
   * However, some of the search conditions of selection clause can be pushed 
   * down into appropriate sub operators. Some comparison expressions on 
   * multiple relations are actually join conditions, and other expression 
   * on single relation can be used in a scan operator or an Index Scan 
   * operator.   
   *
   * @param plan
   */
  private static void pushSelection(LogicalNode plan) {
    SelectionNode selNode = (SelectionNode) PlannerUtil.findTopNode(plan,
        ExprType.SELECTION);
    Preconditions.checkNotNull(selNode);
    
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(selNode.getQual());
    pushSelectionRecursive(plan, Lists.newArrayList(cnf), stack);
  }

  private static void pushSelectionRecursive(LogicalNode plan,
                                             List<EvalNode> cnf, Stack<LogicalNode> stack) {
    
    switch(plan.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) plan;
        pushSelectionRecursive(rootNode.getSubNode(), cnf, stack);
        break;

    case SELECTION:
      SelectionNode selNode = (SelectionNode) plan;
      stack.push(selNode);
      pushSelectionRecursive(selNode.getSubNode(),cnf, stack);
      stack.pop();
      
      // remove the selection operator if there is no search condition 
      // after selection push.
      if(cnf.size() == 0) {
        LogicalNode node = stack.peek();
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setSubNode(selNode.getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      }
      break;
    case JOIN:
      JoinNode join = (JoinNode) plan;

      LogicalNode outer = join.getOuterNode();
      LogicalNode inner = join.getInnerNode();

      pushSelectionRecursive(outer, cnf, stack);
      pushSelectionRecursive(inner, cnf, stack);

      List<EvalNode> matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (PlannerUtil.canBeEvaluated(eval, plan)) {
          matched.add(eval);
        }
      }

      EvalNode qual = null;
      if (matched.size() > 1) {
        // merged into one eval tree
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else if (matched.size() == 1) {
        // if the number of matched expr is one
        qual = matched.get(0);
      }

      if (qual != null) {
        JoinNode joinNode = (JoinNode) plan;
        if (joinNode.hasJoinQual()) {
          EvalNode conjQual = EvalTreeUtil.
              transformCNF2Singleton(joinNode.getJoinQual(), qual);
          joinNode.setJoinQual(conjQual);
        } else {
          joinNode.setJoinQual(qual);
        }
        if (joinNode.getJoinType() == JoinType.CROSS_JOIN) {
          joinNode.setJoinType(JoinType.INNER);
        }
        cnf.removeAll(matched);
      }

      break;

    case SCAN:
      matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (PlannerUtil.canBeEvaluated(eval, plan)) {
          matched.add(eval);
        }
      }

      qual = null;
      if (matched.size() > 1) {
        // merged into one eval tree
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else if (matched.size() == 1) {
        // if the number of matched expr is one
        qual = matched.get(0);
      }

      if (qual != null) { // if a matched qual exists
        ScanNode scanNode = (ScanNode) plan;
        scanNode.setQual(qual);
      }

      cnf.removeAll(matched);
      break;

    default:
      stack.push(plan);
      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        pushSelectionRecursive(unary.getSubNode(), cnf, stack);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        pushSelectionRecursive(binary.getOuterNode(), cnf, stack);
        pushSelectionRecursive(binary.getInnerNode(), cnf, stack);
      }
      stack.pop();
      break;
    }
  }

  /**
   * This method pushes down the projection list into the appropriate and
   * below logical operators.
   * @param plan
   */
  private static void pushProjection(LogicalPlan plan)
      throws PlanningException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();

    OptimizationContext optCtx;

    if (plan.getRootBlock().getProjection() != null &&
        plan.getRootBlock().getProjection().isAllProjected()) {
      optCtx = new OptimizationContext(plan,
          plan.getRootBlock().getProjectionNode().getTargets());
    } else {
      optCtx = new OptimizationContext(plan, LogicalPlan.ROOT_BLOCK);
    }

    Collection<Column> finalSchema = plan.getRootBlock().getSchema().getColumns();

    pushProjectionRecursive(plan, optCtx, plan.getRootBlock().getRoot(), stack,
        new HashSet<Column>(finalSchema));

  }

  /**
   * This method visits all operators recursively and shrink output columns according to only necessary columns
   * at each operator.
   *
   * This method has three steps:
   * <ol>
   *   <li></li> collect column references necessary for each operator. For example, sort requires sortkeys,
   *   join requires join conditions, selection requires filter conditions, and groupby requires grouping keys and having
   *
   * 2) shrink the output schema of each operator so that the operator reduces the output columns according to
   * the necessary columns of their parent operators
   * 3) shrink the input schema of each operator according to the shrunk output schemas of the child operators.
   */
  private static LogicalNode pushProjectionRecursive(
      final LogicalPlan plan, final OptimizationContext optContext,
      final LogicalNode node, final Stack<LogicalNode> stack,
      final Set<Column> upperRequired) throws PlanningException {

    LogicalNode currentNode = null;

    switch (node.getType()) {
      // They need only simple work
      case ROOT:
      case STORE:
      case LIMIT:
        currentNode = pushDownCommonPost(plan, optContext, (UnaryNode) node, upperRequired, stack);
        break;

      // They need special preworks.
      case SELECTION:
        currentNode = pushDownSelection(plan, optContext, (SelectionNode) node, upperRequired, stack);
        break;
      case SORT:
        currentNode = pushDownSort(plan, optContext, (SortNode) node, upperRequired, stack);
        break;
      case UNION:
      case EXCEPT:
      case INTERSECT:
        currentNode = pushDownSetNode(plan, optContext, (UnionNode) node, upperRequired, stack);
        break;

      // Projection, GroupBy, Join, and Scan are all projectable.
      // A projectable operator can shrink or expand output columns through alias name and expressions.
      case PROJECTION:
        currentNode = pushDownProjection(plan, optContext, (ProjectionNode) node, upperRequired, stack);
        break;
      case GROUP_BY:
        currentNode = pushDownGroupBy(plan, optContext, (GroupbyNode) node, upperRequired, stack);
        break;
      case JOIN:
        currentNode = pushDownJoin(plan, optContext, (JoinNode) node, upperRequired, stack);
        break;
      case SCAN:
        currentNode = pushdownScanNode(optContext, (ScanNode) node, upperRequired, stack);
        break;

      default:
    }

    return currentNode;
  }

  private static LogicalNode pushDownCommonPost(LogicalPlan plan, OptimizationContext context, UnaryNode node,
                                                Set<Column> upperRequired, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    LogicalNode child = pushProjectionRecursive(plan, context,
        node.getSubNode(), stack, upperRequired);
    stack.pop();
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    if (node instanceof Projectable) {
      pushDownProjectablePost(context, node, upperRequired, isTopmostProjectable(stack));
    }
    return node;
  }

  private static LogicalNode pushDownProjection(LogicalPlan plan, OptimizationContext context,
                                                   ProjectionNode projNode, Set<Column> upperRequired,
                                                   Stack<LogicalNode> path) throws PlanningException {

    for (Target target : projNode.getTargets()) {
      upperRequired.add(target.getColumnSchema());
    }

    path.push(projNode);
    LogicalNode child = pushProjectionRecursive(plan, context, projNode.getSubNode(), path, upperRequired);
    path.pop();

    LogicalNode childNode = projNode.getSubNode();

    // If all expressions are evaluated in the child operators and the last operator is projectable,
    // ProjectionNode will not be necessary. It eliminates ProjectionNode.
    if (context.getTargetListManager().isAllEvaluated() && (childNode instanceof Projectable)) {
      LogicalNode parent = path.peek();
      // update the child node's output schemas
      child.setOutSchema(context.getTargetListManager().getUpdatedSchema());
      PlannerUtil.deleteNode(parent, projNode);
      return child;
    } else {
      projNode.setInSchema(child.getOutSchema());
      projNode.setTargets(context.getTargetListManager().getUpdatedTarget());
      return projNode;
    }
  }

  private static LogicalNode pushDownSelection(LogicalPlan plan, OptimizationContext context,
                                                 SelectionNode selectionNode, Set<Column> upperRequired,
                                                 Stack<LogicalNode> path) throws PlanningException {
    if (selectionNode.getQual() != null) {
      upperRequired.addAll(EvalTreeUtil.findDistinctRefColumns(selectionNode.getQual()));
    }

    return pushDownCommonPost(plan, context, selectionNode, upperRequired, path);
  }

  private static GroupbyNode pushDownGroupBy(LogicalPlan plan, OptimizationContext context, GroupbyNode groupbyNode,
                                             Set<Column> upperRequired, Stack<LogicalNode> stack)
      throws PlanningException {

    Set<Column> currentRequired = new HashSet<Column>(upperRequired);

    if (groupbyNode.hasHavingCondition()) {
      currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(groupbyNode.getHavingCondition()));
    }

    for (Target target : groupbyNode.getTargets()) {
      currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(target.getEvalTree()));
    }

    pushDownCommonPost(plan, context, groupbyNode, currentRequired, stack);
    return groupbyNode;
  }

  private static SortNode pushDownSort(LogicalPlan plan, OptimizationContext context, SortNode sortNode,
                                       Set<Column> upperRequired, Stack<LogicalNode> stack) throws PlanningException {

    for (SortSpec spec : sortNode.getSortKeys()) {
      upperRequired.add(spec.getSortKey());
    }

    pushDownCommonPost(plan, context, sortNode, upperRequired, stack);

    return sortNode;
  }

  private static JoinNode pushDownJoin(LogicalPlan plan, OptimizationContext context, JoinNode joinNode,
                                       Set<Column> upperRequired, Stack<LogicalNode> path)
      throws PlanningException {
    Set<Column> currentRequired = Sets.newHashSet(upperRequired);

    if (joinNode.hasTargets()) {
      EvalNode expr;
      for (Target target : joinNode.getTargets()) {
        expr = target.getEvalTree();
        if (expr.getType() != EvalNode.Type.FIELD) {
          currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(target.getEvalTree()));
        }
      }
    }

    if (joinNode.hasJoinQual()) {
      currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual()));
    }

    path.push(joinNode);
    LogicalNode outer = pushProjectionRecursive(plan, context,
        joinNode.getOuterNode(), path, currentRequired);
    LogicalNode inner = pushProjectionRecursive(plan, context,
        joinNode.getInnerNode(), path, currentRequired);
    path.pop();

    Schema merged = SchemaUtil.merge(outer.getOutSchema(), inner.getOutSchema());
    joinNode.setInSchema(merged);
    pushDownProjectablePost(context, joinNode, upperRequired, isTopmostProjectable(path));

    return joinNode;
  }

  private static BinaryNode pushDownSetNode(LogicalPlan plan, OptimizationContext context, UnionNode node,
                                            Set<Column> upperRequired, Stack<LogicalNode> stack) throws PlanningException {
    BinaryNode setNode = node;

    LogicalPlan.QueryBlock leftBlock = plan.getBlock(setNode.getOuterNode());
    OptimizationContext leftCtx = new OptimizationContext(plan,
        leftBlock.getTargetListManager().getUnEvaluatedTargets());
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(setNode.getInnerNode());
    OptimizationContext rightCtx = new OptimizationContext(plan,
        rightBlock.getTargetListManager().getUnEvaluatedTargets());

    stack.push(setNode);
    pushProjectionRecursive(plan, leftCtx, setNode.getOuterNode(), stack, upperRequired);
    pushProjectionRecursive(plan, rightCtx, setNode.getInnerNode(), stack, upperRequired);
    stack.pop();

    // if this is the final union, we assume that all targets are evalauted.
    // TODO - is it always correct?
    if (stack.peek().getType() != ExprType.UNION) {
      context.getTargetListManager().setEvaluatedAll();
    }

    return setNode;
  }

  private static ScanNode pushdownScanNode(OptimizationContext optContext, ScanNode scanNode,
                                           Set<Column> upperRequired, Stack<LogicalNode> stack)
      throws PlanningException {
    return (ScanNode) pushDownProjectablePost(optContext, scanNode, upperRequired, isTopmostProjectable(stack));
  }

  private static boolean isTopmostProjectable(Stack<LogicalNode> stack) {
    for (LogicalNode node : stack) {
      if (node.getType() == ExprType.JOIN || node.getType() == ExprType.GROUP_BY) {
        return false;
      }
    }

    return true;
  }

  private static LogicalNode pushDownProjectablePost(OptimizationContext context, LogicalNode node,
                                                     Set<Column> upperRequired, boolean last)
      throws PlanningException {
    TargetListManager targetListManager = context.getTargetListManager();
    EvalNode expr;

    List<Integer> newEvaluatedTargetIds = new ArrayList<Integer>();

    for (int i = 0; i < targetListManager.size(); i++) {
      expr = targetListManager.getTarget(i).getEvalTree();

      if (!targetListManager.isEvaluated(i) && PlannerUtil.canBeEvaluated(expr, node)) {

        if (node instanceof ScanNode) { // For ScanNode

          if (expr.getType() == EvalNode.Type.FIELD && !targetListManager.getTarget(i).hasAlias()) {
            targetListManager.setEvaluated(i);
          } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
            targetListManager.setEvaluated(i);
            newEvaluatedTargetIds.add(i);
          }

        } else if (node instanceof GroupbyNode) { // For GroupBy
          if (EvalTreeUtil.findDistinctAggFunction(expr).size() > 0 && expr.getType() != EvalNode.Type.FIELD) {
            targetListManager.setEvaluated(i);
            newEvaluatedTargetIds.add(i);
          }

        } else if (node instanceof JoinNode) {
          if (expr.getType() != EvalNode.Type.FIELD && EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
            targetListManager.setEvaluated(i);
            newEvaluatedTargetIds.add(i);
          }
        }
      }
    }

    Projectable projectable = (Projectable) node;
    if (last) {
      Preconditions.checkState(targetListManager.isAllEvaluated(), "Not all targets are evaluated.");
      projectable.setTargets(targetListManager.getTargets());
      targetListManager.getUpdatedTarget();
      node.setOutSchema(targetListManager.getUpdatedSchema());
    } else {
    // Preparing targets regardless of that the node has targets or not.
    // This part is required because some node does not have any targets,
    // if the node has the same input and output schemas.

    Target [] checkingTargets;
    if (!projectable.hasTargets()) {
      Schema outSchema = node.getOutSchema();
      checkingTargets = new Target[outSchema.getColumnNum() + newEvaluatedTargetIds.size()];
      PlannerUtil.schemaToTargets(outSchema, checkingTargets);
      int baseIdx = outSchema.getColumnNum();
      for (int i = 0; i < newEvaluatedTargetIds.size(); i++) {
        checkingTargets[baseIdx + i] = targetListManager.getTarget(newEvaluatedTargetIds.get(i));
      }
    } else {
      checkingTargets = projectable.getTargets();
    }

    List<Target> projectedTargets = new ArrayList<Target>();
    for (Column column : upperRequired) {
      for (Target target : checkingTargets) {

        if (target.hasAlias() && target.getAlias().equalsIgnoreCase(column.getQualifiedName())) {
            projectedTargets.add(target);
        } else {

          if (target.getColumnSchema().equals(column)) {
            projectedTargets.add(target);
          }
        }
      }
    }

    projectable.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    targetListManager.getUpdatedTarget();
    node.setOutSchema(PlannerUtil.targetToSchema(projectable.getTargets()));
    }

    return node;
  }
}