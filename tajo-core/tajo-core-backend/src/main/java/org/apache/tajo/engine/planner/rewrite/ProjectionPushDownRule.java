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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;

import java.util.*;

public class ProjectionPushDownRule extends BasicLogicalPlanVisitor<ProjectionPushDownRule.PushDownContext, LogicalNode>
    implements RewriteRule {
  /** Class Logger */
  private final Log LOG = LogFactory.getLog(ProjectionPushDownRule.class);
  private static final String name = "ProjectionPushDown";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized) || !plan.getRootBlock().hasTableExpression()) {
      LOG.info("This query skips the logical optimization step.");
      return false;
    }

    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();

    LogicalPlan.QueryBlock topmostBlock;

    // skip a non-table-expression block.
    if (plan.getRootBlock().getRootType() == NodeType.INSERT) {
      topmostBlock = plan.getChildBlocks(rootBlock).iterator().next();
    } else {
      topmostBlock = rootBlock;
    }

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    PushDownContext context = new PushDownContext(topmostBlock);
    context.plan = plan;

    if (topmostBlock.getProjection() != null && topmostBlock.getProjection().isAllProjected()) {
      context.targetListManager = new TargetListManager(plan, topmostBlock.getProjectionNode().getTargets());
    } else {
      context.targetListManager= new TargetListManager(plan, topmostBlock.getName());
    }

    visitChild(context, plan, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class PushDownContext {
    LogicalPlan.QueryBlock queryBlock;
    LogicalPlan plan;
    TargetListManager targetListManager;
    Set<Column> upperRequired;

    public PushDownContext(LogicalPlan.QueryBlock block) {
      this.queryBlock = block;
    }

    public PushDownContext(ProjectionPushDownRule.PushDownContext context) {
      this.plan = context.plan;
      this.queryBlock = context.queryBlock;
      this.targetListManager = context.targetListManager;
      this.upperRequired = context.upperRequired;
    }

    public PushDownContext(ProjectionPushDownRule.PushDownContext context, LogicalPlan.QueryBlock queryBlock) {
      this(context);
      this.queryBlock = queryBlock;
    }
  }

  @Override
  public LogicalNode visitRoot(PushDownContext context, LogicalPlan plan, LogicalRootNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    return pushDownCommonPost(context, node, stack);
  }

  @Override
  public LogicalNode visitProjection(PushDownContext context, LogicalPlan plan, ProjectionNode node,
                                     Stack<LogicalNode> stack) throws PlanningException {
    if (context.upperRequired == null) { // all projected
      context.upperRequired = new HashSet<Column>();
      for (Target target : node.getTargets()) {
        context.upperRequired.add(target.getColumnSchema());
      }
    } else {
      List<Target> projectedTarget = new ArrayList<Target>();
      for (Target target : node.getTargets()) {
        if (context.upperRequired.contains(target.getColumnSchema())) {
          projectedTarget.add(target);
        }
      }
      node.setTargets(projectedTarget.toArray(new Target[projectedTarget.size()]));

      context.upperRequired = new HashSet<Column>();
      for (Target target : node.getTargets()) {
        context.upperRequired.add(target.getColumnSchema());
      }
    }

    stack.push(node);
    LogicalNode child = visitChild(context, plan, node.getChild(), stack);
    stack.pop();

    LogicalNode childNode = node.getChild();

    // If all expressions are evaluated in the child operators and the last operator is projectable,
    // ProjectionNode will not be necessary. It eliminates ProjectionNode.
    if (context.targetListManager.isAllResolved() && (childNode instanceof Projectable)) {
      child.setOutSchema(context.targetListManager.getUpdatedSchema());
      if (stack.isEmpty()) {
        // update the child node's output schemas
        context.queryBlock.setRoot(child);
      } else if (stack.peek().getType() == NodeType.TABLE_SUBQUERY) {
        ((TableSubQueryNode)stack.peek()).setSubQuery(childNode);
      } else {
        LogicalNode parent = stack.peek();
        PlannerUtil.deleteNode(parent, node);
      }
      return child;
    } else {
      node.setInSchema(child.getOutSchema());
      node.setTargets(context.targetListManager.getUpdatedTarget());
      return node;
    }
  }

  @Override
  public LogicalNode visitLimit(PushDownContext context, LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return pushDownCommonPost(context, node, stack);
  }

  @Override
  public LogicalNode visitSort(PushDownContext context, LogicalPlan plan, SortNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    for (SortSpec spec : node.getSortKeys()) {
      context.upperRequired.add(spec.getSortKey());
    }

    return pushDownCommonPost(context, node, stack);
  }

  @Override
  public LogicalNode visitGroupBy(PushDownContext context, LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    Set<Column> currentRequired = new HashSet<Column>(context.upperRequired);

    for (Target target : node.getTargets()) {
      currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(target.getEvalTree()));
    }

    for (Column column : node.getGroupingColumns()) {
      currentRequired.add(column);
    }

    PushDownContext groupByContext = new PushDownContext(context);
    groupByContext.upperRequired = currentRequired;
    return pushDownCommonPost(groupByContext, node, stack);
  }

  @Override
  public LogicalNode visitFilter(PushDownContext context, LogicalPlan plan, SelectionNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    if (node.getQual() != null) {
      context.upperRequired.addAll(EvalTreeUtil.findDistinctRefColumns(node.getQual()));
    }

    return pushDownCommonPost(context, node, stack);
  }

  @Override
  public LogicalNode visitJoin(PushDownContext context, LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    Set<Column> currentRequired = Sets.newHashSet(context.upperRequired);

    if (node.hasTargets()) {
      EvalNode expr;
      for (Target target : node.getTargets()) {
        expr = target.getEvalTree();
        if (expr.getType() != EvalType.FIELD) {
          currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(target.getEvalTree()));
        }
      }
    }

    if (node.hasJoinQual()) {
      currentRequired.addAll(EvalTreeUtil.findDistinctRefColumns(node.getJoinQual()));
    }

    PushDownContext leftContext = new PushDownContext(context);
    PushDownContext rightContext = new PushDownContext(context);
    leftContext.upperRequired = currentRequired;
    rightContext.upperRequired = currentRequired;

    stack.push(node);
    LogicalNode outer = visitChild(leftContext, plan, node.getLeftChild(), stack);
    LogicalNode inner = visitChild(rightContext, plan, node.getRightChild(), stack);
    stack.pop();

    Schema merged = SchemaUtil.merge(outer.getOutSchema(), inner.getOutSchema());
    node.setInSchema(merged);
    pushDownProjectablePost(context, node, isTopmostProjectable(stack));

    return node;
  }

  @Override
  public LogicalNode visitUnion(PushDownContext context, LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return pushDownSetNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitExcept(PushDownContext context, LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return pushDownSetNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitIntersect(PushDownContext context, LogicalPlan plan, IntersectNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
    return pushDownSetNode(plan, node, stack, context);
  }

  @Override
  public LogicalNode visitTableSubQuery(PushDownContext context, LogicalPlan plan, TableSubQueryNode node,
                                        Stack<LogicalNode> stack) throws PlanningException {
    LogicalPlan.QueryBlock subBlock = plan.getBlock(node.getSubQuery());
    LogicalNode subRoot = subBlock.getRoot();

    Stack<LogicalNode> newStack = new Stack<LogicalNode>();
    newStack.push(node);
    PushDownContext newContext = new PushDownContext(subBlock);

    newContext.upperRequired = new HashSet<Column>();

    if (subBlock.hasProjection() && subBlock.getProjection().isAllProjected()
        && context.upperRequired.size() == 0) {
      newContext.targetListManager = new TargetListManager(plan, subBlock.getProjectionNode().getTargets());
    } else {
      if (!subBlock.hasGrouping()) {
        List<Target> projectedTarget = new ArrayList<Target>();
        for (Target target : subBlock.getTargetListManager().getUnresolvedTargets()) {
          for (Column column : context.upperRequired) {
            if (column.hasQualifier() && !node.getTableName().equals(column.getQualifier())) {
              continue;
            }
            if (target.getColumnSchema().getColumnName().equalsIgnoreCase(column.getColumnName())) {
              projectedTarget.add(target);
            }
          }
        }
        newContext.targetListManager = new TargetListManager(plan,
            projectedTarget.toArray(new Target[projectedTarget.size()]));

      } else {
        newContext.targetListManager = new TargetListManager(plan,
            subBlock.getTargetListManager().getUnresolvedTargets());
      }
    }

    newContext.upperRequired.addAll(PlannerUtil.targetToSchema(newContext.targetListManager.getTargets()).getColumns());

    LogicalNode child = visitChild(newContext, plan, subRoot, newStack);
    newStack.pop();
    Schema inSchema = (Schema) child.getOutSchema().clone();
    inSchema.setQualifier(node.getCanonicalName(), true);
    node.setInSchema(inSchema);
    return pushDownProjectablePost(context, node, isTopmostProjectable(stack));
  }

  @Override
  public LogicalNode visitScan(PushDownContext context, LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return pushDownProjectablePost(context, node, isTopmostProjectable(stack));
  }

  @Override
  public LogicalNode visitStoreTable(PushDownContext context, LogicalPlan plan, StoreTableNode node,
                                     Stack<LogicalNode> stack) throws PlanningException {
    return pushDownCommonPost(context, node, stack);
  }

  private LogicalNode pushDownCommonPost(PushDownContext context, UnaryNode node, Stack<LogicalNode> stack)
      throws PlanningException {

    stack.push(node);
    LogicalNode child = visitChild(context, context.plan, node.getChild(), stack);
    stack.pop();
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    if (node instanceof Projectable) {
      pushDownProjectablePost(context, node, isTopmostProjectable(stack));
    }
    return node;
  }

  private static LogicalNode pushDownProjectablePost(PushDownContext context, LogicalNode node, boolean last)
      throws PlanningException {
    TargetListManager targetListManager = context.targetListManager;
    EvalNode expr;

    List<Integer> newEvaluatedTargetIds = new ArrayList<Integer>();

    for (int i = 0; i < targetListManager.size(); i++) {
      expr = targetListManager.getTarget(i).getEvalTree();

      if (!targetListManager.isResolved(i) && PlannerUtil.canBeEvaluated(expr, node)) {

        if (node instanceof RelationNode) { // For ScanNode

          if (expr.getType() == EvalType.FIELD && !targetListManager.getTarget(i).hasAlias()) {
            targetListManager.resolve(i);
          } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
            targetListManager.resolve(i);
            newEvaluatedTargetIds.add(i);
          }

        } else if (node instanceof GroupbyNode) { // For GroupBy
          if (EvalTreeUtil.findDistinctAggFunction(expr).size() > 0 && expr.getType() != EvalType.FIELD) {
            targetListManager.resolve(i);
            newEvaluatedTargetIds.add(i);
          }

        } else if (node instanceof JoinNode) {
          if (expr.getType() != EvalType.FIELD && EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
            targetListManager.resolve(i);
            newEvaluatedTargetIds.add(i);
          }
        }
      }
    }

    Projectable projectable = (Projectable) node;
    if (last) {
      Preconditions.checkState(targetListManager.isAllResolved(), "Not all targets are evaluated");
      if (node.getType() != NodeType.GROUP_BY) {
        projectable.setTargets(targetListManager.getTargets());
      }
      node.setOutSchema(targetListManager.getUpdatedSchema());
    } else {
      // Preparing targets regardless of that the node has targets or not.
      // This part is required because some node does not have any targets,
      // if the node has the same input and output schemas.

      Target[] checkingTargets;
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
      for (Target target : checkingTargets) {
        for (Column column : context.upperRequired) {
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

  private static boolean isTopmostProjectable(Stack<LogicalNode> stack) {
    for (LogicalNode node : stack) {
      if (node.getType() == NodeType.JOIN || node.getType() == NodeType.GROUP_BY) {
        return false;
      }
    }

    return true;
  }

  private TargetListManager buildSubBlockTargetList(LogicalPlan plan,
      LogicalPlan.QueryBlock subQueryBlock, TableSubQueryNode subQueryNode, Set<Column> upperRequired) {
    TargetListManager subBlockTargetList;
    List<Target> projectedTarget = new ArrayList<Target>();
    for (Target target : subQueryBlock.getTargetListManager().getUnresolvedTargets()) {
      for (Column column : upperRequired) {
        if (!subQueryNode.getTableName().equals(column.getQualifier())) {
          continue;
        }
        if (target.getColumnSchema().getColumnName().equalsIgnoreCase(column.getColumnName())) {
          projectedTarget.add(target);
        }
      }
    }
    subBlockTargetList = new TargetListManager(plan, projectedTarget.toArray(new Target[projectedTarget.size()]));
    return subBlockTargetList;
  }

  private BinaryNode pushDownSetNode(LogicalPlan plan, BinaryNode setNode, Stack<LogicalNode> stack,
                                            PushDownContext context) throws PlanningException {

    LogicalPlan.QueryBlock currentBlock = plan.getBlock(setNode);
    LogicalPlan.QueryBlock leftBlock = plan.getChildBlocks(currentBlock).get(0);
    LogicalPlan.QueryBlock rightBlock = plan.getChildBlocks(currentBlock).get(1);

    PushDownContext leftContext = new PushDownContext(context, leftBlock);
    leftContext.targetListManager = buildSubBlockTargetList(plan, leftBlock,
        (TableSubQueryNode) setNode.getLeftChild(), context.upperRequired);

    PushDownContext rightContext = new PushDownContext(context, rightBlock);
    rightContext.targetListManager = buildSubBlockTargetList(plan, rightBlock,
        (TableSubQueryNode) setNode.getRightChild(), context.upperRequired);

    stack.push(setNode);
    visitChild(leftContext, plan, setNode.getLeftChild(), stack);
    visitChild(rightContext, plan, setNode.getRightChild(), stack);
    stack.pop();

    // if this is the final union, we assume that all targets are evalauted.
    // TODO - is it always correct?
    if (stack.peek().getType() != NodeType.UNION) {
      context.targetListManager.resolveAll();
    }

    return setNode;
  }
}
