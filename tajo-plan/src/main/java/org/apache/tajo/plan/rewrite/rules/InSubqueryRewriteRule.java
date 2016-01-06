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

package org.apache.tajo.plan.rewrite.rules;

import com.google.common.base.Preconditions;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.*;

/**
 * InSubqueryRewriteRule finds all subqueries occurring in the where clause with "IN" keywords,
 * and replaces them with appropriate join plans.
 * This rule must be executed before {@link FilterPushDownRule}.
 *
 */
public class InSubqueryRewriteRule implements LogicalPlanRewriteRule {

  private static final String NAME = "InSubqueryRewrite";
  private final Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    for (LogicalNode eachNode : PlannerUtil.findAllNodes(context.getPlan().getRootNode(), NodeType.SELECTION)) {
      SelectionNode selectionNode = (SelectionNode) eachNode;
      if (!extractInSubquery(selectionNode.getQual()).isEmpty()) {
        return true;
      }
    }

    return false;
  }

  static List<InEval> extractInSubquery(EvalNode qual) {
    List<InEval> inSubqueries = new ArrayList<>();
    for (EvalNode eachQual : EvalTreeUtil.findEvalsByType(qual, EvalType.IN)) {
      InEval inEval = (InEval) eachQual;
      if (inEval.getRightExpr().getType() == EvalType.SUBQUERY) {
        inSubqueries.add(inEval);
      }
    }
    return inSubqueries;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlan.QueryBlock rootBlock = context.getPlan().getRootBlock();
    LogicalPlan plan = context.getPlan();
    rewriter.visit(context.getQueryContext(), plan, rootBlock, rootBlock.getRoot(), new Stack<>());
    return plan;
  }

  private static final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {
    @Override
    public Object visitFilter(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                              Stack<LogicalNode> stack) throws TajoException {
      // Since InSubqueryRewriteRule is executed before FilterPushDownRule,
      // we can expect that in-subqueries are found at only SelectionNode.

      // Visit every child first.
      List<InEval> inSubqueries = extractInSubquery(node.getQual());
      stack.push(node);
      for (InEval eachIn : inSubqueries) {
        SubqueryEval subqueryEval = eachIn.getRightExpr();
        QueryBlock childBlock = plan.getBlock(subqueryEval.getSubQueryNode().getSubQuery());
        visit(context, plan, childBlock, childBlock.getRoot(), stack);
      }
      visit(context, plan, block, node.getChild(), stack);
      stack.pop();

      LogicalNode baseRelation = node.getChild();
      for (InEval eachIn : inSubqueries) {
        // 1. find the base relation for the column of the outer query

        // We assume that the left child of an in-subquery is either a FieldEval or a CastEval.
        Preconditions.checkArgument(eachIn.getLeftExpr().getType() == EvalType.FIELD ||
            eachIn.getLeftExpr().getType() == EvalType.CAST);
        EvalNode leftEval = eachIn.getLeftExpr();
        SubqueryEval subqueryEval = eachIn.getRightExpr();
        QueryBlock childBlock = plan.getBlock(subqueryEval.getSubQueryNode().getSubQuery());

        // 2. create join
        JoinType joinType = eachIn.isNot() ? JoinType.LEFT_ANTI : JoinType.LEFT_SEMI;
        JoinNode joinNode = new JoinNode(plan.newPID());
        joinNode.init(joinType, baseRelation, subqueryEval.getSubQueryNode());
        joinNode.setJoinQual(buildJoinCondition(leftEval, subqueryEval.getSubQueryNode()));
        ProjectionNode projectionNode = PlannerUtil.findTopNode(subqueryEval.getSubQueryNode(), NodeType.PROJECTION);
        // Insert an aggregation operator rather than just setting the distinct flag of the ProjectionNode
        // because the performance of distinct aggregation is poor.
        insertDistinctOperator(plan, childBlock, projectionNode, projectionNode.getChild());

        Schema inSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
            joinNode.getRightChild().getOutSchema());
        joinNode.setInSchema(inSchema);
        joinNode.setOutSchema(node.getOutSchema());

        List<Target> targets = new ArrayList<>();
        targets.addAll(PlannerUtil.schemaToTargets(inSchema));
        joinNode.setTargets(targets);

        block.addJoinType(joinType);
        block.registerNode(joinNode);
        plan.addHistory("IN subquery is rewritten into " + (eachIn.isNot() ? "anti" : "semi") + " join.");

        // 3. set the created join as the base relation
        baseRelation = joinNode;
      }
      
      // 4. remove in quals
      EvalNode[] originDnfs = AlgebraicUtil.toDisjunctiveNormalFormArray(node.getQual());
      List<EvalNode> rewrittenDnfs = new ArrayList<>();
      for (EvalNode eachDnf : originDnfs) {
        Set<EvalNode> cnfs = new HashSet<>(Arrays.asList(AlgebraicUtil.toConjunctiveNormalFormArray(eachDnf)));
        cnfs.removeAll(inSubqueries);
        if (!cnfs.isEmpty()) {
          rewrittenDnfs.add(AlgebraicUtil.createSingletonExprFromCNF(cnfs));
        }
      }
      if (rewrittenDnfs.size() > 0) {
        node.setQual(AlgebraicUtil.createSingletonExprFromDNF(rewrittenDnfs.toArray(new EvalNode[rewrittenDnfs.size()])));
        // The current selection node is expected to be removed at the filter push down phase.
        node.setChild(baseRelation);
      } else {
        PlannerUtil.replaceNode(plan, block.getRoot(), node, baseRelation);
        block.unregisterNode(node);
      }

      return null;
    }

    private void insertDistinctOperator(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        ProjectionNode projectionNode, LogicalNode child) throws TajoException {
      if (projectionNode.getChild().getType() != NodeType.GROUP_BY) {
        Schema outSchema = projectionNode.getOutSchema();
        GroupbyNode dupRemoval = plan.createNode(GroupbyNode.class);
        dupRemoval.setChild(child);
        dupRemoval.setInSchema(projectionNode.getInSchema());
        dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
        dupRemoval.setGroupingColumns(outSchema.toArray());

        block.registerNode(dupRemoval);
        block.setAggregationRequire();

        projectionNode.setChild(dupRemoval);
        projectionNode.setInSchema(dupRemoval.getOutSchema());
      }
    }

    private EvalNode buildJoinCondition(EvalNode leftField, TableSubQueryNode subQueryNode) {
      FieldEval rightField = new FieldEval(subQueryNode.getOutSchema().getColumn(0));
      return new BinaryEval(EvalType.EQUAL, leftField, rightField);
    }

  }
}
