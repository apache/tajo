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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Set;
import java.util.Stack;

public class InSubqueryRewriteRule implements LogicalPlanRewriteRule {

  private static final Log LOG = LogFactory.getLog(InSubqueryRewriteRule.class);
  private static final String NAME = "InSubqueryRewrite";
  private final Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    for (LogicalNode eachNode : PlannerUtil.findAllNodes(plan.getRootNode(), NodeType.SELECTION)) {
      SelectionNode selectionNode = (SelectionNode) eachNode;
      if (!extractInSubquery(selectionNode.getQual()).isEmpty()) {
        return true;
      }
    }

    return false;
  }

  static List<InEval> extractInSubquery(EvalNode qual) {
    List<InEval> inSubqueries = TUtil.newList();
    for (EvalNode eachQual : EvalTreeUtil.findEvalsByType(qual, EvalType.IN)) {
      InEval inEval = (InEval) eachQual;
      if (inEval.getRightExpr().getType() == EvalType.SUBQUERY) {
        inSubqueries.add(inEval);
      }
    }
    return inSubqueries;
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    rewriter.visit(queryContext, plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private static final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {
    @Override
    public Object visitFilter(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                              Stack<LogicalNode> stack) throws PlanningException {
      List<InEval> inSubqueries = extractInSubquery(node.getQual());
      stack.push(node);
      for (InEval eachIn : inSubqueries) {
        SubqueryEval subqueryEval = eachIn.getRightExpr();
        QueryBlock childBlock = plan.getBlock(subqueryEval.getSubQueryNode());
        visit(context, plan, childBlock, childBlock.getRoot(), stack);
      }
      visit(context, plan, block, node.getChild(), stack);
      stack.pop();

      LogicalNode baseRelation = null;
      for (InEval eachIn : inSubqueries) {
        // find the base relation for the column of the outer query
        Preconditions.checkArgument(eachIn.getLeftExpr().getType() == EvalType.FIELD);
        FieldEval fieldEval = eachIn.getLeftExpr();
        SubqueryEval subqueryEval = eachIn.getRightExpr();
        baseRelation = baseRelation == null ? node.getChild() : baseRelation;

        // create join
        JoinNode joinNode = new JoinNode(plan.newPID());
        joinNode.init(eachIn.isNot() ? JoinType.LEFT_ANTI : JoinType.LEFT_SEMI,
            baseRelation, subqueryEval.getSubQueryNode());
        joinNode.setJoinQual(buildJoinCondition(fieldEval, subqueryEval.getSubQueryNode()));

        // set the created join as the base relation
        baseRelation = joinNode;
      }
      
      // remove in quals
      EvalNode[] originDnfs = AlgebraicUtil.toDisjunctiveNormalFormArray(node.getQual());
      List<EvalNode> rewrittenDnfs = TUtil.newList();
      for (EvalNode eachDnf : originDnfs) {
        Set<EvalNode> cnfs = TUtil.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(eachDnf));
        cnfs.removeAll(inSubqueries);
        if (!cnfs.isEmpty()) {
          rewrittenDnfs.add(AlgebraicUtil.createSingletonExprFromCNF(cnfs));
        }
      }
      node.setQual(AlgebraicUtil.createSingletonExprFromDNF(rewrittenDnfs.toArray(new EvalNode[rewrittenDnfs.size()])));

      // Selection node is expected to be removed at the projection push down phase.
      node.setChild(baseRelation);

      return null;
    }

    private EvalNode buildJoinCondition(FieldEval leftField, TableSubQueryNode subQueryNode) {
      FieldEval rightField = new FieldEval(subQueryNode.getOutSchema().getColumn(0));
      return new BinaryEval(EvalType.EQUAL, leftField, rightField);
    }

  }
}
