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
import com.google.common.collect.Lists;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.exception.InvalidQueryException;

import java.util.List;
import java.util.Stack;

public class FilterPushDownRule extends BasicLogicalPlanVisitor<List<EvalNode>> implements RewriteRule {
  private static final String NAME = "FilterPushDown";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    return PlannerUtil.findTopNode(toBeOptimized, NodeType.SELECTION) != null;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalNode root = plan.getRootBlock().getRoot();
    SelectionNode selNode = (SelectionNode) PlannerUtil.findTopNode(root, NodeType.SELECTION);
    Preconditions.checkNotNull(selNode);

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(selNode.getQual());

    visitChild(plan, root, stack, Lists.newArrayList(cnf));
    return plan;
  }

  public LogicalNode visitFilter(LogicalPlan plan, SelectionNode selNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {
    stack.push(selNode);
    visitChild(plan, selNode.getChild(), stack, cnf);
    stack.pop();

    // remove the selection operator if there is no search condition
    // after selection push.
    if(cnf.size() == 0) {
      LogicalNode node = stack.peek();
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        unary.setChild(selNode.getChild());
      } else {
        throw new InvalidQueryException("Unexpected Logical Query Plan");
      }
    }

    return selNode;
  }

  public LogicalNode visitJoin(LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    visitChild(plan, left, stack, cnf);
    visitChild(plan, right, stack, cnf);

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, joinNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = EvalTreeUtil.transformCNF2Singleton(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) {
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

    return joinNode;
  }

  public LogicalNode visitScan(LogicalPlan plan, ScanNode scanNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, scanNode)) {
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

    if (qual != null) { // if a matched qual exists
      scanNode.setQual(qual);
    }

    cnf.removeAll(matched);

    return scanNode;
  }
}
