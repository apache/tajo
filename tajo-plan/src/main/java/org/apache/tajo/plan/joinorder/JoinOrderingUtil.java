/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class JoinOrderingUtil {

  public static Set<EvalNode> findJoinConditionForJoinVertex(Set<EvalNode> candidates, JoinEdge edge) {
    Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
    for (EvalNode predicate : candidates) {
      if (EvalTreeUtil.isJoinQual(predicate, false)
          && checkIfEvaluatedAtVertex(predicate, edge)) {
        conditionsForThisJoin.add(predicate);
      }
    }
    return conditionsForThisJoin;
  }

  public static boolean checkIfEvaluatedAtVertex(EvalNode evalNode, JoinEdge edge) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);
    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }
    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }
    if (columnRefs.size() > 0 && !edge.getSchema().containsAll(columnRefs)) {
      return false;
    }
    return true;
  }

  /**
   * Associativity rules
   *
   * ==============================================================
   * Left-Hand Bracketed  | Right-Hand Bracketed  | Equivalence
   * ==============================================================
   * (A inner B) inner C  | A inner (B inner C)   | Equivalent
   * (A left B) inner C   | A left (B inner C)    | Not equivalent
   * (A right B) inner C  | A right (B inner C)   | Equivalent
   * (A full B) inner C   | A full (B inner C)    | Not equivalent
   * (A inner B) left C   | A inner (B left C)    | Equivalent
   * (A left B) left C    | A left (B left C)     | Equivalent
   * (A right B) left C   | A right (B left C)    | Equivalent
   * (A full B) left C    | A full (B left C)     | Equivalent
   * (A inner B) right C  | A inner (B right C)   | Not equivalent
   * (A left B) right C   | A left (B right C)    | Not equivalent
   * (A right B) right C  | A right (B right C)   | Equivalent
   * (A full B) right C   | A full (B right C)    | Not equivalent
   * (A inner B) full C   | A inner (B full C)    | Not equivalent
   * (A left B) full C    | A left (B full C)     | Not equivalent
   * (A right B) full C   | A right (B full C)    | Equivalent
   * (A full B) full C    | A full (B full C)     | Equivalent
   * ==============================================================
   */
  public static boolean isAssociativeJoin(JoinType leftType, JoinType rightType) {
    if (leftType == rightType) {
      return true;
    }

    if (leftType == JoinType.RIGHT_OUTER) {
      return true;
    }

    if (leftType == JoinType.LEFT_OUTER) {
      // When the left type is the left outer join, input join types are associative
      // if the right type is also the left outer join.
      // This case is already checked above.
      return false;
    }

    if (leftType == JoinType.INNER) {
      if (rightType == JoinType.LEFT_OUTER) {
        return true;
      } else {
        return false;
      }
    }

    if (leftType == JoinType.FULL_OUTER) {
      if (rightType == JoinType.LEFT_OUTER) {
        return true;
      } else {
        return false;
      }
    }

    // TODO: consider when a join qual involves columns from two or more tables
    return false;
  }


  public static Set<RelationNode> findRelationVertexes(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                       LogicalNode from) throws PlanningException {
    RelationNodeFinderContext context = new RelationNodeFinderContext();
    context.findMostLeft = context.findMostRight = true;
    RelationNodeFinder finder = new RelationNodeFinder();
    finder.visit(context, plan, block, from, new Stack<LogicalNode>());
    return context.founds;
  }

  public static RelationNode findMostLeftRelation(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                  LogicalNode from) throws PlanningException {
    RelationNodeFinderContext context = new RelationNodeFinderContext();
    context.findMostLeft = true;
    RelationNodeFinder finder = new RelationNodeFinder();
    finder.visit(context, plan, block, from, new Stack<LogicalNode>());
    return context.founds.isEmpty() ? null : context.founds.iterator().next();
  }

  public static RelationNode findMostRightRelation(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                   LogicalNode from) throws PlanningException {
    RelationNodeFinderContext context = new RelationNodeFinderContext();
    context.findMostRight = true;
    RelationNodeFinder finder = new RelationNodeFinder();
    finder.visit(context, plan, block, from, new Stack<LogicalNode>());
    return context.founds.isEmpty() ? null : context.founds.iterator().next();
  }

  private static class RelationNodeFinderContext {
    private Set<RelationNode> founds = TUtil.newHashSet();
    private boolean findMostLeft;
    private boolean findMostRight;
  }

  private static class RelationNodeFinder extends BasicLogicalPlanVisitor<RelationNodeFinderContext,LogicalNode> {

    @Override
    public void postHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, RelationNodeFinderContext context)
        throws PlanningException {
      if (node instanceof RelationNode) {
        context.founds.add((RelationNode) node);
      }
    }

    @Override
    public LogicalNode visitJoin(RelationNodeFinderContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
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

  public static JoinNode createJoinNodeFromEdge(LogicalPlan plan, JoinEdge edge) {
    JoinNode node = plan.createNode(JoinNode.class);

    node.init(edge.getJoinType(),
        edge.getLeftVertex().getCorrespondingNode(),
        edge.getRightVertex().getCorrespondingNode());
    if (edge.hasJoinQual()) {
      node.setJoinQual(edge.getSingletonJoinQual());
    }
    return node;
  }
}
