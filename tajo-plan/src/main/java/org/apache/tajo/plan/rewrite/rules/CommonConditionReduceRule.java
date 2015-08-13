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

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.SelectionNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

/**
 * Condition reduce rule reduces the query predicate based on distributivity.
 * For example, the query
 *
 * SELECT *
 * FROM t
 * WHERE (t.a = 1 OR t.b = 10) AND (t.a = 1 OR t.c = 100)
 *
 * is converted into
 *
 * SELECT *
 * FROM t
 * WHERE t.a = 1 OR (t.b = 10 AND t.c = 100).
 *
 */
public class CommonConditionReduceRule implements LogicalPlanRewriteRule {
  private Rewriter rewriter;

  @Override
  public String getName() {
    return "CommonConditionReduceRule";
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    for (LogicalPlan.QueryBlock block : context.getPlan().getQueryBlocks()) {
      if (block.hasNode(NodeType.SELECTION) || block.hasNode(NodeType.JOIN)) {
        rewriter = new Rewriter(context.getPlan());
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    rewriter.visit(null, context.getPlan(), context.getPlan().getRootBlock());
    return context.getPlan();
  }

  /**
   * Rewriter simply triggers rewriting evals while visiting logical nodes.
   */
  private final static class Rewriter extends BasicLogicalPlanVisitor<Object, LogicalNode> {
    EvalRewriter evalRewriter;

    public Rewriter(LogicalPlan plan) {
      evalRewriter = new EvalRewriter(plan);
    }

    @Override
    public LogicalNode visitFilter(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode selNode, Stack<LogicalNode> stack) throws TajoException {
      selNode.setQual(evalRewriter.visit(null, selNode.getQual(), new Stack<EvalNode>()));
      return null;
    }

    @Override
    public LogicalNode visitJoin(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack) throws TajoException {
      if (joinNode.hasJoinQual()) {
        joinNode.setJoinQual(evalRewriter.visit(null, joinNode.getJoinQual(), new Stack<EvalNode>()));
      }
      return null;
    }
  }

  /**
   * EvalRewriter is responsible for rewriting evals based on distributivity.
   */
  private static class EvalRewriter extends SimpleEvalNodeVisitor<Object> {
    final LogicalPlan plan;

    public EvalRewriter(LogicalPlan plan) {
      this.plan = plan;
    }

    @Override
    protected EvalNode visitUnaryEval(Object context, Stack<EvalNode> stack, UnaryEval unaryEval) {
      stack.push(unaryEval);
      EvalNode child = unaryEval.getChild();
      visit(context, child, stack);
      if (child.getType() == EvalType.AND || child.getType() == EvalType.OR) {
        unaryEval.setChild(rewrite((BinaryEval) child));
      }
      stack.pop();
      return unaryEval;
    }

    @Override
    protected EvalNode visitBinaryEval(Object context, Stack<EvalNode> stack, BinaryEval binaryEval) {
      stack.push(binaryEval);
      EvalNode child = binaryEval.getLeftExpr();
      visit(context, child, stack);
      if (child.getType() == EvalType.AND || child.getType() == EvalType.OR) {
        binaryEval.setLeftExpr(rewrite((BinaryEval) child));
      }

      child = binaryEval.getRightExpr();
      visit(context, child, stack);
      if (child.getType() == EvalType.AND || child.getType() == EvalType.OR) {
        binaryEval.setRightExpr(rewrite((BinaryEval) child));
      }

      EvalNode result = rewrite(binaryEval);

      stack.pop();
      return result;
    }

    @Override
    protected EvalNode visitDefaultFunctionEval(Object context, Stack<EvalNode> stack, FunctionEval functionEval) {
      stack.push(functionEval);
      if (functionEval.getArgs() != null) {
        EvalNode [] args = functionEval.getArgs();
        for (int i = 0; i < args.length; i++) {
          visit(context, args[i], stack);
          if (args[i].getType() == EvalType.AND || args[i].getType() == EvalType.OR) {
            functionEval.setArg(i, rewrite((BinaryEval) args[i]));
          }
        }
      }
      stack.pop();
      return functionEval;
    }

    private EvalNode rewrite(BinaryEval evalNode) {
      // Example qual: ( a | b ) ^ ( a | c )
      EvalType outerType = evalNode.getType(); // type of the outer operation. ex) ^
      EvalType innerType = outerType == EvalType.AND ? EvalType.OR : EvalType.AND; // type of the inner operation. ex) |

      EvalNode finalQual;

      if (evalNode.getLeftExpr().getType() == innerType &&
          evalNode.getRightExpr().getType() == innerType) {
        BinaryEval leftChild = evalNode.getLeftExpr();
        BinaryEval rightChild = evalNode.getRightExpr();

        // Find common quals from the left and right children.
        Set<EvalNode> commonQuals = TUtil.newHashSet();
        Set<EvalNode> childrenOfLeft = TUtil.newHashSet(leftChild.getLeftExpr(), leftChild.getRightExpr());
        for (int i = 0; i < 2; i++) {
          if (childrenOfLeft.contains(rightChild.getChild(i))) {
            commonQuals.add(rightChild.getChild(i));
          }
        }

        if (commonQuals.size() == 2) {
          // Ex) ( a | b ) ^ ( a | b )
          // Current binary eval has the same left and right children, so it is useless.
          // Connect the parent of the current eval and one of the children directly.
          finalQual = leftChild;
          plan.addHistory("Common condition is reduced.");

        } else if (commonQuals.size() == 1) {
          // A single common qual is found.
          // ( a | b ) ^ ( a | c ) -> a | (b ^ c)
          EvalNode commonQual = commonQuals.iterator().next();
          EvalNode nonCommonQual;

          if (leftChild.getLeftExpr().equals(commonQual)) {
            nonCommonQual = leftChild.getRightExpr();
          } else {
            nonCommonQual = leftChild.getLeftExpr();
          }

          if (rightChild.getLeftExpr().equals(commonQual)) {
            nonCommonQual = new BinaryEval(outerType, nonCommonQual, rightChild.getRightExpr());
          } else {
            nonCommonQual = new BinaryEval(outerType, nonCommonQual, rightChild.getLeftExpr());
          }

          finalQual = new BinaryEval(innerType, commonQual, nonCommonQual);
          plan.addHistory("Common condition is reduced.");
        } else {
          finalQual = evalNode;
        }
      } else {
        finalQual = evalNode;
      }

      return finalQual;
    }

  }
}
