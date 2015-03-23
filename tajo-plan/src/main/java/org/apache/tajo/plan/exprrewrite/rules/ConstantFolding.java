/*
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

package org.apache.tajo.plan.exprrewrite.rules;

import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.exprrewrite.EvalTreeOptimizationRule;
import org.apache.tajo.plan.annotator.Prioritized;
import org.apache.tajo.plan.LogicalPlanner;

import java.util.Stack;

@Prioritized(priority = 10)
public class ConstantFolding extends SimpleEvalNodeVisitor<LogicalPlanner.PlanContext>
    implements EvalTreeOptimizationRule {

  @Override
  public EvalNode optimize(LogicalPlanner.PlanContext context, EvalNode evalNode) {
    return visit(context, evalNode, new Stack<EvalNode>());
  }

  @Override
  public EvalNode visitBinaryEval(LogicalPlanner.PlanContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    EvalNode lhs = visit(context, binaryEval.getLeftExpr(), stack);
    EvalNode rhs = visit(context, binaryEval.getRightExpr(), stack);
    stack.pop();

    if (!binaryEval.getLeftExpr().equals(lhs)) {
      binaryEval.setLeftExpr(lhs);
    }
    if (!binaryEval.getRightExpr().equals(rhs)) {
      binaryEval.setRightExpr(rhs);
    }

    if (lhs.getType() == EvalType.CONST && rhs.getType() == EvalType.CONST) {
      binaryEval.bind(null);
      return new ConstEval(binaryEval.eval(null));
    }

    return binaryEval;
  }

  @Override
  public EvalNode visitUnaryEval(LogicalPlanner.PlanContext context, Stack<EvalNode> stack, UnaryEval unaryEval) {
    stack.push(unaryEval);
    EvalNode child = visit(context, unaryEval.getChild(), stack);
    stack.pop();

    if (child.getType() == EvalType.CONST) {
      unaryEval.bind(null);
      return new ConstEval(unaryEval.eval(null));
    }

    return unaryEval;
  }

  @Override
  public EvalNode visitFuncCall(LogicalPlanner.PlanContext context, FunctionEval evalNode, Stack<EvalNode> stack) {
    boolean constantOfAllDescendents = true;

    if ("sleep".equals(evalNode.getFuncDesc().getFunctionName())) {
      constantOfAllDescendents = false;
    } else {
      for (EvalNode arg : evalNode.getArgs()) {
        arg = visit(context, arg, stack);
        constantOfAllDescendents &= (arg.getType() == EvalType.CONST);
      }
    }

    if (constantOfAllDescendents && evalNode.getType() == EvalType.FUNCTION) {
      evalNode.bind(null);
      return new ConstEval(evalNode.eval(null));
    } else {
      return evalNode;
    }
  }
}
