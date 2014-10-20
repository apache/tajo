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

package org.apache.tajo.engine.optimizer.eval.rules;

import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.optimizer.eval.EvalTreeOptimizationRule;
import org.apache.tajo.engine.optimizer.eval.Prioritized;
import org.apache.tajo.engine.planner.LogicalPlanner;

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
      return new ConstEval(binaryEval.eval(null, null));
    }

    return binaryEval;
  }

  @Override
  public EvalNode visitUnaryEval(LogicalPlanner.PlanContext context, Stack<EvalNode> stack, UnaryEval unaryEval) {
    stack.push(unaryEval);
    EvalNode child = visit(context, unaryEval.getChild(), stack);
    stack.pop();

    if (child.getType() == EvalType.CONST) {
      return new ConstEval(unaryEval.eval(null, null));
    }

    return unaryEval;
  }

  @Override
  public EvalNode visitFuncCall(LogicalPlanner.PlanContext context, FunctionEval evalNode, Stack<EvalNode> stack) {
    boolean constantOfAllDescendents = true;

    if ("sleep".equals(evalNode.getFuncDesc().getFunctionName())) {
      constantOfAllDescendents = false;
    } else {
      if (evalNode.getArgs() != null) {
        for (EvalNode arg : evalNode.getArgs()) {
          arg = visit(context, arg, stack);
          constantOfAllDescendents &= (arg.getType() == EvalType.CONST);
        }
      }
    }

    if (constantOfAllDescendents && evalNode.getType() == EvalType.FUNCTION) {
      return new ConstEval(evalNode.eval(null, null));
    } else {
      return evalNode;
    }
  }
}
