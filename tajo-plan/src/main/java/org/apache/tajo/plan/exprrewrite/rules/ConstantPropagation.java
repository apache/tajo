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

package org.apache.tajo.plan.exprrewrite.rules;

import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.exprrewrite.EvalTreeOptimizationRule;
import org.apache.tajo.plan.annotator.Prioritized;

import java.util.Stack;

/**
 * It replaces all field references which actually point to constant values by constant values.
 * In order to maximize the effectiveness of constant folding, ConstantPropagation should be performed after
 * constant folding.
 */
@Prioritized(priority = 15)
public class ConstantPropagation extends SimpleEvalNodeVisitor<LogicalPlanner.PlanContext>
    implements EvalTreeOptimizationRule {

  @Override
  public EvalNode optimize(LogicalPlanner.PlanContext context, EvalNode evalNode) {
    if (evalNode.getType() == EvalType.FIELD) {
      FieldEval fieldEval = (FieldEval) evalNode;

      // if a reference points to a const value
      if (context.getQueryBlock().isConstReference(fieldEval.getName())) {
        return context.getQueryBlock().getConstByReference(fieldEval.getName());
      } else {
        return evalNode; // otherwise, it just returns.
      }
    } else {
      return visit(context, evalNode, new Stack<EvalNode>());
    }
  }

  @Override
  public EvalNode visitBinaryEval(LogicalPlanner.PlanContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);

    for (int i = 0; i < 2; i++) {
      if (binaryEval.getChild(i).getType() == EvalType.FIELD) {
        FieldEval fieldEval = (FieldEval) binaryEval.getChild(i);
        if (context.getQueryBlock().isConstReference(fieldEval.getName())) {
          binaryEval.setChild(i, context.getQueryBlock().getConstByReference(fieldEval.getName()));
          continue;
        }
      }

      visit(context, binaryEval.getChild(i), stack);
    }

    stack.pop();

    return binaryEval;
  }

  @Override
  public EvalNode visitUnaryEval(LogicalPlanner.PlanContext context, UnaryEval unaryEval, Stack<EvalNode> stack) {
    stack.push(unaryEval);

    if (unaryEval.getChild().getType() == EvalType.FIELD) {
      FieldEval fieldEval = (FieldEval) unaryEval.getChild();
      if (context.getQueryBlock().isConstReference(fieldEval.getName())) {
        unaryEval.setChild(context.getQueryBlock().getConstByReference(fieldEval.getName()));
        stack.pop();
        return unaryEval;
      }
    }
    visit(context, unaryEval.getChild(), stack);
    stack.pop();

    return unaryEval;
  }

  @Override
  public EvalNode visitFuncCall(LogicalPlanner.PlanContext context, FunctionEval function, Stack<EvalNode> stack) {
    stack.push(function);

    for (int i = 0; i < function.getArgs().length; i++) {
      if (function.getArgs()[i].getType() == EvalType.FIELD) {
        FieldEval fieldEval = (FieldEval) function.getArgs()[i];
        if (context.getQueryBlock().isConstReference(fieldEval.getName())) {
          function.setArg(i, context.getQueryBlock().getConstByReference(fieldEval.getName()));
          continue;
        }
      }

      visit(context, function.getArgs()[i], stack);
    }

    stack.pop();

    return function;
  }
}
