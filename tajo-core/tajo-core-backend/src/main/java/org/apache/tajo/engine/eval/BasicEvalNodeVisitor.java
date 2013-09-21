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

package org.apache.tajo.engine.eval;

import java.util.Stack;

public class BasicEvalNodeVisitor<CONTEXT, RESULT> implements EvalNodeVisitor2<CONTEXT, RESULT> {
  @Override
  public RESULT visitChild(CONTEXT context, Stack<EvalNode> stack, EvalNode evalNode) {
    RESULT result;
    switch (evalNode.getType()) {
      // Column and Value reference expressions
      case CONST:
        result = visitConst(context, stack, (ConstEval) evalNode);
        break;
      case ROW_CONSTANT:
        result = visitRowConstant(context, stack, (RowConstantEval) evalNode);
        break;
      case FIELD:
        result = visitField(context, stack, (FieldEval) evalNode);
        break;

      // Arithmetic expression
      case PLUS:
        result = visitPlus(context, stack, (BinaryEval) evalNode);
        break;
      case MINUS:
        result = visitMinus(context, stack, (BinaryEval) evalNode);
        break;
      case MULTIPLY:
        result = visitMultiply(context, stack, (BinaryEval) evalNode);
        break;
      case DIVIDE:
        result = visitDivide(context, stack, (BinaryEval) evalNode);
        break;
      case MODULAR:
        result = visitModular(context, stack, (BinaryEval) evalNode);
        break;

      // Logical Predicates
      case AND:
        result = visitAnd(context, stack, (BinaryEval) evalNode);
        break;
      case OR:
        result = visitOr(context, stack, (BinaryEval) evalNode);
        break;
      case NOT:
        result = visitNot(context, stack, (NotEval) evalNode);
        break;

      // Comparison Predicates
      case EQUAL:
        result = visitEqual(context, stack, (BinaryEval) evalNode);
        break;
      case NOT_EQUAL:
        result = visitNotEqual(context, stack, (BinaryEval) evalNode);
        break;
      case LTH:
        result = visitLessThan(context, stack, (BinaryEval) evalNode);
        break;
      case LEQ:
        result = visitLessThanOrEqual(context, stack, (BinaryEval) evalNode);
        break;
      case GTH:
        result = visitGreaterThan(context, stack, (BinaryEval) evalNode);
        break;
      case GEQ:
        result = visitGreaterThanOrEqual(context, stack, (BinaryEval) evalNode);
        break;

      // Other Predicates
      case IS_NULL:
        result = visitIsNull(context, stack, (IsNullEval) evalNode);
        break;
      case CASE:
        result = visitCaseWhen(context, stack, (CaseWhenEval) evalNode);
        break;
      case IF_THEN:
        result = visitIfThen(context, stack, (CaseWhenEval.IfThenEval) evalNode);
        break;
      case IN:
        result = visitInPredicate(context, stack, (InEval) evalNode);
        break;
      case LIKE:
        result = visitLike(context, stack, (LikeEval) evalNode);
        break;

      // Functions
      case FUNCTION:
        result = visitFuncCall(context, stack, (FuncCallEval) evalNode);
        break;
      case AGG_FUNCTION:
        result = visitAggrFuncCall(context, stack, (AggFuncCallEval) evalNode);
        break;

      default:
        throw new InvalidEvalException("Unknown EvalNode: " + evalNode);
    }

    return result;
  }

  private RESULT visitDefaultBinaryEval(CONTEXT context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    RESULT result = visitChild(context, stack, binaryEval.getLeftExpr());
    visitChild(context, stack, binaryEval.getRightExpr());
    stack.pop();
    return result;
  }

  private RESULT visitDefaultFunctionEval(CONTEXT context, Stack<EvalNode> stack, FuncEval functionEval) {
    RESULT result = null;
    stack.push(functionEval);
    for (EvalNode arg : functionEval.getArgs()) {
      result = visitChild(context, stack, arg);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitConst(CONTEXT context, Stack<EvalNode> stack, ConstEval evalNode) {
    return null;
  }

  @Override
  public RESULT visitRowConstant(CONTEXT context, Stack<EvalNode> stack, RowConstantEval evalNode) {
    return null;
  }

  @Override
  public RESULT visitField(CONTEXT context, Stack<EvalNode> stack, FieldEval evalNode) {
    return null;
  }

  @Override
  public RESULT visitPlus(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitMinus(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitMultiply(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitDivide(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitModular(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitAnd(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitOr(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitNot(CONTEXT context, Stack<EvalNode> stack, NotEval evalNode) {
    RESULT result;
    stack.push(evalNode);
    if (evalNode.getChild() instanceof NotEval) {
      result = visitChild(context, stack, evalNode);
    } else {
      result = visitChild(context, stack, evalNode.getLeftExpr());
      visitChild(context, stack, evalNode.getRightExpr());
    }
    stack.pop();

    return result;
  }

  @Override
  public RESULT visitEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitNotEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitLessThan(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitLessThanOrEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitGreaterThan(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitGreaterThanOrEqual(CONTEXT context, Stack<EvalNode> stack, BinaryEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitIsNull(CONTEXT context, Stack<EvalNode> stack, IsNullEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitCaseWhen(CONTEXT context, Stack<EvalNode> stack, CaseWhenEval evalNode) {
    RESULT result = null;
    stack.push(evalNode);
    for (CaseWhenEval.IfThenEval ifThenEval : evalNode.getIfThenEvals()) {
      result = visitIfThen(context, stack, ifThenEval);
    }
    if (evalNode.hasElse()) {
      result = visitChild(context, stack, evalNode.getElse());
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIfThen(CONTEXT context, Stack<EvalNode> stack, CaseWhenEval.IfThenEval evalNode) {
    RESULT result;
    stack.push(evalNode);
    result = visitChild(context, stack, evalNode.getConditionExpr());
    visitChild(context, stack, evalNode.getResultExpr());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitInPredicate(CONTEXT context, Stack<EvalNode> stack, InEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitLike(CONTEXT context, Stack<EvalNode> stack, LikeEval evalNode) {
    return visitDefaultBinaryEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitFuncCall(CONTEXT context, Stack<EvalNode> stack, FuncCallEval evalNode) {
    return visitDefaultFunctionEval(context, stack, evalNode);
  }

  @Override
  public RESULT visitAggrFuncCall(CONTEXT context, Stack<EvalNode> stack, AggFuncCallEval evalNode) {
    return visitDefaultFunctionEval(context, stack, evalNode);
  }
}
