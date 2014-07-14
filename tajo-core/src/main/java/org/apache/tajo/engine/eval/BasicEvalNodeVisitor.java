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

import org.apache.tajo.exception.UnsupportedException;

import java.util.Stack;

public class BasicEvalNodeVisitor<CONTEXT, RESULT> implements EvalNodeVisitor2<CONTEXT, RESULT> {

  @Override
  public RESULT visitChild(CONTEXT context, EvalNode evalNode, Stack<EvalNode> stack) {
    RESULT result;
    switch (evalNode.getType()) {
      // Column and Value reference expressions
      case CONST:
        result = visitConst(context, (ConstEval) evalNode, stack);
        break;
      case ROW_CONSTANT:
        result = visitRowConstant(context, (RowConstantEval) evalNode, stack);
        break;
      case FIELD:
        result = visitField(context, stack, (FieldEval) evalNode);
        break;

      // Arithmetic expression
      case PLUS:
        result = visitPlus(context, (BinaryEval) evalNode, stack);
        break;
      case MINUS:
        result = visitMinus(context, (BinaryEval) evalNode, stack);
        break;
      case MULTIPLY:
        result = visitMultiply(context, (BinaryEval) evalNode, stack);
        break;
      case DIVIDE:
        result = visitDivide(context, (BinaryEval) evalNode, stack);
        break;
      case MODULAR:
        result = visitModular(context, (BinaryEval) evalNode, stack);
        break;

      // Logical Predicates
      case AND:
        result = visitAnd(context, (BinaryEval) evalNode, stack);
        break;
      case OR:
        result = visitOr(context, (BinaryEval) evalNode, stack);
        break;
      case NOT:
        result = visitNot(context, (NotEval) evalNode, stack);
        break;

      // Comparison Predicates
      case EQUAL:
        result = visitEqual(context, (BinaryEval) evalNode, stack);
        break;
      case NOT_EQUAL:
        result = visitNotEqual(context, (BinaryEval) evalNode, stack);
        break;
      case LTH:
        result = visitLessThan(context, (BinaryEval) evalNode, stack);
        break;
      case LEQ:
        result = visitLessThanOrEqual(context, (BinaryEval) evalNode, stack);
        break;
      case GTH:
        result = visitGreaterThan(context, (BinaryEval) evalNode, stack);
        break;
      case GEQ:
        result = visitGreaterThanOrEqual(context, (BinaryEval) evalNode, stack);
        break;

      // SQL standard predicates
      case IS_NULL:
        result = visitIsNull(context, (IsNullEval) evalNode, stack);
        break;
      case BETWEEN:
        result = visitBetween(context, (BetweenPredicateEval) evalNode, stack);
        break;
      case CASE:
        result = visitCaseWhen(context, (CaseWhenEval) evalNode, stack);
        break;
      case IF_THEN:
        result = visitIfThen(context, (CaseWhenEval.IfThenEval) evalNode, stack);
        break;
      case IN:
        result = visitInPredicate(context, (InEval) evalNode, stack);
        break;

      // String operators and Pattern match predicates
      case LIKE:
        result = visitLike(context, (LikePredicateEval) evalNode, stack);
        break;
      case SIMILAR_TO:
        result = visitSimilarTo(context, (SimilarToPredicateEval) evalNode, stack);
        break;
      case REGEX:
        result = visitRegex(context, (RegexPredicateEval) evalNode, stack);
        break;
      case CONCATENATE:
        result = visitConcatenate(context, (BinaryEval) evalNode, stack);
        break;

      // Functions
      case FUNCTION:
        result = visitFuncCall(context, (GeneralFunctionEval) evalNode, stack);
        break;
      case AGG_FUNCTION:
        result = visitAggrFuncCall(context, (AggregationFunctionCallEval) evalNode, stack);
        break;
      case WINDOW_FUNCTION:
        result = visitWindowFunc(context, (WindowFunctionEval) evalNode, stack);
      break;

      case SIGNED:
        result = visitSigned(context, (SignedEval) evalNode, stack);
        break;
      case CAST:
        result = visitCast(context, (CastEval) evalNode, stack);
        break;

      default:
        throw new UnsupportedException("Unknown EvalType: " + evalNode);
    }

    return result;
  }

  private RESULT visitDefaultUnaryEval(CONTEXT context, UnaryEval unaryEval, Stack<EvalNode> stack) {
    stack.push(unaryEval);
    RESULT result = visitChild(context, unaryEval.getChild(), stack);
    stack.pop();
    return result;
  }

  private RESULT visitDefaultBinaryEval(CONTEXT context, BinaryEval binaryEval, Stack<EvalNode> stack) {
    stack.push(binaryEval);
    RESULT result = visitChild(context, binaryEval.getLeftExpr(), stack);
    visitChild(context, binaryEval.getRightExpr(), stack);
    stack.pop();
    return result;
  }

  private RESULT visitDefaultFunctionEval(CONTEXT context, FunctionEval functionEval, Stack<EvalNode> stack) {
    RESULT result = null;
    stack.push(functionEval);
    if (functionEval.getArgs() != null) {
      for (EvalNode arg : functionEval.getArgs()) {
        result = visitChild(context, arg, stack);
      }
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitConst(CONTEXT context, ConstEval evalNode, Stack<EvalNode> stack) {
    return null;
  }

  @Override
  public RESULT visitRowConstant(CONTEXT context, RowConstantEval evalNode, Stack<EvalNode> stack) {
    return null;
  }

  @Override
  public RESULT visitField(CONTEXT context, Stack<EvalNode> stack, FieldEval evalNode) {
    return null;
  }

  @Override
  public RESULT visitPlus(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitMinus(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitMultiply(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitDivide(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitModular(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitAnd(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitOr(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitNot(CONTEXT context, NotEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultUnaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitNotEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitLessThan(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitLessThanOrEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitGreaterThan(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitGreaterThanOrEqual(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitIsNull(CONTEXT context, IsNullEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultUnaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitBetween(CONTEXT context, BetweenPredicateEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    RESULT result = visitChild(context, evalNode.getPredicand(), stack);
    visitChild(context, evalNode.getBegin(), stack);
    visitChild(context, evalNode.getEnd(), stack);
    return result;
  }

  @Override
  public RESULT visitCaseWhen(CONTEXT context, CaseWhenEval evalNode, Stack<EvalNode> stack) {
    RESULT result = null;
    stack.push(evalNode);
    for (CaseWhenEval.IfThenEval ifThenEval : evalNode.getIfThenEvals()) {
      result = visitIfThen(context, ifThenEval, stack);
    }
    if (evalNode.hasElse()) {
      result = visitChild(context, evalNode.getElse(), stack);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIfThen(CONTEXT context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack) {
    RESULT result;
    stack.push(evalNode);
    result = visitChild(context, evalNode.getCondition(), stack);
    visitChild(context, evalNode.getResult(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitInPredicate(CONTEXT context, InEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitLike(CONTEXT context, LikePredicateEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitSimilarTo(CONTEXT context, SimilarToPredicateEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitRegex(CONTEXT context, RegexPredicateEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitConcatenate(CONTEXT context, BinaryEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultBinaryEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitFuncCall(CONTEXT context, GeneralFunctionEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultFunctionEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitAggrFuncCall(CONTEXT context, AggregationFunctionCallEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultFunctionEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitWindowFunc(CONTEXT context, WindowFunctionEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultFunctionEval(context, evalNode, stack);
  }

  @Override
  public RESULT visitSigned(CONTEXT context, SignedEval signedEval, Stack<EvalNode> stack) {
    return visitDefaultUnaryEval(context, signedEval, stack);
  }

  @Override
  public RESULT visitCast(CONTEXT context, CastEval castEval, Stack<EvalNode> stack) {
    return visitDefaultUnaryEval(context, castEval, stack);
  }
}
