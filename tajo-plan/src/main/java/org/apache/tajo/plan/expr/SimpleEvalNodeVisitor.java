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

package org.apache.tajo.plan.expr;

import com.google.common.base.Preconditions;
import org.apache.tajo.exception.TajoInternalError;

import java.util.Stack;

/**
 * It provides simple visitor methods for an expression tree. Since <code>SimpleEvalNodeVisitor</code> provides
 * fewer visitor methods, it allows users to write a simple rewriter for expression trees.
 */
public abstract class SimpleEvalNodeVisitor<CONTEXT> {

  public EvalNode visit(CONTEXT context, EvalNode evalNode, Stack<EvalNode> stack) {
    Preconditions.checkNotNull(evalNode);

    EvalNode result;

    if (evalNode instanceof UnaryEval) {
      result = visitUnaryEval(context, (UnaryEval) evalNode, stack);
    } else if (evalNode instanceof BinaryEval) {
      result = visitBinaryEval(context, stack, (BinaryEval) evalNode);
    } else {

      switch (evalNode.getType()) {
      // Column and Value reference expressions
      case CONST:
        result = visitConst(context, (ConstEval) evalNode, stack);
        break;
      case ROW_CONSTANT:
        result = visitRowConstant(context, (RowConstantEval) evalNode, stack);
        break;
      case FIELD:
        result = visitField(context, (FieldEval) evalNode, stack);
        break;


      // SQL standard predicates
      case BETWEEN:
        result = visitBetween(context, (BetweenPredicateEval) evalNode, stack);
        break;
      case CASE:
        result = visitCaseWhen(context, (CaseWhenEval) evalNode, stack);
        break;
      case IF_THEN:
        result = visitIfThen(context, (CaseWhenEval.IfThenEval) evalNode, stack);
        break;

      // Functions
      case FUNCTION:
        result = visitFuncCall(context, (FunctionEval) evalNode, stack);
        break;
      case AGG_FUNCTION:
        result = visitFuncCall(context, (FunctionEval) evalNode, stack);
        break;
      case WINDOW_FUNCTION:
        result = visitFuncCall(context, (FunctionEval) evalNode, stack);
        break;

      case SUBQUERY:
        result = visitSubquery(context, (SubqueryEval) evalNode, stack);
        break;

      default:
        throw new TajoInternalError("Unknown EvalType: " + evalNode);
      }
    }

    return result;
  }

  protected EvalNode visitUnaryEval(CONTEXT context, UnaryEval unaryEval, Stack<EvalNode> stack) {
    stack.push(unaryEval);
    visit(context, unaryEval.getChild(), stack);
    stack.pop();
    return unaryEval;
  }

  protected EvalNode visitBinaryEval(CONTEXT context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    visit(context, binaryEval.getLeftExpr(), stack);
    visit(context, binaryEval.getRightExpr(), stack);
    stack.pop();
    return binaryEval;
  }

  protected EvalNode visitDefaultFunctionEval(CONTEXT context, Stack<EvalNode> stack, FunctionEval functionEval) {
    stack.push(functionEval);
    if (functionEval.getArgs() != null) {
      for (EvalNode arg : functionEval.getArgs()) {
        visit(context, arg, stack);
      }
    }
    stack.pop();
    return functionEval;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Value and Literal
  ///////////////////////////////////////////////////////////////////////////////////////////////

  protected EvalNode visitConst(CONTEXT context, ConstEval evalNode, Stack<EvalNode> stack) {
    return evalNode;
  }

  protected EvalNode visitRowConstant(CONTEXT context, RowConstantEval evalNode, Stack<EvalNode> stack) {
    return evalNode;
  }

  protected EvalNode visitField(CONTEXT context, FieldEval evalNode, Stack<EvalNode> stack) {
    return evalNode;
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////
  // SQL standard predicates
  ///////////////////////////////////////////////////////////////////////////////////////////////

  protected EvalNode visitBetween(CONTEXT context, BetweenPredicateEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getPredicand(), stack);
    visit(context, evalNode.getBegin(), stack);
    visit(context, evalNode.getEnd(), stack);
    return evalNode;
  }

  protected EvalNode visitCaseWhen(CONTEXT context, CaseWhenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    for (CaseWhenEval.IfThenEval ifThenEval : evalNode.getIfThenEvals()) {
      visitIfThen(context, ifThenEval, stack);
    }
    if (evalNode.hasElse()) {
      visit(context, evalNode.getElse(), stack);
    }
    stack.pop();
    return evalNode;
  }

  protected EvalNode visitIfThen(CONTEXT context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getCondition(), stack);
    visit(context, evalNode.getResult(), stack);
    stack.pop();
    return evalNode;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Functions
  ///////////////////////////////////////////////////////////////////////////////////////////////

  protected EvalNode visitFuncCall(CONTEXT context, FunctionEval evalNode, Stack<EvalNode> stack) {
    return visitDefaultFunctionEval(context, stack, evalNode);
  }

  protected EvalNode visitSubquery(CONTEXT context, SubqueryEval evalNode, Stack<EvalNode> stack) {
    return evalNode;
  }
}
