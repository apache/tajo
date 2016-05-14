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

package org.apache.tajo.plan.verifier;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UndefinedOperatorException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.LogicalNode;

import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

/**
 * It verifies one predicate or expression with the semantic and data type checks as follows:
 * <ul>
 *   <ul>Both expressions in a binary expression are compatible to each other</ul>
 *   <ul>All column references of one expression are available at this node</ul>
 * </ul>
 */
public class ExprsVerifier extends BasicEvalNodeVisitor<VerificationState, EvalNode> {
  private static final ExprsVerifier instance;

  static {
    instance = new ExprsVerifier();
  }

  public static VerificationState verify(VerificationState state, LogicalNode currentNode, EvalNode expression) {
    instance.visit(state, expression, new Stack<>());
    Set<Column> referredColumns = EvalTreeUtil.findUniqueColumns(expression);
    for (Column referredColumn : referredColumns) {
      if (!currentNode.getInSchema().contains(referredColumn)) {
        throw new TajoInternalError("Invalid State: " + referredColumn + " cannot be accessible at Node ("
            + currentNode.getPID() + ")");
      }
    }
    return state;
  }

  /**
   * It checks the compatibility of two data types.
   */
  private static boolean isCompatibleType(org.apache.tajo.type.Type dataType1, org.apache.tajo.type.Type dataType2) {
    if (checkNumericType(dataType1) && checkNumericType(dataType2)) {
      return true;
    }

    if (checkTextData(dataType1) && checkTextData(dataType2)) {
      return true;
    }

    if (checkDateTime(dataType1) && checkDateTime(dataType2)) {
      return true;
    }

    return false;
  }

  /**
   * It checks both expressions in a comparison operator are compatible to each other.
   */
  private static void verifyComparisonOperator(VerificationState state, BinaryEval expr) {
    if (!isCompatibleType(expr.getLeftExpr().getValueType(), expr.getRightExpr().getValueType())) {
      state.addVerification(new UndefinedOperatorException(expr.toString()));
    }
  }

  public EvalNode visitEqual(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitEqual(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  public EvalNode visitNotEqual(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitNotEqual(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  @Override
  public EvalNode visitLessThan(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitLessThan(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  @Override
  public EvalNode visitLessThanOrEqual(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitLessThanOrEqual(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  @Override
  public EvalNode visitGreaterThan(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitGreaterThan(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  @Override
  public EvalNode visitGreaterThanOrEqual(VerificationState context, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitGreaterThanOrEqual(context, expr, stack);
    verifyComparisonOperator(context, expr);
    return expr;
  }

  private static void checkDivisionByZero(VerificationState state, BinaryEval evalNode) {
    if (evalNode.getRightExpr().getType() == EvalType.CONST) {
      ConstEval constEval = evalNode.getRightExpr();
      if (constEval.getValue().asFloat8() == 0) {
        state.addVerification(new TajoException(Errors.ResultCode.DIVISION_BY_ZERO, evalNode.toString()));
      }
    }
  }

  private static void checkArithmeticOperand(VerificationState state, BinaryEval evalNode) {
    EvalNode leftExpr = evalNode.getLeftExpr();
    EvalNode rightExpr = evalNode.getRightExpr();

    org.apache.tajo.type.Type leftDataType = leftExpr.getValueType();
    org.apache.tajo.type.Type rightDataType = rightExpr.getValueType();

    TajoDataTypes.Type leftType = leftDataType.kind();
    TajoDataTypes.Type rightType = rightDataType.kind();

    if (leftType == DATE &&
          (checkIntType(rightDataType) ||
              rightType == DATE || rightType == INTERVAL || rightType == TIME)) {
      return;
    }

    if (leftType == INTERVAL &&
        (checkNumericType(rightDataType) ||
            rightType == DATE || rightType == INTERVAL || rightType == TIME ||
            rightType == TIMESTAMP)) {
      return;
    }

    if (leftType == TIME &&
        (rightType == DATE || rightType == INTERVAL || rightType == TIME ||
            rightType == TIMESTAMP)) {
      return;
    }

    if (leftType == TIMESTAMP &&
        (rightType == TIMESTAMP || rightType == INTERVAL || rightType == TajoDataTypes.Type.TIME)) {
      return;
    }

    if (!(checkNumericType(leftDataType) && checkNumericType(rightDataType))) {
      state.addVerification(new UndefinedOperatorException(evalNode.toString()));
    }
  }

  private static boolean checkIntType(org.apache.tajo.type.Type dataType) {
    int typeNumber = dataType.kind().getNumber();
    return INT1.getNumber() < typeNumber && typeNumber <= INT8.getNumber();
  }

  private static boolean checkNumericType(org.apache.tajo.type.Type dataType) {
    int typeNumber = dataType.kind().getNumber();
    return INT1.getNumber() <= typeNumber && typeNumber <= NUMERIC.getNumber();
  }

  private static boolean checkTextData(org.apache.tajo.type.Type dataType) {
    int typeNumber = dataType.kind().getNumber();
    return CHAR.getNumber() <= typeNumber && typeNumber <= TEXT.getNumber();
  }

  private static boolean checkDateTime(org.apache.tajo.type.Type dataType) {
    int typeNumber = dataType.kind().getNumber();
    return (DATE.getNumber() <= typeNumber && typeNumber <= INTERVAL.getNumber()) ||
        (TIMEZ.getNumber() <= typeNumber && typeNumber <= TIMESTAMPZ.getNumber());
  }

  @Override
  public EvalNode visitPlus(VerificationState context, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitPlus(context, evalNode, stack);
    checkArithmeticOperand(context, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitMinus(VerificationState context, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitMinus(context, evalNode, stack);
    checkArithmeticOperand(context, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitMultiply(VerificationState context, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitMultiply(context, evalNode, stack);
    checkArithmeticOperand(context, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitDivide(VerificationState context, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(context, evalNode, stack);
    checkArithmeticOperand(context, evalNode);
    checkDivisionByZero(context, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitModular(VerificationState context, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(context, evalNode, stack);
    checkArithmeticOperand(context, evalNode);
    checkDivisionByZero(context, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitFuncCall(VerificationState context, GeneralFunctionEval evalNode, Stack<EvalNode> stack) {
    super.visitFuncCall(context, evalNode, stack);
    if (evalNode.getArgs() != null) {
      for (EvalNode param : evalNode.getArgs()) {
        visit(context, param, stack);
      }
    }
    return evalNode;
  }
}
