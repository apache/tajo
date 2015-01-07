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
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.LogicalNode;

import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;

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

  public static VerificationState verify(VerificationState state, LogicalNode currentNode, EvalNode expression)
      throws PlanningException {
    instance.visitChild(state, expression, new Stack<EvalNode>());
    Set<Column> referredColumns = EvalTreeUtil.findUniqueColumns(expression);
    for (Column referredColumn : referredColumns) {
      if (!currentNode.getInSchema().contains(referredColumn)) {
        throw new PlanningException("Invalid State: " + referredColumn + " cannot be accessible at Node ("
            + currentNode.getPID() + ")");
      }
    }
    return state;
  }

  /**
   * It checks the compatibility of two data types.
   */
  private static boolean isCompatibleType(DataType dataType1, DataType dataType2) {
    if (checkNumericType(dataType1) && checkNumericType(dataType2)) {
      return true;
    }

    if (checkTextData(dataType1) && checkTextData(dataType2)) {
      return true;
    }

    if (checkDateTime(dataType1) && checkDateTime(dataType2)) {
      return true;
    }

    if (checkNetworkType(dataType1) && checkNetworkType(dataType2)) {
      return true;
    }

    return false;
  }

  /**
   * It checks both expressions in a comparison operator are compatible to each other.
   */
  private static void verifyComparisonOperator(VerificationState state, BinaryEval expr) {
    DataType leftType = expr.getLeftExpr().getValueType();
    DataType rightType = expr.getRightExpr().getValueType();
    if (!isCompatibleType(leftType, rightType)) {
      state.addVerification("No operator matches the given name and argument type(s): " + expr.toString());
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
        state.addVerification("division by zero");
      }
    }
  }

  private static void checkArithmeticOperand(VerificationState state, BinaryEval evalNode) {
    EvalNode leftExpr = evalNode.getLeftExpr();
    EvalNode rightExpr = evalNode.getRightExpr();

    DataType leftDataType = leftExpr.getValueType();
    DataType rightDataType = rightExpr.getValueType();

    Type leftType = leftDataType.getType();
    Type rightType = rightDataType.getType();

    if (leftType == Type.DATE &&
          (checkIntType(rightDataType) ||
              rightType == Type.DATE || rightType == Type.INTERVAL || rightType == Type.TIME)) {
      return;
    }

    if (leftType == Type.INTERVAL &&
        (checkNumericType(rightDataType) ||
            rightType == Type.DATE || rightType == Type.INTERVAL || rightType == Type.TIME ||
            rightType == Type.TIMESTAMP)) {
      return;
    }

    if (leftType == Type.TIME &&
        (rightType == Type.DATE || rightType == Type.INTERVAL || rightType == Type.TIME)) {
      return;
    }

    if (leftType == Type.TIMESTAMP &&
        (rightType == Type.TIMESTAMP || rightType == Type.INTERVAL || rightType == Type.TIME)) {
      return;
    }

    if (!(checkNumericType(leftDataType) && checkNumericType(rightDataType))) {
      state.addVerification("No operator matches the given name and argument type(s): " + evalNode.toString());
    }
  }

  private static boolean checkNetworkType(DataType dataType) {
    return dataType.getType() == Type.INET4 || dataType.getType() == Type.INET6;
  }

  private static boolean checkIntType(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return Type.INT1.getNumber() < typeNumber && typeNumber <= Type.INT8.getNumber();
  }

  private static boolean checkNumericType(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return Type.INT1.getNumber() <= typeNumber && typeNumber <= Type.NUMERIC.getNumber();
  }

  private static boolean checkTextData(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return Type.CHAR.getNumber() <= typeNumber && typeNumber <= Type.TEXT.getNumber();
  }

  private static boolean checkDateTime(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return (Type.DATE.getNumber() <= typeNumber && typeNumber <= Type.INTERVAL.getNumber()) ||
        (Type.TIMEZ.getNumber() <= typeNumber && typeNumber <= Type.TIMESTAMPZ.getNumber());
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
        visitChild(context, param, stack);
      }
    }
    return evalNode;
  }
}
