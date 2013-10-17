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

package org.apache.tajo.engine.planner;

import org.apache.tajo.engine.eval.*;

import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class ExprsVerifier extends BasicEvalNodeVisitor<VerificationState, EvalNode> {
  private static final ExprsVerifier instance;

  static {
    instance = new ExprsVerifier();
  }

  public static VerificationState verify(VerificationState state, EvalNode expression) {
    instance.visitChild(state, expression, new Stack<EvalNode>());
    return state;
  }

  private static boolean isCompatibleType(DataType dataType1, DataType dataType2) {
    if (checkNumericType(dataType1) && checkNumericType(dataType2)) {
      return true;
    }

    if (checkTextData(dataType1) && checkTextData(dataType2)) {
      return true;
    }

    return false;
  }

  private static void verifyComparisonOperator(VerificationState state, BinaryEval expr) {
    DataType leftType = expr.getLeftExpr().getValueType();
    DataType rightType = expr.getRightExpr().getValueType();
    if (!isCompatibleType(leftType, rightType)) {
      state.addVerification("No operator matches the given name and argument type(s): " + expr.toString());
    }
  }

  public EvalNode visitEqual(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitEqual(state, expr, stack);
    verifyComparisonOperator(state, expr);
    return expr;
  }

  public EvalNode visitNotEqual(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitNotEqual(state, expr, stack);
    verifyComparisonOperator(state, expr);
    return expr;
  }

  @Override
  public EvalNode visitLessThan(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitLessThan(state, expr, stack);
    verifyComparisonOperator(state, expr);
    return expr;
  }

  @Override
  public EvalNode visitLessThanOrEqual(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitLessThanOrEqual(state, expr, stack);
    verifyComparisonOperator(state, expr);
    return expr;
  }

  @Override
  public EvalNode visitGreaterThan(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitGreaterThan(state, expr, stack);
    verifyComparisonOperator(state, expr);
    return expr;
  }

  @Override
  public EvalNode visitGreaterThanOrEqual(VerificationState state, BinaryEval expr, Stack<EvalNode> stack) {
    super.visitGreaterThanOrEqual(state, expr, stack);
    verifyComparisonOperator(state, expr);
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
    if (!(checkNumericType(leftExpr.getValueType())
        && checkNumericType(rightExpr.getValueType()))) {
      state.addVerification("No operator matches the given name and argument type(s): " + evalNode.toString());
    }
  }

  private static boolean checkNumericType(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return Type.INT1.getNumber() < typeNumber && typeNumber <= Type.DECIMAL.getNumber();
  }

  private static boolean checkTextData(DataType dataType) {
    int typeNumber = dataType.getType().getNumber();
    return Type.CHAR.getNumber() < typeNumber && typeNumber <= Type.TEXT.getNumber();
  }

  @Override
  public EvalNode visitPlus(VerificationState state, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(state, evalNode, stack);
    checkArithmeticOperand(state, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitMinus(VerificationState state, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(state, evalNode, stack);
    checkArithmeticOperand(state, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitMultiply(VerificationState state, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(state, evalNode, stack);
    checkArithmeticOperand(state, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitDivide(VerificationState state, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(state, evalNode, stack);
    checkArithmeticOperand(state, evalNode);
    checkDivisionByZero(state, evalNode);
    return evalNode;
  }

  @Override
  public EvalNode visitModular(VerificationState state, BinaryEval evalNode, Stack<EvalNode> stack) {
    super.visitDivide(state, evalNode, stack);
    checkArithmeticOperand(state, evalNode);
    checkDivisionByZero(state, evalNode);
    return evalNode;
  }
}
