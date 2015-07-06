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

package org.apache.tajo.plan.util;

import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.plan.expr.*;

import java.util.Stack;

/**
 *  This build SQL statements for getting partitions informs on CatalogStore with partition filters.
 *
 *  This can get partition informs by two columns value, such as, column name, partition value.
 *  And above columns type are text type. Thus this will assume type of FieldEval to text type.
 *
 *  Also the SQL result will be executed on CatalogStore, such as, DerbyDB, MySQL, etc.
 *  Therefore, this need to avoid distinctive SQL usages (ie: cast(PARTITION_VALUE as INT8).
 */
public class SQLFinderWithPartitionFilter extends SimpleEvalNodeVisitor<Object>{
  Stack<String> stringStack = new Stack<String>();

  public String getResult() {
    return stringStack.pop();
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Value and Literal
  ///////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitConst(Object context, ConstEval evalNode, Stack<EvalNode> stack) {
    StringBuilder sb = new StringBuilder();
    sb.append("'").append(evalNode.getValue().asChars()).append("'");
    stringStack.push(sb.toString());
    return evalNode;
  }

  @Override
  public EvalNode visitRowConstant(Object context, RowConstantEval evalNode, Stack<EvalNode> stack) {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for(int i = 0; i < evalNode.getValues().length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("'").append(evalNode.getValues()[i].asChars()).append("'");
    }
    sb.append(")");
    stringStack.push(sb.toString());
    return evalNode;
  }

  @Override
  public EvalNode visitField(Object context, Stack<EvalNode> stack, FieldEval evalNode) {
    StringBuilder sb = new StringBuilder();
    sb.append(evalNode.getColumnRef().getSimpleName());
    stringStack.push(sb.toString());
    return evalNode;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // SQL standard predicates
  ///////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitBetween(Object context, BetweenPredicateEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);

    visit(context, evalNode.getPredicand(), stack);
    String predicandSql = stringStack.pop();

    visit(context, evalNode.getBegin(), stack);
    String beginSql= stringStack.pop();

    visit(context, evalNode.getEnd(), stack);
    String endSql = stringStack.pop();

    StringBuilder sb = new StringBuilder();
    sb.append(predicandSql);
    sb.append(" BETWEEN ");
    sb.append(beginSql);
    sb.append(" AND ");
    sb.append(endSql);
    stringStack.push(sb.toString());

    return evalNode;
  }

  @Override
  public EvalNode visitCaseWhen(Object context, CaseWhenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);

    StringBuilder sb = new StringBuilder();
    sb.append("CASE ");

    for (CaseWhenEval.IfThenEval ifThenEval : evalNode.getIfThenEvals()) {
      visitIfThen(context, ifThenEval, stack);
      String whenSql = stringStack.pop();
      sb.append(whenSql).append(" ");
    }

    if (evalNode.hasElse()) {
      visit(context, evalNode.getElse(), stack);
      String elseSql = stringStack.pop();
      sb.append("ELSE ").append(elseSql).append(" END");
    }
    stack.pop();

    return evalNode;
  }

  @Override
  public EvalNode visitIfThen(Object context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack) {
    StringBuilder sb = new StringBuilder();
    stack.push(evalNode);

    visit(context, evalNode.getCondition(), stack);
    String conditionSql = stringStack.pop();

    visit(context, evalNode.getResult(), stack);
    String resultSql = stringStack.pop();

    sb.append("WHEN ");
    sb.append(conditionSql);
    sb.append(" THEN ");
    sb.append(resultSql);
    stringStack.push(sb.toString());

    stack.pop();
    return evalNode;
  }

  @Override
  public EvalNode visitBinaryEval(Object context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    EvalNode lhs = visit(context, binaryEval.getLeftExpr(), stack);
    String leftSql = stringStack.pop();
    EvalNode rhs = visit(context, binaryEval.getRightExpr(), stack);
    String rightSql = stringStack.pop();
    stack.pop();

    if (!binaryEval.getLeftExpr().equals(lhs)) {
      binaryEval.setLeftExpr(lhs);
    }
    if (!binaryEval.getRightExpr().equals(rhs)) {
      binaryEval.setRightExpr(rhs);
    }

    if (lhs.getType() == EvalType.CONST && rhs.getType() == EvalType.CONST) {
      return new ConstEval(binaryEval.bind(null, null).eval(null));
    }

    StringBuilder sb = new StringBuilder();
    sb.append(leftSql);
    sb.append(" ").append(binaryEval.getType().getOperatorName()).append(" ");
    sb.append(rightSql);
    stringStack.push(sb.toString());

    return binaryEval;
  }

  @Override
  public EvalNode visitUnaryEval(Object context, Stack<EvalNode> stack, UnaryEval unaryEval) {
    stack.push(unaryEval);
    EvalNode child = visit(context, unaryEval.getChild(), stack);
    stack.pop();

    if (child.getType() == EvalType.CONST) {
      return new ConstEval(unaryEval.bind(null, null).eval(null));
    }

    String childSql = stringStack.pop();

    StringBuilder sb = new StringBuilder();
    if (unaryEval.getType() == EvalType.IS_NULL) {
      IsNullEval isNullEval = (IsNullEval) unaryEval;
      sb.append(childSql);
      sb.append(" IS ");
      if (isNullEval.isNot()) {
        sb.append("NOT NULL");
      } else {
        sb.append("NULL");
      }
    } else if (unaryEval.getType() == EvalType.CAST) {
      if (childSql.equals(CatalogConstants.COL_PARTITION_VALUE)) {
        sb.append(childSql);
      } else {
        sb.append("CAST (");
        sb.append(childSql);
        sb.append(" AS ");
        sb.append(unaryEval.getType());
        sb.append(")");
      }
    } else if (unaryEval.getType() == EvalType.NOT) {
      sb.append("NOT ");
      sb.append(childSql);
    } else if (unaryEval.getType() == EvalType.SIGNED) {
      SignedEval signedEval = (SignedEval)unaryEval;
      if (signedEval.isNegative()) {
        sb.append("-");
      } else {
        sb.append("+");
      }
      sb.append(childSql);
    }
    stringStack.push(sb.toString());

    return unaryEval;
  }
}
