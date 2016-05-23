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

import org.apache.tajo.algebra.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DateDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.*;

import java.util.Stack;

/**
 * This converts EvalNode tree to Expr tree.
 *
 */
public class EvalNodeToExprConverter extends SimpleEvalNodeVisitor<Object> {
  private Stack<Expr> exprs = new Stack<>();

  private String tableName;

  public EvalNodeToExprConverter(String tableName) {
    this.tableName = tableName;
  }

  public Expr getResult() {
    return exprs.pop();
  }

  @Override
  protected EvalNode visitBinaryEval(Object o, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    visit(o, binaryEval.getLeftExpr(), stack);
    Expr left = exprs.pop();

    visit(o, binaryEval.getRightExpr(), stack);
    Expr right = exprs.pop();

    Expr expr = null;
    switch (binaryEval.getType()) {
      // Arithmetic expression
      case PLUS:
        expr = new BinaryOperator(OpType.Plus, left, right);
        break;
      case MINUS:
        expr = new BinaryOperator(OpType.Minus, left, right);
        break;
      case MULTIPLY:
        expr = new BinaryOperator(OpType.Multiply, left, right);
        break;
      case DIVIDE:
        expr = new BinaryOperator(OpType.Divide, left, right);
        break;
      case MODULAR:
        expr = new BinaryOperator(OpType.Modular, left, right);
        break;

      // Logical Predicates
      case AND:
        expr = new BinaryOperator(OpType.And, left, right);
        break;
      case OR:
        expr = new BinaryOperator(OpType.Or, left, right);
        break;
      case NOT:
        expr = new BinaryOperator(OpType.Not, left, right);
        break;

      // Comparison Predicates
      case EQUAL:
        expr = new BinaryOperator(OpType.Equals, left, right);
        break;
      case NOT_EQUAL:
        expr = new BinaryOperator(OpType.NotEquals, left, right);
        break;
      case LTH:
        expr = new BinaryOperator(OpType.LessThan, left, right);
        break;
      case LEQ:
        expr = new BinaryOperator(OpType.LessThanOrEquals, left, right);
        break;
      case GTH:
        expr = new BinaryOperator(OpType.GreaterThan, left, right);
        break;
      case GEQ:
        expr = new BinaryOperator(OpType.GreaterThanOrEquals, left, right);
        break;

      // SQL standard predicates
      case IS_NULL:
        expr = new BinaryOperator(OpType.IsNullPredicate, left, right);
        break;
      case CASE:
        expr = new BinaryOperator(OpType.CaseWhen, left, right);
        break;
      case IN:
        InEval inEval = (InEval) binaryEval;
        expr = new InPredicate(left, right, inEval.isNot());
        break;

      // String operators and Pattern match predicates
      case LIKE:
        LikePredicateEval likePredicateEval = (LikePredicateEval) binaryEval;
        expr = new PatternMatchPredicate(OpType.LikePredicate, likePredicateEval.isNot(), left, right);
        break;
      case SIMILAR_TO:
        SimilarToPredicateEval similarToPredicateEval = (SimilarToPredicateEval) binaryEval;
        expr = new PatternMatchPredicate(OpType.SimilarToPredicate, similarToPredicateEval.isNot(), left, right);
        break;
      case REGEX:
        RegexPredicateEval regexPredicateEval = (RegexPredicateEval) binaryEval;
        expr = new PatternMatchPredicate(OpType.Regexp, regexPredicateEval.isNot(), left, right);
        break;
      case CONCATENATE:
      default:
        throw new RuntimeException("Unsupported type: " + binaryEval.getType().name());
    }

    if (expr != null) {
      exprs.push(expr);
    }

    stack.pop();
    return null;
  }

  @Override
  protected EvalNode visitConst(Object o, ConstEval evalNode, Stack<EvalNode> stack) {
    exprs.push(convertDatumToExpr(evalNode.getValueType().kind(), evalNode.getValue()));
    return super.visitConst(o, evalNode, stack);
  }

  @Override
  protected EvalNode visitRowConstant(Object o, RowConstantEval evalNode, Stack<EvalNode> stack) {
    Expr[] values = new Expr[evalNode.getValues().length];
    for (int i = 0; i < evalNode.getValues().length; i++) {
      Datum datum = evalNode.getValues()[i];
      values[i] = convertDatumToExpr(datum.kind(), datum);
    }
    ValueListExpr expr = new ValueListExpr(values);
    exprs.push(expr);

    return super.visitRowConstant(o, evalNode, stack);
  }

  /**
   * Convert specified Datum to Expr
   *
   * @param type value type
   * @param datum target datum
   * @return converted datum
   */
  private Expr convertDatumToExpr(TajoDataTypes.Type type, Datum datum) {
    Expr value = null;
    DateValue dateValue;
    TimeValue timeValue;

    switch (type) {
      case NULL_TYPE:
        value = new NullLiteral();
        break;
      case BOOLEAN:
        value = new BooleanLiteral(datum.asBool());
        break;
      case INT1:
      case INT2:
      case INT4:
        value = new LiteralValue(datum.asChars(), LiteralValue.LiteralType.Unsigned_Integer);
        break;
      case INT8:
        value = new LiteralValue(datum.asChars(), LiteralValue.LiteralType.Unsigned_Large_Integer);
        break;
      case FLOAT4:
      case FLOAT8:
        value = new LiteralValue(datum.asChars(), LiteralValue.LiteralType.Unsigned_Float);
        break;
      case TEXT:
        value = new LiteralValue(datum.asChars(), LiteralValue.LiteralType.String);
        break;
      case DATE:
        DateDatum dateDatum = (DateDatum) datum;

        dateValue = new DateValue(""+dateDatum.getYear(),
          ""+dateDatum.getMonthOfYear(), ""+dateDatum.getDayOfMonth());
        value = new DateLiteral(dateValue);

        break;
      case TIMESTAMP:
        TimestampDatum timestampDatum = (TimestampDatum) datum;

        dateValue = new DateValue(""+timestampDatum.getYear(),
          ""+timestampDatum.getMonthOfYear(), ""+timestampDatum.getDayOfMonth());

        timeValue = new TimeValue(""+timestampDatum.getHourOfDay()
          , ""+timestampDatum.getMinuteOfHour(), ""+timestampDatum.getSecondOfMinute());

        value = new TimestampLiteral(dateValue, timeValue);
        break;
      case TIME:
        TimeDatum timeDatum = (TimeDatum) datum;
        timeValue = new TimeValue(""+timeDatum.getHourOfDay()
          , ""+timeDatum.getMinuteOfHour(), ""+timeDatum.getSecondOfMinute());

        value = new TimeLiteral(timeValue);
        break;
      default:
        throw new RuntimeException(new UnsupportedException(type.name()));
    }

    return value;
  }

  @Override
  protected EvalNode visitField(Object o, FieldEval evalNode, Stack<EvalNode> stack)  {
    ColumnReferenceExpr expr = new ColumnReferenceExpr(tableName, evalNode.getColumnName());
    exprs.push(expr);
    return super.visitField(o, evalNode, stack);
  }

  @Override
  protected EvalNode visitBetween(Object o, BetweenPredicateEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);

    visit(o, evalNode.getPredicand(), stack);
    Expr predicand = exprs.pop();

    visit(o, evalNode.getBegin(), stack);
    Expr begin = exprs.pop();

    visit(o, evalNode.getEnd(), stack);
    Expr end = exprs.pop();

    Expr expr = new BetweenPredicate(evalNode.isNot(), evalNode.isSymmetric(), predicand, begin, end);
    exprs.push(expr);

    stack.pop();

    return null;
  }

  @Override
  protected EvalNode visitCaseWhen(Object o, CaseWhenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);

    CaseWhenPredicate caseWhenPredicate = new CaseWhenPredicate();

    for (CaseWhenEval.IfThenEval ifThenEval : evalNode.getIfThenEvals()) {
      visit(o, ifThenEval.getCondition(), stack);
      Expr condition = exprs.pop();
      visit(o, ifThenEval.getResult(), stack);
      Expr result = exprs.pop();

      caseWhenPredicate.addWhen(condition, result);
    }

    if (evalNode.hasElse()) {
      visit(o, evalNode.getElse(), stack);
      Expr elseResult = exprs.pop();
      caseWhenPredicate.setElseResult(elseResult);
    }

    exprs.push(caseWhenPredicate);

    stack.pop();

    return null;
  }
}