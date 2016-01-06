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
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.*;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.TimeZone;

/**
 *  This build SQL statements for getting partitions informs on CatalogStore with algebra expressions.
 *  This visitor assumes that all columns of algebra expressions are reserved for one table.
 *
 */
public class PartitionFilterAlgebraVisitor extends SimpleAlgebraVisitor<Object, Expr> {
  private String tableAlias;
  private Column column;
  private boolean isHiveCatalog = false;

  private Stack<String> queries = new Stack();
  private List<Pair<Type, Object>> parameters = new ArrayList<>();

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
  }

  public boolean isHiveCatalog() {
    return isHiveCatalog;
  }

  public void setIsHiveCatalog(boolean isHiveCatalog) {
    this.isHiveCatalog = isHiveCatalog;
  }

  public List<Pair<Type, Object>> getParameters() {
    return parameters;
  }

  public void setParameters(List<Pair<Type, Object>> parameters) {
    this.parameters = parameters;
  }

  public void clearParameters() {
    this.parameters.clear();
  }

  public String getResult() {
    return queries.pop();
  }

  @Override
  public Expr visit(Object ctx, Stack<Expr> stack, Expr expr) throws TajoException {
    if (expr.getType() == OpType.LikePredicate) {
      return visitLikePredicate(ctx, stack, (PatternMatchPredicate) expr);
    } else if (expr.getType() == OpType.SimilarToPredicate) {
      return visitSimilarToPredicate(ctx, stack, (PatternMatchPredicate) expr);
    } else if (expr.getType() == OpType.Regexp) {
      return visitRegexpPredicate(ctx, stack, (PatternMatchPredicate) expr);
    }
    return super.visit(ctx, stack, expr);
  }

  @Override
  public Expr visitDateLiteral(Object ctx, Stack<Expr> stack, DateLiteral expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      sb.append("?").append(" )");
      parameters.add(new Pair(Type.DATE, Date.valueOf(expr.toString())));
    } else {
      sb.append("\"").append(expr.toString()).append("\"");
    }
    queries.push(sb.toString());
    return expr;
  }

  @Override
  public Expr visitTimestampLiteral(Object ctx, Stack<Expr> stack, TimestampLiteral expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      DateValue dateValue = expr.getDate();
      TimeValue timeValue = expr.getTime();

      int [] dates = ExprAnnotator.dateToIntArray(dateValue.getYears(),
        dateValue.getMonths(),
        dateValue.getDays());
      int [] times = ExprAnnotator.timeToIntArray(timeValue.getHours(),
        timeValue.getMinutes(),
        timeValue.getSeconds(),
        timeValue.getSecondsFraction());

      long julianTimestamp;
      if (timeValue.hasSecondsFraction()) {
        julianTimestamp = DateTimeUtil.toJulianTimestamp(dates[0], dates[1], dates[2], times[0], times[1], times[2],
          times[3] * 1000);
      } else {
        julianTimestamp = DateTimeUtil.toJulianTimestamp(dates[0], dates[1], dates[2], times[0], times[1], times[2], 0);
      }

      TimeMeta tm = new TimeMeta();
      DateTimeUtil.toJulianTimeMeta(julianTimestamp, tm);

      TimeZone tz = TimeZone.getDefault();
      DateTimeUtil.toUTCTimezone(tm, tz);

      sb.append("?").append(" )");
      Timestamp timestamp = new Timestamp(DateTimeUtil.julianTimeToJavaTime(DateTimeUtil.toJulianTimestamp(tm)));
      parameters.add(new Pair(Type.TIMESTAMP, timestamp));
    } else {
      sb.append("\"").append(expr.toString()).append("\"");
    }
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitTimeLiteral(Object ctx, Stack<Expr> stack, TimeLiteral expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      TimeValue timeValue = expr.getTime();

      int [] times = ExprAnnotator.timeToIntArray(timeValue.getHours(),
        timeValue.getMinutes(),
        timeValue.getSeconds(),
        timeValue.getSecondsFraction());

      long time;
      if (timeValue.hasSecondsFraction()) {
        time = DateTimeUtil.toTime(times[0], times[1], times[2], times[3] * 1000);
      } else {
        time = DateTimeUtil.toTime(times[0], times[1], times[2], 0);
      }
      TimeDatum timeDatum = new TimeDatum(time);
      TimeMeta tm = timeDatum.asTimeMeta();

      TimeZone tz = TimeZone.getDefault();
      DateTimeUtil.toUTCTimezone(tm, tz);

      sb.append("?").append(" )");
      parameters.add(new Pair(Type.TIME, new Time(DateTimeUtil.toJavaTime(tm.hours, tm.minutes, tm.secs, tm.fsecs))));
    } else {
      sb.append("\"").append(expr.toString()).append("\"");
    }
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitLiteral(Object ctx, Stack<Expr> stack, LiteralValue expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      sb.append("?").append(" )");
      switch (expr.getValueType()) {
        case Boolean:
          parameters.add(new Pair(Type.BOOLEAN, Boolean.valueOf(expr.getValue())));
          break;
        case Unsigned_Float:
          parameters.add(new Pair(Type.FLOAT8, Double.valueOf(expr.getValue())));
          break;
        case String:
          parameters.add(new Pair(Type.TEXT, expr.getValue()));
          break;
        default:
          parameters.add(new Pair(Type.INT8, Long.valueOf(expr.getValue())));
          break;
      }
    } else {
      switch (expr.getValueType()) {
        case String:
          sb.append("\"").append(expr.getValue()).append("\"");
          break;
        default:
          sb.append(expr.getValue());
          break;
      }
    }
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitValueListExpr(Object ctx, Stack<Expr> stack, ValueListExpr expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      sb.append("(");
      for(int i = 0; i < expr.getValues().length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append("?");
        stack.push(expr.getValues()[i]);
        visit(ctx, stack, expr.getValues()[i]);
        stack.pop();
      }
      sb.append(")");
      sb.append(" )");
      queries.push(sb.toString());
    } else {
      throw new UnsupportedException("IN Operator");
    }

    return expr;
  }

  @Override
  public Expr visitColumnReference(Object ctx, Stack<Expr> stack, ColumnReferenceExpr expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      sb.append("( ").append(tableAlias).append(".").append(CatalogConstants.COL_COLUMN_NAME)
        .append(" = '").append(expr.getName()).append("'")
        .append(" AND ").append(tableAlias).append(".").append(CatalogConstants.COL_PARTITION_VALUE);
    } else {
      sb.append(expr.getName());
    }
    queries.push(sb.toString());
    return expr;
  }


  @Override
  public Expr visitUnaryOperator(Object ctx, Stack<Expr> stack, UnaryOperator expr) throws TajoException {
    stack.push(expr);
    Expr child = visit(ctx, stack, expr.getChild());
    stack.pop();

    if (child.getType() == OpType.Literal) {
      return new NullLiteral();
    }

    String childSql = queries.pop();

    StringBuilder sb = new StringBuilder();
    if (expr.getType() == OpType.IsNullPredicate) {
      IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
      sb.append(childSql);
      sb.append(" IS ");
      if (isNullPredicate.isNot()) {
        sb.append("NOT NULL");
      } else {
        sb.append("NULL");
      }
    }

    if (!isHiveCatalog) {
      sb.append(" )");
    }
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitBetween(Object ctx, Stack<Expr> stack, BetweenPredicate expr) throws TajoException {
    stack.push(expr);

    visit(ctx, stack, expr.predicand());
    String predicandSql = queries.pop();

    visit(ctx, stack, expr.begin());
    String beginSql= queries.pop();
    if (!isHiveCatalog && beginSql.endsWith(")")) {
      beginSql = beginSql.substring(0, beginSql.length()-1);
    }

    visit(ctx, stack, expr.end());
    String endSql = queries.pop();
    if (!isHiveCatalog && endSql.endsWith(")")) {
      endSql = beginSql.substring(0, endSql.length()-1);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(predicandSql);
    sb.append(" BETWEEN ");
    sb.append(beginSql);
    sb.append(" AND ");
    sb.append(endSql);

    if (!isHiveCatalog) {
      sb.append(")");
    }

    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitCaseWhen(Object ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws TajoException {
    stack.push(expr);

    StringBuilder sb = new StringBuilder();
    sb.append("CASE ");

    String condition, result;

    for (CaseWhenPredicate.WhenExpr when : expr.getWhens()) {
      visit(ctx, stack, when.getCondition());
      condition = queries.pop();
      visit(ctx, stack, when.getResult());
      result = queries.pop();

      String whenSql = condition + " " + result;
      if (!isHiveCatalog && whenSql.endsWith(")")) {
        whenSql = whenSql.substring(0, whenSql.length()-1);
      }

      sb.append(whenSql).append(" ");
    }

    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
      String elseSql = queries.pop();
      if (!isHiveCatalog && elseSql.endsWith(")")) {
        elseSql = elseSql.substring(0, elseSql.length()-1);
      }

      sb.append("ELSE ").append(elseSql).append(" END");
    }

    if (!isHiveCatalog) {
      sb.append(")");
    }

    queries.push(sb.toString());

    stack.pop();
    return expr;
  }

  @Override
  public Expr visitBinaryOperator(Object ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException {
    stack.push(expr);
    Expr lhs = visit(ctx, stack, expr.getLeft());
    String leftSql = queries.pop();
    Expr rhs = visit(ctx, stack, expr.getRight());
    String rightSql = queries.pop();
    stack.pop();

    if (!expr.getLeft().equals(lhs)) {
      expr.setLeft(lhs);
    }
    if (!expr.getRight().equals(rhs)) {
      expr.setRight(rhs);
    }

    if (lhs.getType() == OpType.Literal && rhs.getType() == OpType.Literal) {
      return new NullLiteral();
    }

    StringBuilder sb = new StringBuilder();
    sb.append(leftSql);
    sb.append(" ").append(getOperator(expr.getType())).append(" ");
    sb.append(rightSql);
    queries.push(sb.toString());

    return expr;
  }

  private String getOperator(OpType type) {
    String operator;
    switch (type) {
      case Not:
        operator = "!";
        break;
      case And:
        operator = "AND";
        break;
      case Or:
        operator = "OR";
        break;
      case Equals:
        operator = "=";
        break;
      case IsNullPredicate:
        operator = "IS NULL";
        break;
      case NotEquals:
        operator = "<>";
        break;
      case LessThan:
        operator = "<";
        break;
      case LessThanOrEquals:
        operator = "<=";
        break;
      case GreaterThan:
        operator = ">";
        break;
      case GreaterThanOrEquals:
        operator = ">=";
        break;
      case Plus:
        operator = "+";
        break;
      case Minus:
        operator = "-";
        break;
      case Modular:
        operator = "%";
        break;
      case Multiply:
        operator = "*";
        break;
      case Divide:
        operator = "/";
        break;
      case LikePredicate:
        operator = "LIKE";
        break;
      case SimilarToPredicate:
        operator = "([.])";
        break;
      case InPredicate:
        operator = "IN";
        break;
      case Asterisk:
        operator = "*";
        break;
      //TODO: need to check more types.
      default:
        operator = type.name();
        break;
    }

    return operator;
  }

  @Override
  public Expr visitLikePredicate(Object ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException {
    stack.push(expr);

    visit(ctx, stack, expr.getPredicand());
    String predicand = queries.pop();
    visit(ctx, stack, expr.getPattern());
    String pattern = queries.pop();
    stack.pop();

    if(isHiveCatalog) {
      if (pattern.startsWith("%") || pattern.endsWith("%")) {
        throw new UnsupportedException("LIKE Operator with '%'");
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append(predicand);

    if (expr.isNot()) {
      sb.append(" NOT ");
    }

    if (expr.isCaseInsensitive()) {
      sb.append(" ILIKE ");
    } else {
      sb.append(" LIKE ");
    }


    sb.append(pattern);
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitSimilarToPredicate(Object ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException {
    if (isHiveCatalog) {
      throw new UnsupportedException("SIMILAR TO Operator");
    }

    stack.push(expr);

    visit(ctx, stack, expr.getPredicand());
    String predicand = queries.pop();
    visit(ctx, stack, expr.getPattern());
    String pattern = queries.pop();
    stack.pop();

    StringBuilder sb = new StringBuilder();
    sb.append(predicand);

    if (expr.isNot()) {
      sb.append(" NOT ");
    }

    sb.append(" SIMILAR TO ");

    sb.append(pattern);
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitRegexpPredicate(Object ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException {
    if (isHiveCatalog) {
      throw new UnsupportedException("REGEXP Operator");
    }

    stack.push(expr);

    visit(ctx, stack, expr.getPredicand());
    String predicand = queries.pop();
    visit(ctx, stack, expr.getPattern());
    String pattern = queries.pop();
    stack.pop();

    StringBuilder sb = new StringBuilder();
    sb.append(predicand);

    if (expr.isNot()) {
      sb.append(" NOT ");
    }
    sb.append(" REGEXP ");

    sb.append(pattern);
    queries.push(sb.toString());

    return expr;
  }

}