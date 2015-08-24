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
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Stack;

public class PartitionFilterAlgebraVisitor extends SimpleAlgebraVisitor<Object, Expr> {

  private boolean isHiveCatalog = false;
  private boolean existRowCostant = false;
  private String tableAlias;
  private Column column;

  private Stack<String> queries = new Stack<String>();
  private List<String> parameters = TUtil.newList();

  public boolean isHiveCatalog() {
    return isHiveCatalog;
  }

  public void setIsHiveCatalog(boolean isHiveCatalog) {
    this.isHiveCatalog = isHiveCatalog;
  }

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

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }

  public void clearParameters() {
    this.parameters.clear();
  }

  public String getResult() {
    return queries.pop();
  }

  public boolean isExistRowCostant() {
    return existRowCostant;
  }

  public void setExistRowCostant(boolean existRowCostant) {
    this.existRowCostant = existRowCostant;
  }

  @Override
  public Expr visit(Object ctx, Stack<Expr> stack, Expr expr) throws TajoException {
    return super.visit(ctx, stack, expr);
  }

  @Override
  public Expr visitLiteral(Object ctx, Stack<Expr> stack, LiteralValue expr) throws TajoException {
    StringBuilder sb = new StringBuilder();

    if (!isHiveCatalog) {
      sb.append("?").append(" )");
      parameters.add(expr.getValue());
    } else {
      switch (column.getDataType().getType()) {
        case INT1:
        case INT2:
        case INT4:
        case INT8:
        case FLOAT4:
        case FLOAT8:
          sb.append(expr.getValue());
          break;
        default:
//           In hive, Filtering can be done only on string partition keys. for example "part1 = \"p1_abc\" and part2
//           <= "\p2_test\"".
          sb.append("\"").append(expr.getValue()).append("\"");
          break;
      }
    }
    queries.push(sb.toString());

    return expr;
  }

  @Override
  public Expr visitValueListExpr(Object ctx, Stack<Expr> stack, ValueListExpr expr) throws TajoException {
    existRowCostant = true;

    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for(int i = 0; i < expr.getValues().length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      if (!isHiveCatalog) {
        sb.append("?");
        stack.push(expr.getValues()[i]);
        visit(ctx, stack, expr.getValues()[i]);
        parameters.add(queries.pop());
        stack.pop();
      }
    }
    sb.append(")");

    if (!isHiveCatalog) {
      sb.append(" )");
    }
    queries.push(sb.toString());
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

}