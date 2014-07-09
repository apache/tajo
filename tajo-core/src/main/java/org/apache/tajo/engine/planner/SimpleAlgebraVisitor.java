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

package org.apache.tajo.engine.planner;

import org.apache.tajo.algebra.*;

import java.util.Stack;

/**
 * <code>SimpleAlgebraVisitor</code> provides a simple and fewer visit methods. It makes building concrete class easier.
 */
public abstract class SimpleAlgebraVisitor<CONTEXT, RESULT> extends BaseAlgebraVisitor<CONTEXT, RESULT> {

  public RESULT visit(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    RESULT result = null;
    if (expr instanceof UnaryOperator) {
      preHook(ctx, stack, expr);
      result = visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
      postHook(ctx, stack, expr, result);
    } else if (expr instanceof BinaryOperator) {
      preHook(ctx, stack, expr);
      result = visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
      postHook(ctx, stack, expr, result);
    } else {
      result = super.visit(ctx, stack, expr);
    }

    return result;
  }

  public RESULT visitUnaryOperator(CONTEXT ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    stack.push(expr);
    RESULT result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  public RESULT visitBinaryOperator(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getLeft());
    RESULT result = visit(ctx, stack, expr.getRight());
    stack.pop();
    return result;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Relational Operator Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    return super.visitProjection(ctx, stack, expr);
  }

  @Override
  public RESULT visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    return super.visitLimit(ctx, stack, expr);
  }

  @Override
  public RESULT visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    return super.visitSort(ctx, stack, expr);
  }

  @Override
  public RESULT visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    return super.visitHaving(ctx, stack, expr);
  }

  @Override
  public RESULT visitGroupBy(CONTEXT ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    return super.visitGroupBy(ctx, stack, expr);
  }

  public RESULT visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    return super.visitFilter(ctx, stack, expr);
  }

  @Override
  public RESULT visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    return super.visitJoin(ctx, stack, expr);
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr) throws PlanningException {
    return super.visitTableSubQuery(ctx, stack, expr);
  }

  @Override
  public RESULT visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException {
    return super.visitRelationList(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    return super.visitCreateTable(ctx, stack, expr);
  }

  @Override
  public RESULT visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException {
    return super.visitDropTable(ctx, stack, expr);
  }

  @Override
  public RESULT visitAlterTable(CONTEXT ctx, Stack<Expr> stack, AlterTable expr) throws PlanningException {
    return super.visitAlterTable(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public RESULT visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    return super.visitInsert(ctx, stack, expr);
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException {
    return super.visitBetween(ctx, stack, expr);
  }

  @Override
  public RESULT visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException {
    return super.visitCaseWhen(ctx, stack, expr);
  }

  @Override
  public RESULT visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException {
    return super.visitValueListExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Functions and General Set Function Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public RESULT visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    return super.visitFunction(ctx, stack, expr);
  }

  @Override
  public RESULT visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    return super.visitCountRowsFunction(ctx, stack, expr);
  }

  @Override
  public RESULT visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    return super.visitGeneralSetFunction(ctx, stack, expr);
  }

  @Override
  public RESULT visitWindowFunction(CONTEXT ctx, Stack<Expr> stack, WindowFunctionExpr expr) throws PlanningException {
    return super.visitWindowFunction(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException {
    return super.visitDataType(ctx, stack, expr);
  }

  @Override
  public RESULT visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException {
    return super.visitLiteral(ctx, stack, expr);
  }

  @Override
  public RESULT visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException {
    return super.visitNullLiteral(ctx, stack, expr);
  }

  @Override
  public RESULT visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws PlanningException {
    return super.visitTimestampLiteral(ctx, stack, expr);
  }

  @Override
  public RESULT visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException {
    return super.visitTimeLiteral(ctx, stack, expr);
  }
}
