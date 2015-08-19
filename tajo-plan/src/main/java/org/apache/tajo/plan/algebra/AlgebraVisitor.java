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

package org.apache.tajo.plan.algebra;

import org.apache.tajo.algebra.*;
import org.apache.tajo.exception.TajoException;

import java.util.Stack;

public interface AlgebraVisitor<CONTEXT, RESULT> {
  // Relational Operators
  RESULT visitSetSession(CONTEXT ctx, Stack<Expr> stack, SetSession expr) throws TajoException;

  // Relational Operators
  RESULT visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws TajoException;
  RESULT visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws TajoException;
  RESULT visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws TajoException;
  RESULT visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws TajoException;
  RESULT visitGroupBy(CONTEXT ctx, Stack<Expr> stack, Aggregation expr) throws TajoException;
  RESULT visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws TajoException;
  RESULT visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws TajoException;
  RESULT visitUnion(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws TajoException;
  RESULT visitExcept(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws TajoException;
  RESULT visitIntersect(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws TajoException;
  RESULT visitSimpleTableSubquery(CONTEXT ctx, Stack<Expr> stack, SimpleTableSubquery expr) throws TajoException;
  RESULT visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr) throws TajoException;
  RESULT visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws TajoException;
  RESULT visitRelation(CONTEXT ctx, Stack<Expr> stack, Relation expr) throws TajoException;
  RESULT visitScalarSubQuery(CONTEXT ctx, Stack<Expr> stack, ScalarSubQuery expr) throws TajoException;
  RESULT visitExplain(CONTEXT ctx, Stack<Expr> stack, Explain expr) throws TajoException;

  // Data definition language
  RESULT visitCreateDatabase(CONTEXT ctx, Stack<Expr> stack, CreateDatabase expr) throws TajoException;
  RESULT visitDropDatabase(CONTEXT ctx, Stack<Expr> stack, DropDatabase expr) throws TajoException;
  RESULT visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws TajoException;
  RESULT visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws TajoException;
  RESULT visitAlterTablespace(CONTEXT ctx, Stack<Expr> stack, AlterTablespace expr) throws TajoException;
  RESULT visitAlterTable(CONTEXT ctx, Stack<Expr> stack, AlterTable expr) throws TajoException;
  RESULT visitTruncateTable(CONTEXT ctx, Stack<Expr> stack, TruncateTable expr) throws TajoException;
  RESULT visitCreateIndex(CONTEXT ctx, Stack<Expr> stack, CreateIndex expr) throws TajoException;
  RESULT visitDropIndex(CONTEXT ctx, Stack<Expr> stack, DropIndex expr) throws TajoException;

    // Insert or Update
  RESULT visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws TajoException;

  // Logical operators
  RESULT visitAnd(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitOr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitNot(CONTEXT ctx, Stack<Expr> stack, NotExpr expr) throws TajoException;

  // comparison predicates
  RESULT visitEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitNotEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitLessThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitLessThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitGreaterThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitGreaterThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;

  // Other Predicates
  RESULT visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws TajoException;
  RESULT visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws TajoException;
  RESULT visitIsNullPredicate(CONTEXT ctx, Stack<Expr> stack, IsNullPredicate expr) throws TajoException;
  RESULT visitInPredicate(CONTEXT ctx, Stack<Expr> stack, InPredicate expr) throws TajoException;
  RESULT visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws TajoException;
  RESULT visitExistsPredicate(CONTEXT ctx, Stack<Expr> stack, ExistsPredicate expr) throws TajoException;

  // String Operator or Pattern Matching Predicates
  RESULT visitLikePredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException;
  RESULT visitSimilarToPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException;
  RESULT visitRegexpPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws TajoException;
  RESULT visitConcatenate(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;

  // arithmetic operators
  RESULT visitPlus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitMinus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitMultiply(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitDivide(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;
  RESULT visitModular(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws TajoException;

  // other expressions
  RESULT visitSign(CONTEXT ctx, Stack<Expr> stack, SignedExpr expr) throws TajoException;
  RESULT visitColumnReference(CONTEXT ctx, Stack<Expr> stack, ColumnReferenceExpr expr) throws TajoException;
  RESULT visitTargetExpr(CONTEXT ctx, Stack<Expr> stack, NamedExpr expr) throws TajoException;
  RESULT visitQualifiedAsterisk(CONTEXT ctx, Stack<Expr> stack, QualifiedAsteriskExpr expr) throws TajoException;

  // functions
  RESULT visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws TajoException;
  RESULT visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws TajoException;
  RESULT visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr) throws TajoException;
  RESULT visitWindowFunction(CONTEXT ctx, Stack<Expr> stack, WindowFunctionExpr expr) throws TajoException;

  // Literal
  RESULT visitCastExpr(CONTEXT ctx, Stack<Expr> stack, CastExpr expr) throws TajoException;

  RESULT visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws TajoException;
  RESULT visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws TajoException;
  RESULT visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws TajoException;
  RESULT visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws TajoException;
  RESULT visitIntervalLiteral(CONTEXT ctx, Stack<Expr> stack, IntervalLiteral expr) throws TajoException;
  RESULT visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws TajoException;
  RESULT visitDateLiteral(CONTEXT ctx, Stack<Expr> stack, DateLiteral expr) throws TajoException;
}
