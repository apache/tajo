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

public interface AlgebraVisitor<CONTEXT, RESULT> {
  // Relational Operators
  RESULT visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws PlanningException;
  RESULT visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws PlanningException;
  RESULT visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws PlanningException;
  RESULT visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws PlanningException;
  RESULT visitGroupBy(CONTEXT ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException;
  RESULT visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws PlanningException;
  RESULT visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws PlanningException;
  RESULT visitUnion(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException;
  RESULT visitExcept(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException;
  RESULT visitIntersect(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException;
  RESULT visitSimpleTableSubQuery(CONTEXT ctx, Stack<Expr> stack, SimpleTableSubQuery expr) throws PlanningException;
  RESULT visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr) throws PlanningException;
  RESULT visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException;
  RESULT visitRelation(CONTEXT ctx, Stack<Expr> stack, Relation expr) throws PlanningException;
  RESULT visitScalarSubQuery(CONTEXT ctx, Stack<Expr> stack, ScalarSubQuery expr) throws PlanningException;
  RESULT visitExplain(CONTEXT ctx, Stack<Expr> stack, Explain expr) throws PlanningException;

  // Data definition language
  RESULT visitCreateDatabase(CONTEXT ctx, Stack<Expr> stack, CreateDatabase expr) throws PlanningException;
  RESULT visitDropDatabase(CONTEXT ctx, Stack<Expr> stack, DropDatabase expr) throws PlanningException;
  RESULT visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException;
  RESULT visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException;
  RESULT visitAlterTablespace(CONTEXT ctx, Stack<Expr> stack, AlterTablespace expr) throws PlanningException;
  RESULT visitAlterTable(CONTEXT ctx, Stack<Expr> stack, AlterTable expr) throws PlanningException;
  RESULT visitTruncateTable(CONTEXT ctx, Stack<Expr> stack, TruncateTable expr) throws PlanningException;

    // Insert or Update
  RESULT visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException;

  // Logical operators
  RESULT visitAnd(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitOr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitNot(CONTEXT ctx, Stack<Expr> stack, NotExpr expr) throws PlanningException;

  // comparison predicates
  RESULT visitEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitNotEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitLessThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitLessThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitGreaterThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitGreaterThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;

  // Other Predicates
  RESULT visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException;
  RESULT visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException;
  RESULT visitIsNullPredicate(CONTEXT ctx, Stack<Expr> stack, IsNullPredicate expr) throws PlanningException;
  RESULT visitInPredicate(CONTEXT ctx, Stack<Expr> stack, InPredicate expr) throws PlanningException;
  RESULT visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException;
  RESULT visitExistsPredicate(CONTEXT ctx, Stack<Expr> stack, ExistsPredicate expr) throws PlanningException;

  // String Operator or Pattern Matching Predicates
  RESULT visitLikePredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitSimilarToPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitRegexpPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitConcatenate(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;

  // arithmetic operators
  RESULT visitPlus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitMinus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitMultiply(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitDivide(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitModular(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;

  // other expressions
  RESULT visitSign(CONTEXT ctx, Stack<Expr> stack, SignedExpr expr) throws PlanningException;
  RESULT visitColumnReference(CONTEXT ctx, Stack<Expr> stack, ColumnReferenceExpr expr) throws PlanningException;
  RESULT visitTargetExpr(CONTEXT ctx, Stack<Expr> stack, NamedExpr expr) throws PlanningException;
  RESULT visitQualifiedAsterisk(CONTEXT ctx, Stack<Expr> stack, QualifiedAsteriskExpr expr) throws PlanningException;

  // functions
  RESULT visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException;
  RESULT visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException;
  RESULT visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr) throws PlanningException;
  RESULT visitWindowFunction(CONTEXT ctx, Stack<Expr> stack, WindowFunctionExpr expr) throws PlanningException;

  // Literal
  RESULT visitCastExpr(CONTEXT ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException;

  RESULT visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException;
  RESULT visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException;
  RESULT visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException;
  RESULT visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws PlanningException;
  RESULT visitIntervalLiteral(CONTEXT ctx, Stack<Expr> stack, IntervalLiteral expr) throws PlanningException;
  RESULT visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException;
  RESULT visitDateLiteral(CONTEXT ctx, Stack<Expr> stack, DateLiteral expr) throws PlanningException;
}
