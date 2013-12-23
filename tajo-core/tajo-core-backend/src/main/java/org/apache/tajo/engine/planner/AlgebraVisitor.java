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
  RESULT visitProjection(CONTEXT ctx, Stack<OpType> stack, Projection expr) throws PlanningException;
  RESULT visitLimit(CONTEXT ctx, Stack<OpType> stack, Limit expr) throws PlanningException;
  RESULT visitSort(CONTEXT ctx, Stack<OpType> stack, Sort expr) throws PlanningException;
  RESULT visitHaving(CONTEXT ctx, Stack<OpType> stack, Having expr) throws PlanningException;
  RESULT visitGroupBy(CONTEXT ctx, Stack<OpType> stack, Aggregation expr) throws PlanningException;
  RESULT visitJoin(CONTEXT ctx, Stack<OpType> stack, Join expr) throws PlanningException;
  RESULT visitFilter(CONTEXT ctx, Stack<OpType> stack, Selection expr) throws PlanningException;
  RESULT visitUnion(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitExcept(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitIntersect(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException;
  RESULT visitSimpleTableSubQuery(CONTEXT ctx, Stack<OpType> stack, SimpleTableSubQuery expr) throws PlanningException;
  RESULT visitTableSubQuery(CONTEXT ctx, Stack<OpType> stack, TablePrimarySubQuery expr) throws PlanningException;
  RESULT visitRelationList(CONTEXT ctx, Stack<OpType> stack, RelationList expr) throws PlanningException;
  RESULT visitRelation(CONTEXT ctx, Stack<OpType> stack, Relation expr) throws PlanningException;
  RESULT visitScalarSubQuery(CONTEXT ctx, Stack<OpType> stack, ScalarSubQuery expr) throws PlanningException;

  // Data definition language
  RESULT visitCreateTable(CONTEXT ctx, Stack<OpType> stack, CreateTable expr) throws PlanningException;
  RESULT visitDropTable(CONTEXT ctx, Stack<OpType> stack, DropTable expr) throws PlanningException;

  // Insert or Update
  RESULT visitInsert(CONTEXT ctx, Stack<OpType> stack, Insert expr) throws PlanningException;

  // Logical operators
  RESULT visitAnd(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitOr(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitNot(CONTEXT ctx, Stack<OpType> stack, NotExpr expr) throws PlanningException;

  // comparison predicates
  RESULT visitEquals(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitNotEquals(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitLessThan(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitLessThanOrEquals(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitGreaterThan(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitGreaterThanOrEquals(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;

  // Other Predicates
  RESULT visitBetween(CONTEXT ctx, Stack<OpType> stack, BetweenPredicate expr) throws PlanningException;
  RESULT visitCaseWhen(CONTEXT ctx, Stack<OpType> stack, CaseWhenPredicate expr) throws PlanningException;
  RESULT visitIsNullPredicate(CONTEXT ctx, Stack<OpType> stack, IsNullPredicate expr) throws PlanningException;
  RESULT visitInPredicate(CONTEXT ctx, Stack<OpType> stack, InPredicate expr) throws PlanningException;
  RESULT visitValueListExpr(CONTEXT ctx, Stack<OpType> stack, ValueListExpr expr) throws PlanningException;
  RESULT visitExistsPredicate(CONTEXT ctx, Stack<OpType> stack, ExistsPredicate expr) throws PlanningException;

  // String Operator or Pattern Matching Predicates
  RESULT visitLikePredicate(CONTEXT ctx, Stack<OpType> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitSimilarToPredicate(CONTEXT ctx, Stack<OpType> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitRegexpPredicate(CONTEXT ctx, Stack<OpType> stack, PatternMatchPredicate expr) throws PlanningException;
  RESULT visitConcatenate(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;

  // arithmetic operators
  RESULT visitPlus(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitMinus(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitMultiply(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitDivide(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;
  RESULT visitModular(CONTEXT ctx, Stack<OpType> stack, BinaryOperator expr) throws PlanningException;

  // other expressions
  RESULT visitSign(CONTEXT ctx, Stack<OpType> stack, SignedExpr expr) throws PlanningException;
  RESULT visitColumnReference(CONTEXT ctx, Stack<OpType> stack, ColumnReferenceExpr expr) throws PlanningException;
  RESULT visitTargetExpr(CONTEXT ctx, Stack<OpType> stack, TargetExpr expr) throws PlanningException;
  RESULT visitFunction(CONTEXT ctx, Stack<OpType> stack, FunctionExpr expr) throws PlanningException;

  // set functions
  RESULT visitCountRowsFunction(CONTEXT ctx, Stack<OpType> stack, CountRowsFunctionExpr expr) throws PlanningException;
  RESULT visitGeneralSetFunction(CONTEXT ctx, Stack<OpType> stack, GeneralSetFunctionExpr expr)
      throws PlanningException;

  // Literal
  RESULT visitCastExpr(CONTEXT ctx, Stack<OpType> stack, CastExpr expr) throws PlanningException;

  RESULT visitDataType(CONTEXT ctx, Stack<OpType> stack, DataTypeExpr expr) throws PlanningException;
  RESULT visitLiteral(CONTEXT ctx, Stack<OpType> stack, LiteralValue expr) throws PlanningException;
  RESULT visitNullLiteral(CONTEXT ctx, Stack<OpType> stack, NullLiteral expr) throws PlanningException;
  RESULT visitTimestampLiteral(CONTEXT ctx, Stack<OpType> stack, TimestampLiteral expr) throws PlanningException;

}
