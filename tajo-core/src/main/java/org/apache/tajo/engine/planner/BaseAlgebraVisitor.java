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

public class BaseAlgebraVisitor<CONTEXT, RESULT> implements AlgebraVisitor<CONTEXT, RESULT> {

  /**
   * The prehook is called before each expression is visited.
   */
  public void preHook(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
  }


  /**
   * The posthook is called before each expression is visited.
   */
  public RESULT postHook(CONTEXT ctx, Stack<Expr> stack, Expr expr, RESULT current) throws PlanningException {
    return current;
  }

  /**
   * visit visits each relational operator expression recursively.
   *
   * @param stack The stack contains the upper operators' type.
   * @param expr The visiting relational operator
   */
  public RESULT visit(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    preHook(ctx, stack, expr);

    RESULT current;

    switch (expr.getType()) {

    case Projection:
      current = visitProjection(ctx, stack, (Projection) expr);
      break;
    case Limit:
      current = visitLimit(ctx, stack, (Limit) expr);
      break;
    case Sort:
      current = visitSort(ctx, stack, (Sort) expr);
      break;
    case Having:
      current = visitHaving(ctx, stack, (Having) expr);
      break;
    case Aggregation:
      current = visitGroupBy(ctx, stack, (Aggregation) expr);
      break;
    case Join:
      current = visitJoin(ctx, stack, (Join) expr);
      break;
    case Filter:
      current = visitFilter(ctx, stack, (Selection) expr);
      break;
    case Union:
      current = visitUnion(ctx, stack, (SetOperation) expr);
      break;
    case Except:
      current = visitExcept(ctx, stack, (SetOperation) expr);
      break;
    case Intersect:
      current = visitIntersect(ctx, stack, (SetOperation) expr);
      break;
    case SimpleTableSubQuery:
      current = visitSimpleTableSubQuery(ctx, stack, (SimpleTableSubQuery) expr);
      break;
    case TablePrimaryTableSubQuery:
      current = visitTableSubQuery(ctx, stack, (TablePrimarySubQuery) expr);
      break;
    case RelationList:
      current = visitRelationList(ctx, stack, (RelationList) expr);
      break;
    case Relation:
      current = visitRelation(ctx, stack, (Relation) expr);
      break;
    case ScalarSubQuery:
      current = visitScalarSubQuery(ctx, stack, (ScalarSubQuery) expr);
      break;
    case Explain:
      current = visitExplain(ctx, stack, (Explain) expr);
      break;

    case CreateDatabase:
      current = visitCreateDatabase(ctx, stack, (CreateDatabase) expr);
      break;
    case DropDatabase:
      current = visitDropDatabase(ctx, stack, (DropDatabase) expr);
      break;
    case CreateTable:
      current = visitCreateTable(ctx, stack, (CreateTable) expr);
      break;
    case DropTable:
      current = visitDropTable(ctx, stack, (DropTable) expr);
      break;
    case AlterTablespace:
      current = visitAlterTablespace(ctx, stack, (AlterTablespace) expr);
      break;
    case AlterTable:
      current = visitAlterTable(ctx, stack, (AlterTable) expr);
      break;
    case TruncateTable:
      current = visitTruncateTable(ctx, stack, (TruncateTable)expr);
      break;

    case Insert:
      current = visitInsert(ctx, stack, (Insert) expr);
      break;

    case And:
      current = visitAnd(ctx, stack, (BinaryOperator) expr);
      break;
    case Or:
      current = visitOr(ctx, stack, (BinaryOperator) expr);
      break;
    case Not:
      current = visitNot(ctx, stack, (NotExpr) expr);
      break;

    case Equals:
      current = visitEquals(ctx, stack, (BinaryOperator) expr);
      break;
    case NotEquals:
      current = visitNotEquals(ctx, stack, (BinaryOperator) expr);
      break;
    case LessThan:
      current = visitLessThan(ctx, stack, (BinaryOperator) expr);
      break;
    case LessThanOrEquals:
      current = visitLessThanOrEquals(ctx, stack, (BinaryOperator) expr);
      break;
    case GreaterThan:
      current = visitGreaterThan(ctx, stack, (BinaryOperator) expr);
      break;
    case GreaterThanOrEquals:
      current = visitGreaterThanOrEquals(ctx, stack, (BinaryOperator) expr);
      break;

    // Other Predicates
    case Between:
      current = visitBetween(ctx, stack, (BetweenPredicate) expr);
      break;
    case CaseWhen:
      current = visitCaseWhen(ctx, stack, (CaseWhenPredicate) expr);
      break;
    case IsNullPredicate:
      current = visitIsNullPredicate(ctx, stack, (IsNullPredicate) expr);
      break;
    case InPredicate:
      current = visitInPredicate(ctx, stack, (InPredicate) expr);
      break;
    case ValueList:
      current = visitValueListExpr(ctx, stack, (ValueListExpr) expr);
      break;
    case ExistsPredicate:
      current = visitExistsPredicate(ctx, stack, (ExistsPredicate) expr);
      break;

    // String Operator or Pattern Matching Predicates
    case LikePredicate:
      current = visitLikePredicate(ctx, stack, (PatternMatchPredicate) expr);
      break;
    case SimilarToPredicate:
      current = visitSimilarToPredicate(ctx, stack, (PatternMatchPredicate) expr);
      break;
    case Regexp:
      current = visitRegexpPredicate(ctx, stack, (PatternMatchPredicate) expr);
      break;
    case Concatenate:
      current = visitConcatenate(ctx, stack, (BinaryOperator) expr);
      break;

    // Arithmetic Operators
    case Plus:
      current = visitPlus(ctx, stack, (BinaryOperator) expr);
      break;
    case Minus:
      current = visitMinus(ctx, stack, (BinaryOperator) expr);
      break;
    case Multiply:
      current = visitMultiply(ctx, stack, (BinaryOperator) expr);
      break;
    case Divide:
      current = visitDivide(ctx, stack, (BinaryOperator) expr);
      break;
    case Modular:
      current = visitModular(ctx, stack, (BinaryOperator) expr);
      break;

    // Other Expressions
    case Sign:
      current = visitSign(ctx, stack, (SignedExpr) expr);
      break;
    case Column:
      current = visitColumnReference(ctx, stack, (ColumnReferenceExpr) expr);
      break;
    case Target:
      current = visitTargetExpr(ctx, stack, (NamedExpr) expr);
      break;
    case Function:
      current = visitFunction(ctx, stack, (FunctionExpr) expr);
      break;
    case Asterisk:
      current = visitQualifiedAsterisk(ctx, stack, (QualifiedAsteriskExpr) expr);
      break;


    case CountRowsFunction:
      current = visitCountRowsFunction(ctx, stack, (CountRowsFunctionExpr) expr);
      break;
    case GeneralSetFunction:
      current = visitGeneralSetFunction(ctx, stack, (GeneralSetFunctionExpr) expr);
      break;
    case WindowFunction:
      current = visitWindowFunction(ctx, stack, (WindowFunctionExpr) expr);
      break;


    case DataType:
      current = visitDataType(ctx, stack, (DataTypeExpr) expr);
      break;
    case Cast:
      current = visitCastExpr(ctx, stack, (CastExpr) expr);
      break;
    case Literal:
      current = visitLiteral(ctx, stack, (LiteralValue) expr);
      break;
    case NullLiteral:
      current = visitNullLiteral(ctx, stack, (NullLiteral) expr);
      break;
    case DateLiteral:
      current = visitDateLiteral(ctx, stack, (DateLiteral) expr);
      break;
    case TimeLiteral:
      current = visitTimeLiteral(ctx, stack, (TimeLiteral) expr);
      break;
    case TimestampLiteral:
      current = visitTimestampLiteral(ctx, stack, (TimestampLiteral) expr);
      break;
    case IntervalLiteral:
      current = visitIntervalLiteral(ctx, stack, (IntervalLiteral) expr);
      break;


    default:
      throw new PlanningException("Cannot support this type algebra \"" + expr.getType() + "\"");
    }

    // skip postHook against only one relation
    if (expr.getType() == OpType.RelationList) {
      RelationList relationList = (RelationList)expr;
      if (relationList.size() == 1 && relationList.getRelations()[0].getType() == OpType.Relation) {
        return current;
      }
    }

    postHook(ctx, stack, expr, current);
    return current;
  }

  private RESULT visitDefaultUnaryExpr(CONTEXT ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    stack.push(expr);
    RESULT child = visit(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  private RESULT visitDefaultBinaryExpr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    stack.push(expr);
    RESULT child = visit(ctx, stack, expr.getLeft());
    visit(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    stack.push(expr);
    try {
      for (NamedExpr target : expr.getNamedExprs()) {
        visit(ctx, stack, target);
      }
      if (expr.hasChild()) {
        return visit(ctx, stack, expr.getChild());
      }
    } finally {
      stack.pop();
    }
    return null;
  }

  @Override
  public RESULT visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getFetchFirstNum());
    RESULT result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    for (Sort.SortSpec sortSpec : expr.getSortSpecs()) {
      visit(ctx, stack, sortSpec.getKey());
    }
    RESULT result = visit(ctx, stack, expr.getChild());
    return result;
  }

  @Override
  public RESULT visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    RESULT result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitGroupBy(CONTEXT ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    stack.push(expr);

    for (org.apache.tajo.algebra.Aggregation.GroupElement groupElement : expr.getGroupSet()) {
      for (Expr groupingSet : groupElement.getGroupingSets()) {
        visit(ctx, stack, groupingSet);
      }
    }

    RESULT result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    visit(ctx, stack, expr.getLeft());
    RESULT result = visit(ctx, stack, expr.getRight());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    RESULT result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitUnion(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitExcept(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitIntersect(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitSimpleTableSubQuery(CONTEXT ctx, Stack<Expr> stack, SimpleTableSubQuery expr)
      throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {
    stack.push(expr);
    RESULT child = visit(ctx, stack, expr.getSubQuery());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException {
    stack.push(expr);
    RESULT child = null;
    for (Expr e : expr.getRelations()) {
      child = visit(ctx, stack, e);
    }
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitRelation(CONTEXT ctx, Stack<Expr> stack, Relation expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitScalarSubQuery(CONTEXT ctx, Stack<Expr> stack, ScalarSubQuery expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitExplain(CONTEXT ctx, Stack<Expr> stack, Explain expr) throws PlanningException {
    stack.push(expr);
    RESULT child = visit(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitCreateDatabase(CONTEXT ctx, Stack<Expr> stack, CreateDatabase expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitDropDatabase(CONTEXT ctx, Stack<Expr> stack, DropDatabase expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    stack.push(expr);
    RESULT child = null;
    if (expr.hasSubQuery()) {
      child = visit(ctx, stack, expr.getSubQuery());
    }
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitAlterTablespace(CONTEXT ctx, Stack<Expr> stack, AlterTablespace expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitAlterTable(CONTEXT ctx, Stack<Expr> stack, AlterTable expr) throws PlanningException {
    return null;
  }

  public RESULT visitTruncateTable(CONTEXT ctx, Stack<Expr> stack, TruncateTable expr) throws PlanningException {
    return null;
  }
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public RESULT visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    stack.push(expr);
    RESULT child = visit(ctx, stack, expr.getSubQuery());
    stack.pop();
    return child;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Logical Operator Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitAnd(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitOr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitNot(CONTEXT ctx, Stack<Expr> stack, NotExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Comparison Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public RESULT visitEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitNotEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitLessThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitLessThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitGreaterThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitGreaterThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException {
    stack.push(expr);
    RESULT result = visit(ctx, stack, expr.predicand());
    visit(ctx, stack, expr.begin());
    visit(ctx, stack, expr.end());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException {
    stack.push(expr);
    RESULT result = null;
    for (CaseWhenPredicate.WhenExpr when : expr.getWhens()) {
      result = visit(ctx, stack, when.getCondition());
      visit(ctx, stack, when.getResult());
    }
    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIsNullPredicate(CONTEXT ctx, Stack<Expr> stack, IsNullPredicate expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitInPredicate(CONTEXT ctx, Stack<Expr> stack, InPredicate expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException {
    stack.push(expr);
    RESULT result = null;
    for (Expr value : expr.getValues()) {
      result = visit(ctx, stack, value);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitExistsPredicate(CONTEXT ctx, Stack<Expr> stack, ExistsPredicate expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // String Operator or Pattern Matching Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public RESULT visitLikePredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitSimilarToPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitRegexpPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitConcatenate(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Arithmetic Operators
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitPlus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitMinus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitMultiply(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitDivide(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitModular(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitSign(CONTEXT ctx, Stack<Expr> stack, SignedExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitColumnReference(CONTEXT ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitTargetExpr(CONTEXT ctx, Stack<Expr> stack, NamedExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public RESULT visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    stack.push(expr);
    RESULT result = null;
    if (expr.hasParams()) {
      for (Expr param : expr.getParams()) {
        result = visit(ctx, stack, param);
      }
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitQualifiedAsterisk(CONTEXT ctx, Stack<Expr> stack, QualifiedAsteriskExpr expr) throws PlanningException {
    return null;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    stack.push(expr);
    RESULT result = null;
    for (Expr param : expr.getParams()) {
      result = visit(ctx, stack, param);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitWindowFunction(CONTEXT ctx, Stack<Expr> stack, WindowFunctionExpr expr) throws PlanningException {
    stack.push(expr);
    RESULT result = null;
    for (Expr param : expr.getParams()) {
      result = visit(ctx, stack, param);
    }

    WindowSpec windowSpec = expr.getWindowSpec();

    if (windowSpec.hasPartitionBy()) {
      for (Expr partitionKey : windowSpec.getPartitionKeys()) {
        visit(ctx, stack, partitionKey);
      }
    }
    if (windowSpec.hasOrderBy()) {
      for (Sort.SortSpec sortKey : windowSpec.getSortSpecs()) {
        visit(ctx, stack, sortKey.getKey());
      }
    }
    if (windowSpec.hasWindowFrame()) {
      if (windowSpec.getWindowFrame().getStartBound().hasNumber()) {
        visit(ctx, stack, windowSpec.getWindowFrame().getStartBound().getNumber());
      }
      if (windowSpec.getWindowFrame().getEndBound().hasNumber()) {
        visit(ctx, stack, windowSpec.getWindowFrame().getEndBound().getNumber());
      }
    }

    stack.pop();
    return result;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public RESULT visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitCastExpr(CONTEXT ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    stack.push(expr);
    RESULT result = visit(ctx, stack, expr.getOperand());
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitIntervalLiteral(CONTEXT ctx, Stack<Expr> stack, IntervalLiteral expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitDateLiteral(CONTEXT ctx, Stack<Expr> stack, DateLiteral expr) throws PlanningException {
    return null;
  }
}
