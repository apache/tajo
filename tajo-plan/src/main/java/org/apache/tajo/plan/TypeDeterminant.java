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

package org.apache.tajo.plan;

import com.google.common.base.Preconditions;
import org.apache.tajo.DataTypeUtil;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;

import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type.BOOLEAN;
import static org.apache.tajo.common.TajoDataTypes.Type.NULL_TYPE;

public class TypeDeterminant extends SimpleAlgebraVisitor<LogicalPlanner.PlanContext, DataType> {
  private DataType BOOL_TYPE = CatalogUtil.newSimpleDataType(BOOLEAN);
  private CatalogService catalog;

  public TypeDeterminant(CatalogService catalog) {
    this.catalog = catalog;
  }

  public DataType determineDataType(LogicalPlanner.PlanContext ctx, Expr expr) throws PlanningException {
    return visit(ctx, new Stack<Expr>(), expr);
  }

  @Override
  public DataType visitUnaryOperator(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, UnaryOperator expr)
      throws PlanningException {
    stack.push(expr);
    DataType dataType = null;
    switch (expr.getType()) {
    case IsNullPredicate:
    case ExistsPredicate:
      dataType = BOOL_TYPE;
      break;
    case Cast:
      dataType = LogicalPlanner.convertDataType(((CastExpr)expr).getTarget()).getDataType();
      break;
    default:
      dataType = visit(ctx, stack, expr.getChild());
    }

    return dataType;
  }

  @Override
  public DataType visitBinaryOperator(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    stack.push(expr);
    DataType lhsType = visit(ctx, stack, expr.getLeft());
    DataType rhsType = visit(ctx, stack, expr.getRight());
    stack.pop();
    return computeBinaryType(expr.getType(), lhsType, rhsType);
  }

  public DataType computeBinaryType(OpType type, DataType lhsDataType, DataType rhsDataType) throws PlanningException {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(lhsDataType);
    Preconditions.checkNotNull(rhsDataType);

    if(OpType.isLogicalType(type) || OpType.isComparisonType(type)) {
      return BOOL_TYPE;
    } else if (OpType.isArithmeticType(type)) {
      return DataTypeUtil.determineType(lhsDataType, rhsDataType);
    } else if (type == OpType.Concatenate) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
    } else if (type == OpType.InPredicate) {
      return BOOL_TYPE;
    } else if (type == OpType.LikePredicate || type == OpType.SimilarToPredicate || type == OpType.Regexp) {
      return BOOL_TYPE;
    } else {
      throw new PlanningException(type.name() + "is not binary type");
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public DataType visitBetween(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, BetweenPredicate expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(BOOLEAN);
  }

  @Override
  public DataType visitCaseWhen(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CaseWhenPredicate caseWhen)
      throws PlanningException {
    DataType lastDataType = null;

    for (CaseWhenPredicate.WhenExpr when : caseWhen.getWhens()) {
      DataType resultType = visit(ctx, stack, when.getResult());
      if (lastDataType != null) {
        lastDataType = CatalogUtil.getWidestType(lastDataType, resultType);
      } else {
        lastDataType = resultType;
      }
    }

    if (caseWhen.hasElseResult()) {
      DataType elseResultType = visit(ctx, stack, caseWhen.getElseResult());
      lastDataType = CatalogUtil.getWidestType(lastDataType, elseResultType);
    }

    return lastDataType;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public DataType visitColumnReference(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    stack.push(expr);
    Column column = ctx.plan.resolveColumn(ctx.queryBlock, expr);
    stack.pop();
    return column.getDataType();
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public DataType visitFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, FunctionExpr expr)
      throws PlanningException {
    stack.push(expr); // <--- Push

    // Given parameters
    Expr[] params = expr.getParams();
    if (params == null) {
      params = new Expr[0];
    }

    DataType[] givenArgs = new DataType[params.length];
    DataType[] paramTypes = new DataType[params.length];

    for (int i = 0; i < params.length; i++) {
      givenArgs[i] = visit(ctx, stack, params[i]);
      paramTypes[i] = givenArgs[i];
    }

    stack.pop(); // <--- Pop

    if (!catalog.containFunction(expr.getSignature(), paramTypes)) {
      throw new NoSuchFunctionException(expr.getSignature(), paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(expr.getSignature(), paramTypes);
    return funcDesc.getReturnType();
  }

  @Override
  public DataType visitCountRowsFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    FunctionDesc countRows = catalog.getFunction("count", CatalogProtos.FunctionType.AGGREGATION,
        new DataType[] {});
    return countRows.getReturnType();
  }

  @Override
  public DataType visitGeneralSetFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack,
                                          GeneralSetFunctionExpr setFunction) throws PlanningException {
    stack.push(setFunction);

    Expr[] params = setFunction.getParams();
    DataType[] givenArgs = new DataType[params.length];
    DataType[] paramTypes = new DataType[params.length];

    CatalogProtos.FunctionType functionType = setFunction.isDistinct() ?
        CatalogProtos.FunctionType.DISTINCT_AGGREGATION : CatalogProtos.FunctionType.AGGREGATION;
    givenArgs[0] = visit(ctx, stack, params[0]);
    if (setFunction.getSignature().equalsIgnoreCase("count")) {
      paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.ANY);
    } else {
      paramTypes[0] = givenArgs[0];
    }

    stack.pop(); // <-- pop

    if (!catalog.containFunction(setFunction.getSignature(), functionType, paramTypes)) {
      throw new NoSuchFunctionException(setFunction.getSignature(), paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(setFunction.getSignature(), functionType, paramTypes);
    return funcDesc.getReturnType();
  }

  @Override
  public DataType visitWindowFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, WindowFunctionExpr windowFunc)
      throws PlanningException {
    stack.push(windowFunc); // <--- Push

    String funcName = windowFunc.getSignature();
    boolean distinct = windowFunc.isDistinct();
    Expr[] params = windowFunc.getParams();
    DataType[] givenArgs = new DataType[params.length];
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[params.length];
    CatalogProtos.FunctionType functionType;

    // Rewrite parameters if necessary
    if (params.length > 0) {
      givenArgs[0] = visit(ctx, stack, params[0]);

      if (windowFunc.getSignature().equalsIgnoreCase("count")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.ANY);
      } else if (windowFunc.getSignature().equalsIgnoreCase("row_number")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
      } else {
        paramTypes[0] = givenArgs[0];
      }
      for (int i = 1; i < params.length; i++) {
        givenArgs[i] = visit(ctx, stack, params[i]);
        paramTypes[i] = givenArgs[i];
      }
    }
    stack.pop(); // <--- Pop

    // TODO - containFunction and getFunction should support the function type mask which provides ORing multiple types.
    // the below checking against WINDOW_FUNCTIONS is a workaround code for the above problem.
    if (ExprAnnotator.WINDOW_FUNCTIONS.contains(funcName.toLowerCase())) {
      if (distinct) {
        throw new NoSuchFunctionException("row_number() does not support distinct keyword.");
      }
      functionType = CatalogProtos.FunctionType.WINDOW;
    } else {
      functionType = distinct ?
          CatalogProtos.FunctionType.DISTINCT_AGGREGATION : CatalogProtos.FunctionType.AGGREGATION;
    }

    if (!catalog.containFunction(windowFunc.getSignature(), functionType, paramTypes)) {
      throw new NoSuchFunctionException(funcName, paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(funcName, functionType, paramTypes);

    return funcDesc.getReturnType();
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public DataType visitDataType(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DataTypeExpr expr)
      throws PlanningException {
    return LogicalPlanner.convertDataType(expr).getDataType();
  }

  @Override
  public DataType visitLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, LiteralValue expr)
      throws PlanningException {
    // It must be the same to ExprAnnotator::visitLiteral.

    switch (expr.getValueType()) {
    case Boolean:
      return CatalogUtil.newSimpleDataType(BOOLEAN);
    case String:
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
    case Unsigned_Integer:
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
    case Unsigned_Large_Integer:
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
    case Unsigned_Float:
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
    default:
      throw new RuntimeException("Unsupported type: " + expr.getValueType());
    }
  }

  @Override
  public DataType visitNullLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, NullLiteral expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(NULL_TYPE);
  }

  @Override
  public DataType visitTimestampLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TimestampLiteral expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TIMESTAMP);
  }

  @Override
  public DataType visitTimeLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TimeLiteral expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TIME);
  }

  @Override
  public DataType visitDateLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DateLiteral expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.DATE);
  }

  @Override
  public DataType visitIntervalLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, IntervalLiteral expr)
      throws PlanningException {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INTERVAL);
  }
}
