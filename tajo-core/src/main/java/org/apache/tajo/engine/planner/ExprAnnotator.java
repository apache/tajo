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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.WindowSpec;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameEndBoundType;
import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameStartBoundType;
import static org.apache.tajo.algebra.WindowSpecExpr.WindowFrameUnit;
import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.apache.tajo.engine.planner.logical.WindowSpec.WindowEndBound;
import static org.apache.tajo.engine.planner.logical.WindowSpec.WindowFrame;
import static org.apache.tajo.engine.planner.logical.WindowSpec.WindowStartBound;

/**
 * <code>ExprAnnotator</code> makes an annotated expression called <code>EvalNode</code> from an
 * {@link org.apache.tajo.algebra.Expr}. It visits descendants recursively from a given expression, and finally
 * it returns an EvalNode.
 */
public class ExprAnnotator extends BaseAlgebraVisitor<ExprAnnotator.Context, EvalNode> {
  private CatalogService catalog;

  public ExprAnnotator(CatalogService catalog) {
    this.catalog = catalog;
  }

  static class Context {
    LogicalPlan plan;
    LogicalPlan.QueryBlock currentBlock;

    public Context(LogicalPlan plan, LogicalPlan.QueryBlock block) {
      this.plan = plan;
      this.currentBlock = block;
    }
  }

  public EvalNode createEvalNode(LogicalPlan plan, LogicalPlan.QueryBlock block, Expr expr)
      throws PlanningException {
    Context context = new Context(plan, block);
    return AlgebraicUtil.eliminateConstantExprs(visit(context, new Stack<Expr>(), expr));
  }

  public static void assertEval(boolean condition, String message) throws PlanningException {
    if (!condition) {
      throw new PlanningException(message);
    }
  }

  /**
   * It checks both terms in binary expression. If one of both needs type conversion, it inserts a cast expression.
   *
   * @param lhs left hand side term
   * @param rhs right hand side term
   * @return a pair including left/right hand side terms
   */
  public static Pair<EvalNode, EvalNode> convertTypesIfNecessary(EvalNode lhs, EvalNode rhs) {
    Type lhsType = lhs.getValueType().getType();
    Type rhsType = rhs.getValueType().getType();

    // If one of both is NULL, it just returns the original types without casting.
    if (lhsType == Type.NULL_TYPE || rhsType == Type.NULL_TYPE) {
      return new Pair<EvalNode, EvalNode>(lhs, rhs);
    }

    Type toBeCasted = TUtil.getFromNestedMap(TYPE_CONVERSION_MAP, lhsType, rhsType);
    if (toBeCasted != null) { // if not null, one of either should be converted to another type.
      // Overwrite lhs, rhs, or both with cast expression.
      if (lhsType != toBeCasted) {
        lhs = convertType(lhs, CatalogUtil.newSimpleDataType(toBeCasted));
      }
      if (rhsType != toBeCasted) {
        rhs = convertType(rhs, CatalogUtil.newSimpleDataType(toBeCasted));
      }
    }

    return new Pair<EvalNode, EvalNode>(lhs, rhs);
  }

  /**
   * It picks out the widest range type among given <code>types</code>.
   *
   * Example:
   * <ul>
   *   <li>int, int8  -> int8 </li>
   *   <li>int4, int8, float4  -> float4 </li>
   *   <li>float4, float8 -> float8</li>
   *   <li>float4, text -> exception!</li>
   * </ul>
   *
   * @param types A list of DataTypes
   * @return The widest DataType
   * @throws PlanningException when types are not compatible, it throws the exception.
   */
  static DataType getWidestType(DataType...types) throws PlanningException {
    DataType widest = types[0];
    for (int i = 1; i < types.length; i++) {
      Type candidate = TUtil.getFromNestedMap(TYPE_CONVERSION_MAP, widest.getType(), types[i].getType());
      assertEval(candidate != null, "No matched operation for those types: " + TUtil.arrayToString(types));
      widest = CatalogUtil.newSimpleDataType(candidate);
    }

    return widest;
  }

  /**
   * Insert a type conversion expression to a given expression.
   * If the type of expression and <code>toType</code> is already the same, it just returns the original expression.
   *
   * @param evalNode an expression
   * @param toType target type
   * @return type converted expression.
   */
  private static EvalNode convertType(EvalNode evalNode, DataType toType) {

    // if original and toType is the same, we don't need type conversion.
    if (evalNode.getValueType() == toType) {
      return evalNode;
    }
    // the conversion to null is not allowed.
    if (evalNode.getValueType().getType() == Type.NULL_TYPE || toType.getType() == Type.NULL_TYPE) {
      return evalNode;
    }

    if (evalNode.getType() == EvalType.BETWEEN) {
      BetweenPredicateEval between = (BetweenPredicateEval) evalNode;

      between.setPredicand(convertType(between.getPredicand(), toType));
      between.setBegin(convertType(between.getBegin(), toType));
      between.setEnd(convertType(between.getEnd(), toType));

      return between;

    } else if (evalNode.getType() == EvalType.CASE) {

      CaseWhenEval caseWhenEval = (CaseWhenEval) evalNode;
      for (CaseWhenEval.IfThenEval ifThen : caseWhenEval.getIfThenEvals()) {
        ifThen.setResult(convertType(ifThen.getResult(), toType));
      }

      if (caseWhenEval.hasElse()) {
        caseWhenEval.setElseResult(convertType(caseWhenEval.getElse(), toType));
      }

      return caseWhenEval;

    } else if (evalNode.getType() == EvalType.ROW_CONSTANT) {
      RowConstantEval original = (RowConstantEval) evalNode;

      Datum[] datums = original.getValues();
      Datum[] convertedDatum = new Datum[datums.length];

      for (int i = 0; i < datums.length; i++) {
        convertedDatum[i] = DatumFactory.cast(datums[i], toType);
      }

      RowConstantEval convertedRowConstant = new RowConstantEval(convertedDatum);

      return convertedRowConstant;

    } else if (evalNode.getType() == EvalType.CONST) {
      ConstEval original = (ConstEval) evalNode;
      ConstEval newConst = new ConstEval(DatumFactory.cast(original.getValue(), toType));
      return newConst;

    } else {
      return new CastEval(evalNode, toType);
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Logical Operator Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitAnd(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return new BinaryEval(EvalType.AND, left, right);
  }

  @Override
  public EvalNode visitOr(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return new BinaryEval(EvalType.OR, left, right);
  }

  @Override
  public EvalNode visitNot(Context ctx, Stack<Expr> stack, NotExpr expr) throws PlanningException {
    stack.push(expr);
    EvalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();
    return new NotEval(child);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Comparison Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public EvalNode visitEquals(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  @Override
  public EvalNode visitNotEquals(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  @Override
  public EvalNode visitLessThan(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  @Override
  public EvalNode visitLessThanOrEquals(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  @Override
  public EvalNode visitGreaterThan(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  @Override
  public EvalNode visitGreaterThanOrEquals(Context ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    return visitCommonComparison(ctx, stack, expr);
  }

  public EvalNode visitCommonComparison(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    EvalType evalType;
    switch (expr.getType()) {
      case Equals:
        evalType = EvalType.EQUAL;
        break;
      case NotEquals:
        evalType = EvalType.NOT_EQUAL;
        break;
      case LessThan:
        evalType = EvalType.LTH;
        break;
      case LessThanOrEquals:
        evalType = EvalType.LEQ;
        break;
      case GreaterThan:
        evalType = EvalType.GTH;
        break;
      case GreaterThanOrEquals:
        evalType = EvalType.GEQ;
        break;
      default:
      throw new IllegalStateException("Wrong Expr Type: " + expr.getType());
    }

    return createBinaryNode(evalType, left, right);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitBetween(Context ctx, Stack<Expr> stack, BetweenPredicate between) throws PlanningException {
    stack.push(between);
    EvalNode predicand = visit(ctx, stack, between.predicand());
    EvalNode begin = visit(ctx, stack, between.begin());
    EvalNode end = visit(ctx, stack, between.end());
    stack.pop();

    // implicit type conversion
    DataType widestType = getWidestType(predicand.getValueType(), begin.getValueType(), end.getValueType());

    BetweenPredicateEval betweenEval = new BetweenPredicateEval(
        between.isNot(),
        between.isSymmetric(),
        predicand, begin, end);

    betweenEval = (BetweenPredicateEval) convertType(betweenEval, widestType);
    return betweenEval;
  }

  @Override
  public EvalNode visitCaseWhen(Context ctx, Stack<Expr> stack, CaseWhenPredicate caseWhen) throws PlanningException {
    CaseWhenEval caseWhenEval = new CaseWhenEval();

    EvalNode condition;
    EvalNode result;

    for (CaseWhenPredicate.WhenExpr when : caseWhen.getWhens()) {
      condition = visit(ctx, stack, when.getCondition());
      result = visit(ctx, stack, when.getResult());
      caseWhenEval.addWhen(condition, result);
    }

    if (caseWhen.hasElseResult()) {
      caseWhenEval.setElseResult(visit(ctx, stack, caseWhen.getElseResult()));
    }

    // Getting the widest type from all if-then expressions and else expression.
    DataType widestType = caseWhenEval.getIfThenEvals().get(0).getResult().getValueType();
    for (int i = 1; i < caseWhenEval.getIfThenEvals().size(); i++) {
      widestType = getWidestType(caseWhenEval.getIfThenEvals().get(i).getResult().getValueType(), widestType);
    }
    if (caseWhen.hasElseResult()) {
      widestType = getWidestType(widestType, caseWhenEval.getElse().getValueType());
    }

    assertEval(widestType != null, "Invalid Type Conversion for CaseWhen");

    // implicit type conversion
    caseWhenEval = (CaseWhenEval) convertType(caseWhenEval, widestType);

    return caseWhenEval;
  }

  @Override
  public EvalNode visitIsNullPredicate(Context ctx, Stack<Expr> stack, IsNullPredicate expr) throws PlanningException {
    stack.push(expr);
    EvalNode child = visit(ctx, stack, expr.getPredicand());
    stack.pop();
    return new IsNullEval(expr.isNot(), child);
  }

  @Override
  public EvalNode visitInPredicate(Context ctx, Stack<Expr> stack, InPredicate expr) throws PlanningException {
    stack.push(expr);
    EvalNode lhs = visit(ctx, stack, expr.getLeft());
    RowConstantEval rowConstantEval = (RowConstantEval) visit(ctx, stack, expr.getInValue());
    stack.pop();

    Pair<EvalNode, EvalNode> pair = convertTypesIfNecessary(lhs, rowConstantEval);

    return new InEval(pair.getFirst(), (RowConstantEval) pair.getSecond(), expr.isNot());
  }

  @Override
  public EvalNode visitValueListExpr(Context ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException {
    Datum[] values = new Datum[expr.getValues().length];
    EvalNode [] evalNodes = new EvalNode[expr.getValues().length];
    for (int i = 0; i < expr.getValues().length; i++) {
      evalNodes[i] = visit(ctx, stack, expr.getValues()[i]);
      if (!EvalTreeUtil.checkIfCanBeConstant(evalNodes[i])) {
        throw new PlanningException("Non constant values cannot be included in IN PREDICATE.");
      }
      values[i] = EvalTreeUtil.evaluateImmediately(evalNodes[i]);
    }
    return new RowConstantEval(values);
  }

  @Override
  public EvalNode visitExistsPredicate(Context ctx, Stack<Expr> stack, ExistsPredicate expr) throws PlanningException {
    throw new PlanningException("Cannot support EXISTS clause yet");
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // String Operator or Pattern Matching Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public EvalNode visitLikePredicate(Context ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public EvalNode visitSimilarToPredicate(Context ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public EvalNode visitRegexpPredicate(Context ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public EvalNode visitConcatenate(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return new BinaryEval(EvalType.CONCATENATE, left, right);
  }

  private EvalNode visitPatternMatchPredicate(Context ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    EvalNode field = visit(ctx, stack, expr.getPredicand());
    ConstEval pattern = (ConstEval) visit(ctx, stack, expr.getPattern());

    // A pattern is a const value in pattern matching predicates.
    // In a binary expression, the result is always null if a const value in left or right side is null.
    if (pattern.getValue() instanceof NullDatum) {
      return new ConstEval(NullDatum.get());
    } else {
      if (expr.getType() == OpType.LikePredicate) {
        return new LikePredicateEval(expr.isNot(), field, pattern, expr.isCaseInsensitive());
      } else if (expr.getType() == OpType.SimilarToPredicate) {
        return new SimilarToPredicateEval(expr.isNot(), field, pattern);
      } else {
        return new RegexPredicateEval(expr.isNot(), field, pattern, expr.isCaseInsensitive());
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Arithmetic Operators
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  private static BinaryEval createBinaryNode(EvalType type, EvalNode lhs, EvalNode rhs) {
    Pair<EvalNode, EvalNode> pair = convertTypesIfNecessary(lhs, rhs); // implicit type conversion if necessary
    return new BinaryEval(type, pair.getFirst(), pair.getSecond());
  }

  @Override
  public EvalNode visitPlus(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return createBinaryNode(EvalType.PLUS, left, right);
  }

  @Override
  public EvalNode visitMinus(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return createBinaryNode(EvalType.MINUS, left, right);
  }

  @Override
  public EvalNode visitMultiply(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return createBinaryNode(EvalType.MULTIPLY, left, right);
  }

  @Override
  public EvalNode visitDivide(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return createBinaryNode(EvalType.DIVIDE, left, right);
  }

  @Override
  public EvalNode visitModular(Context ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    EvalNode left = visit(ctx, stack, expr.getLeft());
    EvalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();

    return createBinaryNode(EvalType.MODULAR, left, right);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitSign(Context ctx, Stack<Expr> stack, SignedExpr expr) throws PlanningException {
    stack.push(expr);
    EvalNode numericExpr = visit(ctx, stack, expr.getChild());
    stack.pop();

    if (expr.isNegative()) {
      return new SignedEval(expr.isNegative(), numericExpr);
    } else {
      return numericExpr;
    }
  }

  @Override
  public EvalNode visitColumnReference(Context ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    Column column = ctx.plan.resolveColumn(ctx.currentBlock, expr);
    return new FieldEval(column);
  }

  @Override
  public EvalNode visitTargetExpr(Context ctx, Stack<Expr> stack, NamedExpr expr) throws PlanningException {
    throw new PlanningException("ExprAnnotator cannot take NamedExpr");
  }

  @Override
  public EvalNode visitFunction(Context ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    stack.push(expr); // <--- Push

    // Given parameters
    Expr[] params = expr.getParams();
    if (params == null) {
      params = new Expr[0];
    }

    EvalNode[] givenArgs = new EvalNode[params.length];
    DataType[] paramTypes = new DataType[params.length];

    for (int i = 0; i < params.length; i++) {
      givenArgs[i] = visit(ctx, stack, params[i]);
      paramTypes[i] = givenArgs[i].getValueType();
    }

    stack.pop(); // <--- Pop

    if (!catalog.containFunction(expr.getSignature(), paramTypes)) {
      throw new NoSuchFunctionException(expr.getSignature(), paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(expr.getSignature(), paramTypes);

    // trying the implicit type conversion between actual parameter types and the definition types.
    if (CatalogUtil.checkIfVariableLengthParamDefinition(TUtil.newList(funcDesc.getParamTypes()))) {
      DataType lastDataType = null;
      for (int i = 0; i < givenArgs.length; i++) {
        if (i < (funcDesc.getParamTypes().length - 1)) { // variable length
          lastDataType = funcDesc.getParamTypes()[i];
        } else {
          lastDataType = CatalogUtil.newSimpleDataType(CatalogUtil.getPrimitiveTypeOf(lastDataType.getType()));
        }
        givenArgs[i] = convertType(givenArgs[i], lastDataType);
      }
    } else {
      assertEval(funcDesc.getParamTypes().length == givenArgs.length,
          "The number of parameters is mismatched to the function definition: " + funcDesc.toString());
      // According to our function matching method, each given argument can be casted to the definition parameter.
      for (int i = 0; i < givenArgs.length; i++) {
        givenArgs[i] = convertType(givenArgs[i], funcDesc.getParamTypes()[i]);
      }
    }


    try {
      FunctionType functionType = funcDesc.getFuncType();
      if (functionType == FunctionType.GENERAL
          || functionType == FunctionType.UDF) {
        return new GeneralFunctionEval(funcDesc, (GeneralFunction) funcDesc.newInstance(), givenArgs);
      } else if (functionType == FunctionType.AGGREGATION
          || functionType == FunctionType.UDA) {
        if (!ctx.currentBlock.hasNode(NodeType.GROUP_BY)) {
          ctx.currentBlock.setAggregationRequire();
        }
        return new AggregationFunctionCallEval(funcDesc, (AggFunction) funcDesc.newInstance(), givenArgs);
      } else if (functionType == FunctionType.DISTINCT_AGGREGATION
          || functionType == FunctionType.DISTINCT_UDA) {
        throw new PlanningException("Unsupported function: " + funcDesc.toString());
      } else {
        throw new PlanningException("Unsupported Function Type: " + functionType.name());
      }
    } catch (InternalException e) {
      throw new PlanningException(e);
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitCountRowsFunction(Context ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    FunctionDesc countRows = catalog.getFunction("count", FunctionType.AGGREGATION,
        new DataType[] {});
    if (countRows == null) {
      throw new NoSuchFunctionException(expr.getSignature(), new DataType[]{});
    }

    try {
      ctx.currentBlock.setAggregationRequire();

      return new AggregationFunctionCallEval(countRows, (AggFunction) countRows.newInstance(),
          new EvalNode[] {});
    } catch (InternalException e) {
      throw new NoSuchFunctionException(countRows.getSignature(), new DataType[]{});
    }
  }

  @Override
  public EvalNode visitGeneralSetFunction(Context ctx, Stack<Expr> stack, GeneralSetFunctionExpr setFunction)
      throws PlanningException {

    Expr[] params = setFunction.getParams();
    EvalNode[] givenArgs = new EvalNode[params.length];
    DataType[] paramTypes = new DataType[params.length];

    FunctionType functionType = setFunction.isDistinct() ?
        FunctionType.DISTINCT_AGGREGATION : FunctionType.AGGREGATION;
    givenArgs[0] = visit(ctx, stack, params[0]);
    if (setFunction.getSignature().equalsIgnoreCase("count")) {
      paramTypes[0] = CatalogUtil.newSimpleDataType(Type.ANY);
    } else {
      paramTypes[0] = givenArgs[0].getValueType();
    }

    if (!catalog.containFunction(setFunction.getSignature(), functionType, paramTypes)) {
      throw new NoSuchFunctionException(setFunction.getSignature(), paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(setFunction.getSignature(), functionType, paramTypes);
    if (!ctx.currentBlock.hasNode(NodeType.GROUP_BY)) {
      ctx.currentBlock.setAggregationRequire();
    }

    try {
      return new AggregationFunctionCallEval(funcDesc, (AggFunction) funcDesc.newInstance(), givenArgs);
    } catch (InternalException e) {
      throw new PlanningException(e);
    }
  }

  static final Set<String> WINDOW_FUNCTIONS =
      Sets.newHashSet("row_number", "rank", "dense_rank", "percent_rank", "cume_dist");

  public EvalNode visitWindowFunction(Context ctx, Stack<Expr> stack, WindowFunctionExpr windowFunc)
      throws PlanningException {

    WindowSpecExpr windowSpec = windowFunc.getWindowSpec();

    Expr key;
    if (windowSpec.hasPartitionBy()) {
      for (int i = 0; i < windowSpec.getPartitionKeys().length; i++) {
        key = windowSpec.getPartitionKeys()[i];
        visit(ctx, stack, key);
      }
    }

    EvalNode [] sortKeys = null;
    if (windowSpec.hasOrderBy()) {
      sortKeys = new EvalNode[windowSpec.getSortSpecs().length];
      for (int i = 0; i < windowSpec.getSortSpecs().length; i++) {
        key = windowSpec.getSortSpecs()[i].getKey();
        sortKeys[i] = visit(ctx, stack, key);
      }
    }

    String funcName = windowFunc.getSignature();
    boolean distinct = windowFunc.isDistinct();
    Expr[] params = windowFunc.getParams();
    EvalNode[] givenArgs = new EvalNode[params.length];
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[params.length];
    FunctionType functionType;

    WindowFrame frame = null;

    if (params.length > 0) {
      givenArgs[0] = visit(ctx, stack, params[0]);
      if (windowFunc.getSignature().equalsIgnoreCase("count")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.ANY);
      } else if (windowFunc.getSignature().equalsIgnoreCase("row_number")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(Type.INT8);
      } else {
        paramTypes[0] = givenArgs[0].getValueType();
      }
    } else {
      if (windowFunc.getSignature().equalsIgnoreCase("rank")) {
        givenArgs = sortKeys;
      }
    }

    if (frame == null) {
      if (windowSpec.hasOrderBy()) {
        frame = new WindowFrame(new WindowStartBound(WindowFrameStartBoundType.UNBOUNDED_PRECEDING),
            new WindowEndBound(WindowFrameEndBoundType.CURRENT_ROW));
      } else if (windowFunc.getSignature().equalsIgnoreCase("row_number")) {
        frame = new WindowFrame(new WindowStartBound(WindowFrameStartBoundType.UNBOUNDED_PRECEDING),
            new WindowEndBound(WindowFrameEndBoundType.UNBOUNDED_FOLLOWING));
      } else {
        frame = new WindowFrame();
      }
    }

    // TODO - containFunction and getFunction should support the function type mask which provides ORing multiple types.
    // the below checking against WINDOW_FUNCTIONS is a workaround code for the above problem.
    if (WINDOW_FUNCTIONS.contains(funcName.toLowerCase())) {
      if (distinct) {
        throw new NoSuchFunctionException("row_number() does not support distinct keyword.");
      }
      functionType = FunctionType.WINDOW;
    } else {
      functionType = distinct ? FunctionType.DISTINCT_AGGREGATION : FunctionType.AGGREGATION;
    }

    if (!catalog.containFunction(windowFunc.getSignature(), functionType, paramTypes)) {
      throw new NoSuchFunctionException(funcName, paramTypes);
    }

    FunctionDesc funcDesc = catalog.getFunction(funcName, functionType, paramTypes);

    try {
      return new WindowFunctionEval(funcDesc, (AggFunction) funcDesc.newInstance(), givenArgs, frame);
    } catch (InternalException e) {
      throw new PlanningException(e);
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public EvalNode visitDataType(Context ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException {
    return super.visitDataType(ctx, stack, expr);
  }

  @Override
  public EvalNode visitCastExpr(Context ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    EvalNode child = super.visitCastExpr(ctx, stack, expr);

    if (child.getType() == EvalType.CONST) { // if it is a casting operation for a constant value
      ConstEval constEval = (ConstEval) child; // it will be pre-computed and casted to a constant value
      return new ConstEval(DatumFactory.cast(constEval.getValue(), LogicalPlanner.convertDataType(expr.getTarget())));
    } else {
      return new CastEval(child, LogicalPlanner.convertDataType(expr.getTarget()));
    }
  }

  @Override
  public EvalNode visitLiteral(Context ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException {
    switch (expr.getValueType()) {
    case Boolean:
      return new ConstEval(DatumFactory.createBool(((BooleanLiteral) expr).isTrue()));
    case String:
      return new ConstEval(DatumFactory.createText(expr.getValue()));
    case Unsigned_Integer:
      return new ConstEval(DatumFactory.createInt4(expr.getValue()));
    case Unsigned_Large_Integer:
      return new ConstEval(DatumFactory.createInt8(expr.getValue()));
    case Unsigned_Float:
      return new ConstEval(DatumFactory.createFloat8(expr.getValue()));
    default:
      throw new RuntimeException("Unsupported type: " + expr.getValueType());
    }
  }

  @Override
  public EvalNode visitNullLiteral(Context ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException {
    return new ConstEval(NullDatum.get());
  }

  @Override
  public EvalNode visitDateLiteral(Context context, Stack<Expr> stack, DateLiteral expr) throws PlanningException {
    DateValue dateValue = expr.getDate();
    int[] dates = dateToIntArray(dateValue.getYears(), dateValue.getMonths(), dateValue.getDays());

    TimeMeta tm = new TimeMeta();
    tm.years = dates[0];
    tm.monthOfYear = dates[1];
    tm.dayOfMonth = dates[2];

    DateTimeUtil.j2date(DateTimeUtil.date2j(dates[0], dates[1], dates[2]), tm);

    return new ConstEval(new DateDatum(DateTimeUtil.date2j(tm.years, tm.monthOfYear, tm.dayOfMonth)));
  }

  @Override
  public EvalNode visitTimestampLiteral(Context ctx, Stack<Expr> stack, TimestampLiteral expr)
      throws PlanningException {
    DateValue dateValue = expr.getDate();
    TimeValue timeValue = expr.getTime();

    int [] dates = dateToIntArray(dateValue.getYears(),
        dateValue.getMonths(),
        dateValue.getDays());
    int [] times = timeToIntArray(timeValue.getHours(),
        timeValue.getMinutes(),
        timeValue.getSeconds(),
        timeValue.getSecondsFraction());

    long timestamp;
    if (timeValue.hasSecondsFraction()) {
      timestamp = DateTimeUtil.toJulianTimestamp(dates[0], dates[1], dates[2], times[0], times[1], times[2],
          times[3] * 1000);
    } else {
      timestamp = DateTimeUtil.toJulianTimestamp(dates[0], dates[1], dates[2], times[0], times[1], times[2], 0);
    }

    TimeMeta tm = new TimeMeta();
    DateTimeUtil.toJulianTimeMeta(timestamp, tm);
    DateTimeUtil.toUTCTimezone(tm);

    return new ConstEval(new TimestampDatum(DateTimeUtil.toJulianTimestamp(tm)));
  }

  @Override
  public EvalNode visitIntervalLiteral(Context ctx, Stack<Expr> stack, IntervalLiteral expr) throws PlanningException {
    return new ConstEval(new IntervalDatum(expr.getExprStr()));
  }

  @Override
  public EvalNode visitTimeLiteral(Context ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException {
    TimeValue timeValue = expr.getTime();
    int [] times = timeToIntArray(timeValue.getHours(),
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
    TimeMeta tm = timeDatum.toTimeMeta();
    DateTimeUtil.toUTCTimezone(tm);

    return new ConstEval(new TimeDatum(DateTimeUtil.toTime(tm)));
  }

  public static int [] dateToIntArray(String years, String months, String days)
      throws PlanningException {
    int year = Integer.valueOf(years);
    int month = Integer.valueOf(months);
    int day = Integer.valueOf(days);

    if (!(1 <= year && year <= 9999)) {
      throw new PlanningException(String.format("Years (%d) must be between 1 and 9999 integer value", year));
    }

    if (!(1 <= month && month <= 12)) {
      throw new PlanningException(String.format("Months (%d) must be between 1 and 12 integer value", month));
    }

    if (!(1<= day && day <= 31)) {
      throw new PlanningException(String.format("Days (%d) must be between 1 and 31 integer value", day));
    }

    int [] results = new int[3];
    results[0] = year;
    results[1] = month;
    results[2] = day;

    return results;
  }

  public static int [] timeToIntArray(String hours, String minutes, String seconds, String fractionOfSecond)
      throws PlanningException {
    int hour = Integer.valueOf(hours);
    int minute = Integer.valueOf(minutes);
    int second = Integer.valueOf(seconds);
    int fraction = 0;
    if (fractionOfSecond != null) {
      fraction = Integer.valueOf(fractionOfSecond);
    }

    if (!(0 <= hour && hour <= 23)) {
      throw new PlanningException(String.format("Hours (%d) must be between 0 and 24 integer value", hour));
    }

    if (!(0 <= minute && minute <= 59)) {
      throw new PlanningException(String.format("Minutes (%d) must be between 0 and 59 integer value", minute));
    }

    if (!(0 <= second && second <= 59)) {
      throw new PlanningException(String.format("Seconds (%d) must be between 0 and 59 integer value", second));
    }

    if (fraction != 0) {
      if (!(0 <= fraction && fraction <= 999)) {
        throw new PlanningException(String.format("Seconds (%d) must be between 0 and 999 integer value", fraction));
      }
    }

    int [] results = new int[4];
    results[0] = hour;
    results[1] = minute;
    results[2] = second;
    results[3] = fraction;

    return results;
  }

  /** It is the relationship graph of type conversions. It represents each type can be converted to which types. */
  static final Map<Type, Map<Type, Type>> TYPE_CONVERSION_MAP = Maps.newHashMap();
  static {
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.INT1, Type.INT1);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.INT2, Type.INT2);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT1, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.INT1, Type.INT2);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.INT2, Type.INT2);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT2, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.INT1, Type.INT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.INT2, Type.INT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT4, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.INT1, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.INT2, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.INT4, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INT8, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.INT1, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.INT2, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.INT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.INT8, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT4, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.INT1, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.INT2, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.INT4, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.INT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.FLOAT4, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.FLOAT8, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.TEXT, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.TIMESTAMP, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.TIMESTAMP, Type.TEXT, Type.TEXT);
    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.TEXT, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(TYPE_CONVERSION_MAP, Type.INET4, Type.INET4, Type.INET4);
  }
}
