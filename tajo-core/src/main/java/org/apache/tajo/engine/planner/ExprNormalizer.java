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

import com.google.common.collect.Sets;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.engine.exception.NoSuchColumnException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Set;
import java.util.Stack;

/**
 * ExprNormalizer performs two kinds of works:
 *
 * <h3>1. Duplication Removal.</h3>
 *
 * For example, assume a simple query as follows:
 * <pre>
 *   select price * rate as total_price, ..., order by price * rate
 * </pre>
 *
 * The expression <code>price * rate</code> is duplicated in both select list and order by clause.
 * Against those cases, ExprNormalizer removes duplicated expressions and replaces one with one reference.
 * In the case, ExprNormalizer replaces price * rate with total_price reference.
 *
 * <h3>2. Dissection of Expression</h3>
 *
 * A expression can be a complex expressions, including a mixed of scalar and aggregation expressions.
 * For example, assume an aggregation query as follows:
 * <pre>
 *   select sum(price * rate) * (1 - avg(discount_rate))), ...
 * </pre>
 *
 * In this case, ExprNormalizer dissects the expression 'sum(price * rate) * (1 - avg(discount_rate)))'
 * into the following expressions:
 * <ul>
 *   <li>$1 = price * rage</li>
 *   <li>$2 = sum($1)</li>
 *   <li>$3 = avg(discount_rate)</li>
 *   <li>$4 = $2 * (1 - $3)</li>
 * </ul>
 *
 * It mainly two advantages. Firstly, it makes complex expression evaluations easier across multiple physical executors.
 * Second, it gives move opportunities to remove duplicated expressions.
 *
 * <h3>3. Name Normalization</h3>
 *
 * Users can use qualified column names, unqualified column names or aliased column references.
 *
 * Consider the following example:
 *
 * <pre>
 *   select rate_a as total_rate, rate_a * 100, table1.rate_a, ... WHERE total_rate * 100
 * </pre>
 *
 * <code>total_rate</code>, <code>rate_a</code>, and <code>table1.rate_a</code> are all the same references. But,
 * they have different forms. Due to their different forms, duplication removal can be hard.
 *
 * In order to solve this problem, ExprNormalizer normalizes all column references as qualified names while it keeps
 * its points..
 */
class ExprNormalizer extends SimpleAlgebraVisitor<ExprNormalizer.ExprNormalizedResult, Object> {

  public static class ExprNormalizedResult {
    private final LogicalPlan plan;
    private final LogicalPlan.QueryBlock block;
    private final boolean tryBinaryCommonTermsElimination;

    Expr baseExpr; // outmost expressions, which can includes one or more references of the results of aggregation
                   // function.
    List<NamedExpr> aggExprs = new ArrayList<NamedExpr>(); // aggregation functions
    List<NamedExpr> scalarExprs = new ArrayList<NamedExpr>(); // scalar expressions which can be referred
    List<NamedExpr> windowAggExprs = new ArrayList<NamedExpr>(); // window expressions which can be referred
    Set<WindowSpecReferences> windowSpecs = Sets.newLinkedHashSet();

    private ExprNormalizedResult(LogicalPlanner.PlanContext context, boolean tryBinaryCommonTermsElimination) {
      this.plan = context.plan;
      this.block = context.queryBlock;
      this.tryBinaryCommonTermsElimination = tryBinaryCommonTermsElimination;
    }

    public boolean isBinaryCommonTermsElimination() {
      return tryBinaryCommonTermsElimination;
    }

    @Override
    public String toString() {
      return baseExpr.toString() + ", agg=" + aggExprs.size() + ", scalar=" + scalarExprs.size();
    }
  }

  public ExprNormalizedResult normalize(LogicalPlanner.PlanContext context, Expr expr) throws PlanningException {
    return normalize(context, expr, false);
  }
  public ExprNormalizedResult normalize(LogicalPlanner.PlanContext context, Expr expr, boolean subexprElimination)
      throws PlanningException {
    ExprNormalizedResult exprNormalizedResult = new ExprNormalizedResult(context, subexprElimination);
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    visit(exprNormalizedResult, new Stack<Expr>(), expr);
    exprNormalizedResult.baseExpr = stack.pop();
    return exprNormalizedResult;
  }

  @Override
  public Object visitCaseWhen(ExprNormalizedResult ctx, Stack<Expr> stack, CaseWhenPredicate expr)
      throws PlanningException {
    stack.push(expr);
    for (CaseWhenPredicate.WhenExpr when : expr.getWhens()) {
      visit(ctx, stack, when.getCondition());
      visit(ctx, stack, when.getResult());

      if (OpType.isAggregationFunction(when.getCondition().getType())) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(when.getCondition());
        ctx.aggExprs.add(new NamedExpr(when.getCondition(), referenceName));
        when.setCondition(new ColumnReferenceExpr(referenceName));
      }

      if (OpType.isAggregationFunction(when.getResult().getType())) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(when.getResult());
        ctx.aggExprs.add(new NamedExpr(when.getResult(), referenceName));
        when.setResult(new ColumnReferenceExpr(referenceName));
      }
    }

    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
      if (OpType.isAggregationFunction(expr.getElseResult().getType())) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(expr.getElseResult());
        ctx.aggExprs.add(new NamedExpr(expr.getElseResult(), referenceName));
        expr.setElseResult(new ColumnReferenceExpr(referenceName));
      }
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitUnaryOperator(ExprNormalizedResult ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    super.visitUnaryOperator(ctx, stack, expr);
    if (OpType.isAggregationFunction(expr.getChild().getType())) {
      // Get an anonymous column name and replace the aggregation function by the column name
      String refName = ctx.block.namedExprsMgr.addExpr(expr.getChild());
      ctx.aggExprs.add(new NamedExpr(expr.getChild(), refName));
      expr.setChild(new ColumnReferenceExpr(refName));
    }

    return expr;
  }

  private boolean isBinaryCommonTermsElimination(ExprNormalizedResult ctx, Expr expr) {
    return ctx.isBinaryCommonTermsElimination() && expr.getType() != OpType.Column
        && ctx.block.namedExprsMgr.contains(expr);
  }

  @Override
  public Expr visitBinaryOperator(ExprNormalizedResult ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);

    visit(ctx, new Stack<Expr>(), expr.getLeft());
    if (isBinaryCommonTermsElimination(ctx, expr.getLeft())) {
      String refName = ctx.block.namedExprsMgr.addExpr(expr.getLeft());
      expr.setLeft(new ColumnReferenceExpr(refName));
    }

    visit(ctx, new Stack<Expr>(), expr.getRight());
    if (isBinaryCommonTermsElimination(ctx, expr.getRight())) {
      String refName = ctx.block.namedExprsMgr.addExpr(expr.getRight());
      expr.setRight(new ColumnReferenceExpr(refName));
    }
    stack.pop();

    ////////////////////////
    // For Left Term
    ////////////////////////

    if (OpType.isAggregationFunction(expr.getLeft().getType())) {
      String leftRefName = ctx.block.namedExprsMgr.addExpr(expr.getLeft());
      ctx.aggExprs.add(new NamedExpr(expr.getLeft(), leftRefName));
      expr.setLeft(new ColumnReferenceExpr(leftRefName));
    }


    ////////////////////////
    // For Right Term
    ////////////////////////
    if (OpType.isAggregationFunction(expr.getRight().getType())) {
      String rightRefName = ctx.block.namedExprsMgr.addExpr(expr.getRight());
      ctx.aggExprs.add(new NamedExpr(expr.getRight(), rightRefName));
      expr.setRight(new ColumnReferenceExpr(rightRefName));
    }

    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Function Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitFunction(ExprNormalizedResult ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    stack.push(expr);

    Expr param;
    Expr[] paramExprs = expr.getParams();
    if (paramExprs != null) {
      for (int i = 0; i < paramExprs.length; i++) {
        param = paramExprs[i];
        visit(ctx, stack, param);

        if (OpType.isAggregationFunction(param.getType())) {
          String referenceName = ctx.plan.generateUniqueColumnName(param);
          ctx.aggExprs.add(new NamedExpr(param, referenceName));
          expr.getParams()[i] = new ColumnReferenceExpr(referenceName);
        }
      }
    }
    stack.pop();

    return expr;
  }

  @Override
  public Expr visitGeneralSetFunction(ExprNormalizedResult ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    stack.push(expr);

    Expr param;
    for (int i = 0; i < expr.getParams().length; i++) {
      param = expr.getParams()[i];
      visit(ctx, stack, param);


      // If parameters are all constants, we don't need to dissect an aggregation expression into two parts:
      // function and parameter parts.
      if (!OpType.isLiteralType(param.getType()) && param.getType() != OpType.Column) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(param);
        ctx.scalarExprs.add(new NamedExpr(param, referenceName));
        expr.getParams()[i] = new ColumnReferenceExpr(referenceName);
      }
    }
    stack.pop();
    return expr;
  }

  public Expr visitWindowFunction(ExprNormalizedResult ctx, Stack<Expr> stack, WindowFunctionExpr expr)
      throws PlanningException {
    stack.push(expr);

    WindowSpec windowSpec = expr.getWindowSpec();
    Expr key;

    WindowSpecReferences windowSpecReferences;
    if (windowSpec.hasWindowName()) {
      windowSpecReferences = new WindowSpecReferences(windowSpec.getWindowName());
    } else {
      String [] partitionKeyReferenceNames = null;
      if (windowSpec.hasPartitionBy()) {
        partitionKeyReferenceNames = new String [windowSpec.getPartitionKeys().length];
        for (int i = 0; i < windowSpec.getPartitionKeys().length; i++) {
          key = windowSpec.getPartitionKeys()[i];
          visit(ctx, stack, key);
          partitionKeyReferenceNames[i] = ctx.block.namedExprsMgr.addExpr(key);
        }
      }

      String [] orderKeyReferenceNames = null;
      if (windowSpec.hasOrderBy()) {
        orderKeyReferenceNames = new String[windowSpec.getSortSpecs().length];
        for (int i = 0; i < windowSpec.getSortSpecs().length; i++) {
          key = windowSpec.getSortSpecs()[i].getKey();
          visit(ctx, stack, key);
          String referenceName = ctx.block.namedExprsMgr.addExpr(key);
          if (OpType.isAggregationFunction(key.getType())) {
            ctx.aggExprs.add(new NamedExpr(key, referenceName));
            windowSpec.getSortSpecs()[i].setKey(new ColumnReferenceExpr(referenceName));
          }
          orderKeyReferenceNames[i] = referenceName;
        }
      }
      windowSpecReferences =
          new WindowSpecReferences(partitionKeyReferenceNames,orderKeyReferenceNames);
    }
    ctx.windowSpecs.add(windowSpecReferences);

    String funcExprRef = ctx.block.namedExprsMgr.addExpr(expr);
    ctx.windowAggExprs.add(new NamedExpr(expr, funcExprRef));
    stack.pop();

    ctx.block.setHasWindowFunction();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCastExpr(ExprNormalizedResult ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    super.visitCastExpr(ctx, stack, expr);
    if (OpType.isAggregationFunction(expr.getType())) {
      String referenceName = ctx.block.namedExprsMgr.addExpr(expr.getChild());
      ctx.aggExprs.add(new NamedExpr(expr.getChild(), referenceName));
      expr.setChild(new ColumnReferenceExpr(referenceName));
    }
    return expr;
  }

  @Override
  public Expr visitColumnReference(ExprNormalizedResult ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    // if a column reference is not qualified, it finds and sets the qualified column name.
    if (!(expr.hasQualifier() && CatalogUtil.isFQTableName(expr.getQualifier()))) {
      if (!ctx.block.namedExprsMgr.contains(expr.getCanonicalName()) && expr.getType() == OpType.Column) {
        try {
          String normalized = ctx.plan.getNormalizedColumnName(ctx.block, expr);
          expr.setName(normalized);
        } catch (NoSuchColumnException nsc) {
        }
      }
    }
    return expr;
  }

  public static class WindowSpecReferences {
    String windowName;

    String [] partitionKeys;
    String [] orderKeys;

    public WindowSpecReferences(String windowName) {
      this.windowName = windowName;
    }

    public WindowSpecReferences(String [] partitionKeys, String [] orderKeys) {
      this.partitionKeys = partitionKeys;
      this.orderKeys = orderKeys;
    }

    public String getWindowName() {
      return windowName;
    }

    public boolean hasPartitionKeys() {
      return partitionKeys != null;
    }

    public String [] getPartitionKeys() {
      return partitionKeys;
    }

    public boolean hasOrderBy() {
      return orderKeys != null;
    }

    public String [] getOrderKeys() {
      return orderKeys;
    }
  }
}
