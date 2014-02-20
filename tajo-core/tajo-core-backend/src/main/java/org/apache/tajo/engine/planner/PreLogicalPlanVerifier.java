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

import com.google.common.collect.ObjectArrays;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;
import java.util.Set;
import java.util.Stack;

public class PreLogicalPlanVerifier extends BaseAlgebraVisitor <VerificationState, Expr> {
  private CatalogService catalog;

  public PreLogicalPlanVerifier(CatalogService catalog) {
    this.catalog = catalog;
  }

  public Expr visitProjection(VerificationState state, Stack<Expr> stack, Projection expr) throws PlanningException {
    super.visitProjection(state, stack, expr);

    Set<String> names = TUtil.newHashSet();
    Expr [] distinctValues = null;

    for (NamedExpr namedExpr : expr.getNamedExprs()) {

      if (namedExpr.hasAlias()) {
        if (names.contains(namedExpr.getAlias())) {
          state.addVerification(String.format("column name \"%s\" specified more than once", namedExpr.getAlias()));
        } else {
          names.add(namedExpr.getAlias());
        }
      }

      // no two aggregations can have different DISTINCT columns.
      //
      // For example, the following query will work
      // SELECT count(DISTINCT col1) and sum(DISTINCT col1) ..
      //
      // But, the following query will not work in this time
      //
      // SELECT count(DISTINCT col1) and SUM(DISTINCT col2) ..
      Set<GeneralSetFunctionExpr> exprs = ExprFinder.finds(namedExpr.getExpr(), OpType.GeneralSetFunction);
      if (exprs.size() > 0) {
        for (GeneralSetFunctionExpr setFunction : exprs) {
          if (distinctValues == null && setFunction.isDistinct()) {
            distinctValues = setFunction.getParams();
          } else if (distinctValues != null) {
            if (!Arrays.equals(distinctValues, setFunction.getParams())) {
              Expr [] differences = ObjectArrays.concat(distinctValues, setFunction.getParams(), Expr.class);
              throw new PlanningException("different DISTINCT columns are not supported yet: "
                  + TUtil.arrayToString(differences));
            }
          }
        }
      }

      // Currently, avg functions with distinct aggregation are not supported.
      // This code does not allow users to use avg functions with distinct aggregation.
      if (distinctValues != null) {
        for (GeneralSetFunctionExpr setFunction : exprs) {
          if (setFunction.getSignature().equalsIgnoreCase("avg")) {
            if (setFunction.isDistinct()) {
              throw new PlanningException("avg(distinct) function is not supported yet.");
            } else {
              throw new PlanningException("avg() function with distinct aggregation functions is not supported yet.");
            }
          }
        }
      }
    }
    return expr;
  }

  @Override
  public Expr visitGroupBy(VerificationState ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    super.visitGroupBy(ctx, stack, expr);

    // Enforcer only ordinary grouping set.
    for (Aggregation.GroupElement groupingElement : expr.getGroupSet()) {
      if (groupingElement.getType() != Aggregation.GroupType.OrdinaryGroup) {
        ctx.addVerification(groupingElement.getType() + " is not supported yet");
      }
    }

    Projection projection = null;
    for (Expr parent : stack) {
      if (parent.getType() == OpType.Projection) {
        projection = (Projection) parent;
        break;
      }
    }

    if (projection == null) {
      throw new PlanningException("No Projection");
    }

    return expr;
  }

  @Override
  public Expr visitRelation(VerificationState state, Stack<Expr> stack, Relation expr) throws PlanningException {
    assertRelationExistence(state, expr.getName());
    return expr;
  }

  private boolean assertRelationExistence(VerificationState state, String name) {
    if (!catalog.existsTable(name)) {
      state.addVerification(String.format("relation \"%s\" does not exist", name));
      return false;
    }
    return true;
  }

  private boolean assertRelationNoExistence(VerificationState state, String name) {
    if (catalog.existsTable(name)) {
      state.addVerification(String.format("relation \"%s\" already exists", name));
      return false;
    }
    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCreateTable(VerificationState state, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    super.visitCreateTable(state, stack, expr);
    assertRelationNoExistence(state, expr.getTableName());
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(VerificationState state, Stack<Expr> stack, Insert expr) throws PlanningException {
    Expr child = super.visitInsert(state, stack, expr);

    if (expr.hasTableName()) {
      assertRelationExistence(state, expr.getTableName());
    }

    if (child != null && child.getType() == OpType.Projection) {
      if (expr.hasTargetColumns()) {
        Projection projection = (Projection) child;
        int projectColumnNum = projection.getNamedExprs().length;
        int targetColumnNum = expr.getTargetColumns().length;

        if (targetColumnNum > projectColumnNum)  {
          state.addVerification("INSERT has more target columns than expressions");
        } else if (targetColumnNum < projectColumnNum) {
          state.addVerification("INSERT has more expressions than target columns");
        }
      }
    }

    return expr;
  }
}
