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
import org.apache.tajo.catalog.CatalogService;

import java.util.Stack;

public class PreLogicalPlanVerifier extends BaseAlgebraVisitor <VerificationState, Expr> {
  private CatalogService catalog;

  public PreLogicalPlanVerifier(CatalogService catalog) {
    this.catalog = catalog;
  }

  @Override
  public Expr visitGroupBy(VerificationState ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    Expr child = super.visitGroupBy(ctx, stack, expr);

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
    checkRelationExistence(state, expr.getName());
    return expr;
  }

  private boolean checkRelationExistence(VerificationState state, String name) {
    if (!catalog.existsTable(name)) {
      state.addVerification(String.format("relation \"%s\" does not exist", name));
      return false;
    }
    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(VerificationState context, Stack<Expr> stack, Insert expr) throws PlanningException {
    Expr child = super.visitInsert(context, stack, expr);

    if (expr.hasTableName()) {
      checkRelationExistence(context, expr.getTableName());
    }

    if (child != null && child.getType() == OpType.Projection) {
      if (expr.hasTargetColumns()) {
        Projection projection = (Projection) child;
        int projectColumnNum = projection.getNamedExprs().length;
        int targetColumnNum = expr.getTargetColumns().length;

        if (targetColumnNum > projectColumnNum)  {
          context.addVerification("INSERT has more target columns than expressions");
        } else if (targetColumnNum < projectColumnNum) {
          context.addVerification("INSERT has more expressions than target columns");
        }
      }
    }

    return expr;
  }
}
