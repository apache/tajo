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

import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class PreLogicalPlanVerifier extends BaseAlgebraVisitor <PreLogicalPlanVerifier.Context, Expr> {
  private CatalogService catalog;

  public PreLogicalPlanVerifier(CatalogService catalog) {
    this.catalog = catalog;
  }

  public static class Context {
    Session session;
    VerificationState state;

    public Context(Session session, VerificationState state) {
      this.session = session;
      this.state = state;
    }
  }

  public VerificationState verify(Session session, VerificationState state, Expr expr) throws PlanningException {
    Context context = new Context(session, state);
    visit(context, new Stack<Expr>(), expr);
    return context.state;
  }

  public Expr visitProjection(Context context, Stack<Expr> stack, Projection expr) throws PlanningException {
    super.visitProjection(context, stack, expr);

    Set<String> names = TUtil.newHashSet();
    Expr [] distinctValues = null;

    for (NamedExpr namedExpr : expr.getNamedExprs()) {

      if (namedExpr.hasAlias()) {
        if (names.contains(namedExpr.getAlias())) {
          context.state.addVerification(String.format("column name \"%s\" specified more than once",
              namedExpr.getAlias()));
        } else {
          names.add(namedExpr.getAlias());
        }
      }

      Set<GeneralSetFunctionExpr> exprs = ExprFinder.finds(namedExpr.getExpr(), OpType.GeneralSetFunction);

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
  public Expr visitGroupBy(Context context, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    super.visitGroupBy(context, stack, expr);

    // Enforcer only ordinary grouping set.
    for (Aggregation.GroupElement groupingElement : expr.getGroupSet()) {
      if (groupingElement.getType() != Aggregation.GroupType.OrdinaryGroup) {
        context.state.addVerification(groupingElement.getType() + " is not supported yet");
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
  public Expr visitRelation(Context context, Stack<Expr> stack, Relation expr) throws PlanningException {
    assertRelationExistence(context, expr.getName());
    return expr;
  }

  private boolean assertRelationExistence(Context context, String tableName) {
    String qualifiedName;

    if (CatalogUtil.isFQTableName(tableName)) {
      qualifiedName = tableName;
    } else {
      qualifiedName = CatalogUtil.buildFQName(context.session.getCurrentDatabase(), tableName);
    }

    if (!catalog.existsTable(qualifiedName)) {
      context.state.addVerification(String.format("relation \"%s\" does not exist", qualifiedName));
      return false;
    }
    return true;
  }

  private boolean assertRelationNoExistence(Context context, String tableName) {
    String qualifiedName;

    if (CatalogUtil.isFQTableName(tableName)) {
      qualifiedName = tableName;
    } else {
      qualifiedName = CatalogUtil.buildFQName(context.session.getCurrentDatabase(), tableName);
    }
    if (catalog.existsTable(qualifiedName)) {
      context.state.addVerification(String.format("relation \"%s\" already exists", qualifiedName));
      return false;
    }
    return true;
  }

  private boolean assertUnsupportedStoreType(VerificationState state, String name) {
    if (name != null && name.equals(CatalogProtos.StoreType.RAW.name())) {
      state.addVerification(String.format("Unsupported store type :%s", name));
      return false;
    }
    return true;
  }

  private boolean assertDatabaseExistence(VerificationState state, String name) {
    if (!catalog.existDatabase(name)) {
      state.addVerification(String.format("database \"%s\" does not exist", name));
      return false;
    }
    return true;
  }

  private boolean assertDatabaseNoExistence(VerificationState state, String name) {
    if (catalog.existDatabase(name)) {
      state.addVerification(String.format("database \"%s\" already exists", name));
      return false;
    }
    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////


  @Override
  public Expr visitCreateDatabase(Context context, Stack<Expr> stack, CreateDatabase expr)
      throws PlanningException {
    super.visitCreateDatabase(context, stack, expr);
    if (!expr.isIfNotExists()) {
      assertDatabaseNoExistence(context.state, expr.getDatabaseName());
    }
    return expr;
  }

  @Override
  public Expr visitDropDatabase(Context context, Stack<Expr> stack, DropDatabase expr) throws PlanningException {
    super.visitDropDatabase(context, stack, expr);
    if (!expr.isIfExists()) {
      assertDatabaseExistence(context.state, expr.getDatabaseName());
    }
    return expr;
  }

  @Override
  public Expr visitCreateTable(Context context, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    super.visitCreateTable(context, stack, expr);
    if (!expr.isIfNotExists()) {
      assertRelationNoExistence(context, expr.getTableName());
    }
    assertUnsupportedStoreType(context.state, expr.getStorageType());
    return expr;
  }

  @Override
  public Expr visitDropTable(Context context, Stack<Expr> stack, DropTable expr) throws PlanningException {
    super.visitDropTable(context, stack, expr);
    if (!expr.isIfExists()) {
      assertRelationExistence(context, expr.getTableName());
    }
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(Context context, Stack<Expr> stack, Insert expr) throws PlanningException {
    Expr child = super.visitInsert(context, stack, expr);

    if (expr.hasTableName()) {
      assertRelationExistence(context, expr.getTableName());
    }

    if (child != null && child.getType() == OpType.Projection) {
      Projection projection = (Projection) child;
      int projectColumnNum = projection.getNamedExprs().length;

      if (expr.hasTargetColumns()) {
        int targetColumnNum = expr.getTargetColumns().length;

        if (targetColumnNum > projectColumnNum)  {
          context.state.addVerification("INSERT has more target columns than expressions");
        } else if (targetColumnNum < projectColumnNum) {
          context.state.addVerification("INSERT has more expressions than target columns");
        }
      } else {
        if (expr.hasTableName()) {
          String qualifiedName = expr.getTableName();
          if (TajoConstants.EMPTY_STRING.equals(CatalogUtil.extractQualifier(expr.getTableName()))) {
            qualifiedName = CatalogUtil.buildFQName(context.session.getCurrentDatabase(),
                expr.getTableName());
          }

          TableDesc table = catalog.getTableDesc(qualifiedName);
          if (table.hasPartition()) {
            int columnSize = table.getSchema().getColumns().size();
            columnSize += table.getPartitionMethod().getExpressionSchema().getColumns().size();
            if (projectColumnNum < columnSize) {
              context.state.addVerification("INSERT has smaller expressions than target columns");
            } else if (projectColumnNum > columnSize) {
              context.state.addVerification("INSERT has more expressions than target columns");
            }
          }
        }
      }
    }

    return expr;
  }
}
