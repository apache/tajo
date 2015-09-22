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

package org.apache.tajo.plan.verifier;

import com.google.common.base.Preconditions;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.util.ExprFinder;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.validation.ConstraintViolation;

import java.util.Collection;
import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.plan.verifier.SyntaxErrorUtil.makeSyntaxError;

public class PreLogicalPlanVerifier extends BaseAlgebraVisitor<PreLogicalPlanVerifier.Context, Expr> {
  private CatalogService catalog;

  public PreLogicalPlanVerifier(CatalogService catalog) {
    this.catalog = catalog;
  }

  public static class Context {
    OverridableConf queryContext;
    VerificationState state;

    public Context(OverridableConf queryContext, VerificationState state) {
      this.queryContext = queryContext;
      this.state = state;
    }
  }

  public VerificationState verify(OverridableConf queryContext, VerificationState state, Expr expr)
      throws TajoException {
    Context context = new Context(queryContext, state);
    visit(context, new Stack<Expr>(), expr);
    return context.state;
  }

  @Override
  public Expr visitSetSession(Context ctx, Stack<Expr> stack, SetSession expr) throws TajoException {

    // we should allow undefined session variables which can be used in query statements in the future.
    if (SessionVars.exists(expr.getName())) {
      SessionVars var = SessionVars.get(expr.getName());
      if (var.validator() != null) {
        Collection<ConstraintViolation> violations = var.validator().validate(expr.getValue());

        for (ConstraintViolation violation : violations) {
          ctx.state.addVerification(SyntaxErrorUtil.makeInvalidSessionVar(var.keyname(), violation.getMessage()));
        }
      }
    }

    return expr;
  }

  public Expr visitProjection(Context context, Stack<Expr> stack, Projection expr) throws TajoException {
    super.visitProjection(context, stack, expr);

    Set<String> names = TUtil.newHashSet();

    for (NamedExpr namedExpr : expr.getNamedExprs()) {

      if (namedExpr.hasAlias()) {
        if (names.contains(namedExpr.getAlias())) {
          context.state.addVerification(SyntaxErrorUtil.makeDuplicateAlias(namedExpr.getAlias()));
        } else {
          names.add(namedExpr.getAlias());
        }
      }
    }
    return expr;
  }

  @Override
  public Expr visitLimit(Context context, Stack<Expr> stack, Limit expr) throws TajoException {
    stack.push(expr);

    if (ExprFinder.finds(expr.getFetchFirstNum(), OpType.Column).size() > 0) {
      context.state.addVerification(SyntaxErrorUtil.makeSyntaxError("argument of LIMIT must not contain variables"));
    }

    visit(context, stack, expr.getFetchFirstNum());
    Expr result = visit(context, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public Expr visitGroupBy(Context context, Stack<Expr> stack, Aggregation expr) throws TajoException {
    super.visitGroupBy(context, stack, expr);

    // Enforcer only ordinary grouping set.
    for (Aggregation.GroupElement groupingElement : expr.getGroupSet()) {
      if (groupingElement.getType() != Aggregation.GroupType.OrdinaryGroup) {
        context.state.addVerification(ExceptionUtil.makeNotSupported(groupingElement.getType().name()));
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
      throw new TajoInternalError("No Projection");
    }

    return expr;
  }

  @Override
  public Expr visitRelation(Context context, Stack<Expr> stack, Relation expr) throws TajoException {
    assertRelationExistence(context, expr.getName());
    return expr;
  }

  private boolean assertRelationExistence(Context context, String tableName) {
    String qualifiedName;

    if (CatalogUtil.isFQTableName(tableName)) {
      qualifiedName = tableName;
    } else {
      qualifiedName = CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE), tableName);
    }

    if (!catalog.existsTable(qualifiedName)) {
      context.state.addVerification(new UndefinedTableException(qualifiedName));
      return false;
    }
    return true;
  }

  private void assertRelationSchema(Context context, CreateTable createTable) {
    for (ColumnDefinition colDef : createTable.getTableElements()) {
      if (colDef.isMapType()) {
        context.state.addVerification(new UnsupportedException("map type"));
      }
    }
  }

  private static String guessTableName(Context context, String givenName) {
    String qualifiedName;
    if (CatalogUtil.isFQTableName(givenName)) {
      qualifiedName = givenName;
    } else {
      qualifiedName = CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE), givenName);
    }

    return qualifiedName;
  }

  private boolean assertRelationNoExistence(Context context, String tableName) {
    String qualifiedName = guessTableName(context, tableName);

    if (catalog.existsTable(qualifiedName)) {
      context.state.addVerification(new DuplicateTableException(qualifiedName));
      return false;
    }
    return true;
  }

  private boolean assertSupportedStoreType(VerificationState state, String name) {
    Preconditions.checkNotNull(name);

    if (name.equalsIgnoreCase("RAW")) {
      state.addVerification(SyntaxErrorUtil.makeUnknownDataFormat(name));
      return false;
    }
    return true;
  }

  private boolean assertDatabaseExistence(VerificationState state, String name) {
    if (!catalog.existDatabase(name)) {
      state.addVerification(new UndefinedDatabaseException(name));
      return false;
    }
    return true;
  }

  private boolean assertDatabaseNoExistence(VerificationState state, String name) {
    if (catalog.existDatabase(name)) {
      state.addVerification(new DuplicateDatabaseException(name));
      return false;
    }
    return true;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////


  @Override
  public Expr visitCreateDatabase(Context context, Stack<Expr> stack, CreateDatabase expr)
      throws TajoException {
    super.visitCreateDatabase(context, stack, expr);
    if (!expr.isIfNotExists()) {
      assertDatabaseNoExistence(context.state, expr.getDatabaseName());
    }
    return expr;
  }

  @Override
  public Expr visitDropDatabase(Context context, Stack<Expr> stack, DropDatabase expr) throws TajoException {
    super.visitDropDatabase(context, stack, expr);
    if (!expr.isIfExists()) {
      assertDatabaseExistence(context.state, expr.getDatabaseName());
    }
    return expr;
  }

  @Override
  public Expr visitCreateTable(Context context, Stack<Expr> stack, CreateTable expr) throws TajoException {
    super.visitCreateTable(context, stack, expr);

    if (!expr.isIfNotExists()) {
      assertRelationNoExistence(context, expr.getTableName());
    }

    if (expr.hasTableElements()) {
      assertRelationSchema(context, expr);
    }

    if (expr.hasStorageType()) {
      assertSupportedStoreType(context.state, expr.getStorageType());
    }

    return expr;
  }

  @Override
  public Expr visitDropTable(Context context, Stack<Expr> stack, DropTable expr) throws TajoException {
    super.visitDropTable(context, stack, expr);
    if (!expr.isIfExists()) {
      assertRelationExistence(context, expr.getTableName());
    }
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(Context context, Stack<Expr> stack, Insert expr) throws TajoException {
    Expr child = super.visitInsert(context, stack, expr);

    if (expr.hasTableName()) {
      assertRelationExistence(context, expr.getTableName());
    }

    if (expr.hasStorageType()) {
      assertSupportedStoreType(context.state, expr.getStorageType());
    }

    if (child != null && child.getType() == OpType.Projection) {
      Projection projection = (Projection) child;

      // checking if at least one asterisk exists in target list
      boolean includeAsterisk = false;
      for (NamedExpr namedExpr : projection.getNamedExprs()) {
        includeAsterisk |= namedExpr.getExpr().getType() == OpType.Asterisk;
      }

      // If one asterisk expression exists, we verify the match between the target exprs and output exprs.
      // This verification will be in LogicalPlanVerifier.
      if (!includeAsterisk) {

        int projectColumnNum = projection.getNamedExprs().length;

        if (expr.hasTargetColumns()) {
          int targetColumnNum = expr.getTargetColumns().length;

          if (targetColumnNum > projectColumnNum) {
            context.state.addVerification(makeSyntaxError("INSERT has more target columns than expressions"));
          } else if (targetColumnNum < projectColumnNum) {
            context.state.addVerification(makeSyntaxError("INSERT has more expressions than target columns"));
          }
        } else {
          if (expr.hasTableName()) {
            String qualifiedName = expr.getTableName();
            if (TajoConstants.EMPTY_STRING.equals(CatalogUtil.extractQualifier(expr.getTableName()))) {
              qualifiedName = CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE),
                  expr.getTableName());
            }

            TableDesc table = catalog.getTableDesc(qualifiedName);
            if (table == null) {
              context.state.addVerification(new UndefinedTableException(qualifiedName));
              return null;
            }
            if (table.hasPartition()) {
              int columnSize = table.getSchema().getRootColumns().size();
              columnSize += table.getPartitionMethod().getExpressionSchema().getRootColumns().size();

              if (projectColumnNum < columnSize) {
                context.state.addVerification(makeSyntaxError("INSERT has smaller expressions than target columns"));
              } else if (projectColumnNum > columnSize) {
                context.state.addVerification(makeSyntaxError("INSERT has more expressions than target columns"));
              }
            }
          }
        }
      }
    }

    return expr;
  }
}
