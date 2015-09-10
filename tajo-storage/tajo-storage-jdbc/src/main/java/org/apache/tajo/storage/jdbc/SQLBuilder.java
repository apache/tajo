/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.jdbc;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.util.Stack;

/**
 * Generator to build a SQL statement from a plan fragment
 */
public class SQLBuilder {
  @SuppressWarnings("unused")
  private final DatabaseMetaData dbMetaData;
  private final SQLExpressionGenerator sqlExprGen;

  public static class SQLBuilderContext {
    StringBuilder sb;
  }

  public SQLBuilder(DatabaseMetaData dbMetaData, SQLExpressionGenerator exprGen) {
    this.dbMetaData = dbMetaData;
    this.sqlExprGen = exprGen;
  }

  public String build(String tableName, Column [] targets, @Nullable EvalNode filter, @Nullable Long limit) {

    StringBuilder selectClause = new StringBuilder("SELECT ");
    if (targets.length > 0) {
      selectClause.append(StringUtils.join(targets, ",", new Function<Column, String>() {
        @Override
        public String apply(@Nullable Column input) {
          return input.getSimpleName();
        }
      }));
    } else {
      selectClause.append("1");
    }
    selectClause.append(" ");

    StringBuilder fromClause = new StringBuilder("FROM ");
    fromClause.append(tableName).append(" ");

    StringBuilder whereClause = null;
    if (filter != null) {
      whereClause = new StringBuilder("WHERE ");
      whereClause.append(sqlExprGen.generate(filter)).append(" ");
    }

    StringBuilder limitClause = null;
    if (limit != null) {
      limitClause = new StringBuilder("LIMIT ");
      limitClause.append(limit).append(" ");
    }

    return generateSelectStmt(selectClause, fromClause, whereClause, limitClause);
  }

  public String generateSelectStmt(StringBuilder selectClause,
                                   StringBuilder fromClause,
                                   @Nullable StringBuilder whereClause,
                                   @Nullable StringBuilder limitClause) {
    return
        selectClause.toString() +
        fromClause.toString() +
        (whereClause != null ? whereClause.toString() : "") +
        (limitClause != null ? limitClause.toString() : "");
  }

  public String build(LogicalNode planPart) {
    SQLBuilderContext context = new SQLBuilderContext();
    visit(context, planPart, new Stack<LogicalNode>());
    return context.sb.toString();
  }

  public void visit(SQLBuilderContext context, LogicalNode node, Stack<LogicalNode> stack) {
    stack.push(node);

    switch (node.getType()) {
    case SCAN:
      visitScan(context, (ScanNode) node, stack);
      break;

    case GROUP_BY:
      visitGroupBy(context, (GroupbyNode) node, stack);
      break;

    case SELECTION:
      visitFilter(context, (SelectionNode) node, stack);
      break;

    case PROJECTION:
      visitProjection(context, (ProjectionNode) node, stack);
      break;

    case TABLE_SUBQUERY:
      visitDerivedSubquery(context, (TableSubQueryNode) node, stack);
      break;

    default:
      throw new TajoRuntimeException(new UnsupportedException("plan node '" + node.getType().name() + "'"));
    }

    stack.pop();
  }

  public void visitDerivedSubquery(SQLBuilderContext ctx, TableSubQueryNode derivedSubquery, Stack<LogicalNode> stack) {
    ctx.sb.append(" (");
    visit(ctx, derivedSubquery.getSubQuery(), stack);
    ctx.sb.append(" ) ").append(derivedSubquery.getTableName());
  }

  public void visitProjection(SQLBuilderContext ctx, ProjectionNode projection, Stack<LogicalNode> stack) {

    visit(ctx, projection.getChild(), stack);
  }

  public void visitGroupBy(SQLBuilderContext ctx, GroupbyNode groupby, Stack<LogicalNode> stack) {
    visit(ctx, groupby.getChild(), stack);
    ctx.sb.append("GROUP BY ").append(StringUtils.join(groupby.getGroupingColumns(), ",", 0)).append(" ");
  }

  public void visitFilter(SQLBuilderContext ctx, SelectionNode filter, Stack<LogicalNode> stack) {
    visit(ctx, filter.getChild(), stack);
    ctx.sb.append("WHERE " + sqlExprGen.generate(filter.getQual()));
  }

  public void visitScan(SQLBuilderContext ctx, ScanNode scan, Stack<LogicalNode> stack) {

    StringBuilder selectClause = new StringBuilder("SELECT ");
    if (scan.getTargets().length > 0) {
      selectClause.append(generateTargetList(scan.getTargets()));
    } else {
      selectClause.append("1");
    }

    selectClause.append(" ");

    ctx.sb.append("FROM ").append(scan.getTableName()).append(" ");

    if (scan.hasAlias()) {
      ctx.sb.append("AS ").append(scan.getAlias()).append(" ");
    }

    if (scan.hasQual()) {
      ctx.sb.append("WHERE " + sqlExprGen.generate(scan.getQual()));
    }
  }

  public String generateTargetList(Target [] targets) {
    return StringUtils.join(targets, ",", new Function<Target, String>() {
      @Override
      public String apply(@Nullable Target t) {
        StringBuilder sb = new StringBuilder(sqlExprGen.generate(t.getEvalTree()));
        if (t.hasAlias()) {
          sb.append(" AS ").append(t.getAlias());
        }
        return sb.toString();
      }
    });
  }
}
