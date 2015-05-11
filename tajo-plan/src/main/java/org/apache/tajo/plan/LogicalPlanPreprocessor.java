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

import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.NoSuchColumnException;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.expr.ConstEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.nameresolver.NameResolver;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * It finds all relations for each block and builds base schema information.
 */
public class LogicalPlanPreprocessor extends BaseAlgebraVisitor<LogicalPlanner.PlanContext, LogicalNode> {
  private TypeDeterminant typeDeterminant;
  private ExprAnnotator annotator;

  /** Catalog service */
  private CatalogService catalog;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
    this.typeDeterminant = new TypeDeterminant(catalog);
  }

  @Override
  public void preHook(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    ctx.queryBlock.setAlgebraicExpr(expr);
    ctx.plan.mapExprToBlock(expr, ctx.queryBlock.getName());
  }

  @Override
  public LogicalNode postHook(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Expr expr, LogicalNode result)
      throws PlanningException {
    // If non-from statement, result can be null. It avoids that case.
    if (result != null) {
      // setNode method registers each node to corresponding block and plan.
      ctx.queryBlock.registerNode(result);
      // It makes a map between an expr and a logical node.
      ctx.queryBlock.registerExprWithNode(expr, result);
    }
    return result;
  }

  /**
   * Get all columns of the relations correspondent to the asterisk expression.
   * @param ctx
   * @param asteriskExpr
   * @return array of columns
   * @throws PlanningException
   */
  public static Column[] getColumns(LogicalPlanner.PlanContext ctx, QualifiedAsteriskExpr asteriskExpr)
      throws PlanningException {
    RelationNode relationOp = null;
    QueryBlock block = ctx.queryBlock;
    Collection<QueryBlock> queryBlocks = ctx.plan.getQueryBlocks();
    if (asteriskExpr.hasQualifier()) {
      String qualifier;

      if (CatalogUtil.isFQTableName(asteriskExpr.getQualifier())) {
        qualifier = asteriskExpr.getQualifier();
      } else {
        qualifier = CatalogUtil.buildFQName(
            ctx.queryContext.get(SessionVars.CURRENT_DATABASE), asteriskExpr.getQualifier());
      }

      relationOp = block.getRelation(qualifier);

      // if a column name is outside of this query block
      if (relationOp == null) {
        // TODO - nested query can only refer outer query block? or not?
        for (QueryBlock eachBlock : queryBlocks) {
          if (eachBlock.existsRelation(qualifier)) {
            relationOp = eachBlock.getRelation(qualifier);
          }
        }
      }

      // If we cannot find any relation against a qualified column name
      if (relationOp == null) {
        throw new NoSuchColumnException(CatalogUtil.buildFQName(qualifier, "*"));
      }

      Schema schema = relationOp.getLogicalSchema();
      Column[] resolvedColumns = new Column[schema.size()];
      return schema.getRootColumns().toArray(resolvedColumns);
    } else { // if a column reference is not qualified
      // columns of every relation should be resolved.
      Iterator<RelationNode> iterator = block.getRelations().iterator();
      Schema schema;
      List<Column> resolvedColumns = TUtil.newList();

      while (iterator.hasNext()) {
        relationOp = iterator.next();
        schema = relationOp.getLogicalSchema();
        resolvedColumns.addAll(schema.getRootColumns());
      }

      if (resolvedColumns.size() == 0) {
        throw new NoSuchColumnException(asteriskExpr.toString());
      }

      return resolvedColumns.toArray(new Column[resolvedColumns.size()]);
    }
  }

  /**
   * Resolve an asterisk expression to the real column reference expressions.
   * @param ctx context
   * @param asteriskExpr asterisk expression
   * @return a list of NamedExpr each of which has ColumnReferenceExprs as its child
   * @throws PlanningException
   */
  private static List<NamedExpr> resolveAsterisk(LogicalPlanner.PlanContext ctx, QualifiedAsteriskExpr asteriskExpr)
      throws PlanningException {
    Column[] columns = getColumns(ctx, asteriskExpr);
    List<NamedExpr> newTargetExprs = new ArrayList<NamedExpr>(columns.length);
    int i;
    for (i = 0; i < columns.length; i++) {
      newTargetExprs.add(new NamedExpr(new ColumnReferenceExpr(columns[i].getQualifier(), columns[i].getSimpleName())));
    }
    return newTargetExprs;
  }

  private static boolean hasAsterisk(NamedExpr [] namedExprs) {
    for (NamedExpr eachTarget : namedExprs) {
      if (eachTarget.getExpr().getType() == OpType.Asterisk) {
        return true;
      }
    }
    return false;
  }

  private static NamedExpr [] voidResolveAsteriskNamedExpr(LogicalPlanner.PlanContext context,
                                                           NamedExpr [] namedExprs) throws PlanningException {
    List<NamedExpr> rewrittenTargets = TUtil.newList();
    for (NamedExpr originTarget : namedExprs) {
      if (originTarget.getExpr().getType() == OpType.Asterisk) {
        // rewrite targets
        rewrittenTargets.addAll(resolveAsterisk(context, (QualifiedAsteriskExpr) originTarget.getExpr()));
      } else {
        rewrittenTargets.add(originTarget);
      }
    }
    return rewrittenTargets.toArray(new NamedExpr[rewrittenTargets.size()]);
  }

  @Override
  public LogicalNode visitSetSession(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, SetSession expr)
      throws PlanningException {
    SetSessionNode setSession = ctx.plan.createNode(SetSessionNode.class);
    return setSession;
  }

  @Override
  public LogicalNode visitProjection(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Projection expr)
      throws PlanningException {
    // If Non-from statement, it immediately returns.
    if (!expr.hasChild()) {
      EvalExprNode exprNode = ctx.plan.createNode(EvalExprNode.class);
      exprNode.setTargets(buildTargets(ctx, expr.getNamedExprs()));
      return exprNode;
    }

    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    // Resolve the asterisk expression
    if (hasAsterisk(expr.getNamedExprs())) {
      expr.setNamedExprs(voidResolveAsteriskNamedExpr(ctx, expr.getNamedExprs()));
    }

    NamedExpr[] projectTargetExprs = expr.getNamedExprs();
    for (int i = 0; i < expr.getNamedExprs().length; i++) {
      NamedExpr namedExpr = projectTargetExprs[i];

      // 1) Normalize all field names occurred in each expr into full qualified names
      NameRefInSelectListNormalizer.normalize(ctx, namedExpr.getExpr());

      // 2) Register explicit column aliases to block
      if (namedExpr.getExpr().getType() == OpType.Column && namedExpr.hasAlias()) {
        ctx.queryBlock.addColumnAlias(((ColumnReferenceExpr)namedExpr.getExpr()).getCanonicalName(),
            namedExpr.getAlias());
      } else if (OpType.isLiteralType(namedExpr.getExpr().getType()) && namedExpr.hasAlias()) {
        Expr constExpr = namedExpr.getExpr();
        ConstEval constEval = (ConstEval) annotator.createEvalNode(ctx, constExpr, NameResolvingMode.RELS_ONLY);
        ctx.queryBlock.addConstReference(namedExpr.getAlias(), constExpr, constEval);
      }
    }

    Target[] targets = buildTargets(ctx, expr.getNamedExprs());

    stack.pop(); // <--- Pop

    ProjectionNode projectionNode = ctx.plan.createNode(ProjectionNode.class);
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));

    ctx.queryBlock.setSchema(projectionNode.getOutSchema());
    return projectionNode;
  }

  private Target [] buildTargets(LogicalPlanner.PlanContext context, NamedExpr [] exprs) throws PlanningException {
    Target [] targets = new Target[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      NamedExpr namedExpr = exprs[i];
      TajoDataTypes.DataType dataType = typeDeterminant.determineDataType(context, namedExpr.getExpr());

      if (namedExpr.hasAlias()) {
        targets[i] = new Target(new FieldEval(new Column(namedExpr.getAlias(), dataType)));
      } else {
        String generatedName = context.plan.generateUniqueColumnName(namedExpr.getExpr());
        targets[i] = new Target(new FieldEval(new Column(generatedName, dataType)));
      }
    }
    return targets;
  }

  @Override
  public LogicalNode visitLimit(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Limit expr)
      throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    LimitNode limitNode = ctx.plan.createNode(LimitNode.class);
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());
    return limitNode;
  }

  @Override
  public LogicalNode visitSort(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SortNode sortNode = ctx.plan.createNode(SortNode.class);
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    return sortNode;
  }

  @Override
  public LogicalNode visitHaving(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Having expr)
      throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    HavingNode havingNode = ctx.plan.createNode(HavingNode.class);
    havingNode.setInSchema(child.getOutSchema());
    havingNode.setOutSchema(child.getOutSchema());
    return havingNode;
  }

  @Override
  public LogicalNode visitGroupBy(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Aggregation expr)
      throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Projection projection = ctx.queryBlock.getSingletonExpr(OpType.Projection);
    int finalTargetNum = projection.getNamedExprs().length;
    Target [] targets = new Target[finalTargetNum];

    if (hasAsterisk(projection.getNamedExprs())) {
      projection.setNamedExprs(voidResolveAsteriskNamedExpr(ctx, projection.getNamedExprs()));
    }

    for (int i = 0; i < finalTargetNum; i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
      EvalNode evalNode = annotator.createEvalNode(ctx, namedExpr.getExpr(), NameResolvingMode.SUBEXPRS_AND_RELS);

      if (namedExpr.hasAlias()) {
        targets[i] = new Target(evalNode, namedExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, "?name_" + i);
      }
    }
    stack.pop();

    GroupbyNode groupByNode = ctx.plan.createNode(GroupbyNode.class);
    groupByNode.setInSchema(child.getOutSchema());
    groupByNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return groupByNode;
  }

  @Override
  public LogicalNode visitUnion(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, SetOperation expr)
      throws PlanningException {
    LogicalPlan.QueryBlock leftBlock = ctx.plan.newQueryBlock();
    LogicalPlanner.PlanContext leftContext = new LogicalPlanner.PlanContext(ctx, leftBlock);
    LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), expr.getLeft());
    leftBlock.setRoot(leftChild);
    ctx.queryBlock.registerExprWithNode(expr.getLeft(), leftChild);

    LogicalPlan.QueryBlock rightBlock = ctx.plan.newQueryBlock();
    LogicalPlanner.PlanContext rightContext = new LogicalPlanner.PlanContext(ctx, rightBlock);
    LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), expr.getRight());
    rightBlock.setRoot(rightChild);
    ctx.queryBlock.registerExprWithNode(expr.getRight(), rightChild);

    UnionNode unionNode = new UnionNode(ctx.plan.newPID());
    unionNode.setLeftChild(leftChild);
    unionNode.setRightChild(rightChild);
    unionNode.setInSchema(leftChild.getOutSchema());
    unionNode.setOutSchema(leftChild.getOutSchema());

    return unionNode;
  }

  public LogicalNode visitFilter(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Selection expr)
      throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SelectionNode selectionNode = ctx.plan.createNode(SelectionNode.class);
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());
    return selectionNode;
  }

  @Override
  public LogicalNode visitJoin(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    LogicalNode left = visit(ctx, stack, expr.getLeft());
    LogicalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();
    JoinNode joinNode = ctx.plan.createNode(JoinNode.class);
    joinNode.setJoinType(expr.getJoinType());
    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    joinNode.setInSchema(merged);
    joinNode.setOutSchema(merged);

    ctx.queryBlock.addJoinType(expr.getJoinType());
    return joinNode;
  }

  @Override
  public LogicalNode visitRelation(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Relation expr)
      throws PlanningException {
    Relation relation = expr;

    String actualRelationName;
    if (CatalogUtil.isFQTableName(expr.getName())) {
      actualRelationName = relation.getName();
    } else {
      actualRelationName =
          CatalogUtil.buildFQName(ctx.queryContext.get(SessionVars.CURRENT_DATABASE), relation.getName());
    }

    TableDesc desc = catalog.getTableDesc(actualRelationName);
    ScanNode scanNode = ctx.plan.createNode(ScanNode.class);
    if (relation.hasAlias()) {
      scanNode.init(desc, relation.getAlias());
    } else {
      scanNode.init(desc);
    }
    ctx.queryBlock.addRelation(scanNode);

    return scanNode;
  }

  @Override
  public LogicalNode visitTableSubQuery(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {

    LogicalPlanner.PlanContext newContext;
    // Note: TableSubQuery always has a table name.
    // SELECT .... FROM (SELECT ...) TB_NAME <-
    QueryBlock queryBlock = ctx.plan.newQueryBlock();
    newContext = new LogicalPlanner.PlanContext(ctx, queryBlock);
    LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);
    queryBlock.setRoot(child);

    // a table subquery should be dealt as a relation.
    TableSubQueryNode node = ctx.plan.createNode(TableSubQueryNode.class);
    node.init(CatalogUtil.buildFQName(ctx.queryContext.get(SessionVars.CURRENT_DATABASE), expr.getName()), child);
    ctx.queryBlock.addRelation(node);
    return node;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public LogicalNode visitCreateDatabase(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CreateDatabase expr)
      throws PlanningException {
    CreateDatabaseNode createDatabaseNode = ctx.plan.createNode(CreateDatabaseNode.class);
    return createDatabaseNode;
  }

  @Override
  public LogicalNode visitDropDatabase(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DropDatabase expr)
      throws PlanningException {
    DropDatabaseNode dropDatabaseNode = ctx.plan.createNode(DropDatabaseNode.class);
    return dropDatabaseNode;
  }

  @Override
  public LogicalNode visitCreateTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CreateTable expr)
      throws PlanningException {

    CreateTableNode createTableNode = ctx.plan.createNode(CreateTableNode.class);

    if (expr.hasSubQuery()) {
      stack.push(expr);
      visit(ctx, stack, expr.getSubQuery());
      stack.pop();
    }

    return createTableNode;
  }

  @Override
  public LogicalNode visitDropTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DropTable expr)
      throws PlanningException {
    DropTableNode dropTable = ctx.plan.createNode(DropTableNode.class);
    return dropTable;
  }

  @Override
  public LogicalNode visitAlterTablespace(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, AlterTablespace expr)
      throws PlanningException {
    AlterTablespaceNode alterTablespace = ctx.plan.createNode(AlterTablespaceNode.class);
    return alterTablespace;
  }

  @Override
  public LogicalNode visitAlterTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, AlterTable expr)
      throws PlanningException {
    AlterTableNode alterTableNode = ctx.plan.createNode(AlterTableNode.class);
    return alterTableNode;
  }

  @Override
  public LogicalNode visitTruncateTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TruncateTable expr)
      throws PlanningException {
    TruncateTableNode truncateTableNode = ctx.plan.createNode(TruncateTableNode.class);
    return truncateTableNode;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public LogicalNode visitInsert(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Insert expr)
      throws PlanningException {
    LogicalNode child = super.visitInsert(ctx, stack, expr);

    InsertNode insertNode = new InsertNode(ctx.plan.newPID());
    insertNode.setInSchema(child.getOutSchema());
    insertNode.setOutSchema(child.getOutSchema());
    return insertNode;
  }

  static class NameRefInSelectListNormalizer extends SimpleAlgebraVisitor<LogicalPlanner.PlanContext, Object> {
    private static final NameRefInSelectListNormalizer instance;

    static {
      instance = new NameRefInSelectListNormalizer();
    }

    public static void normalize(LogicalPlanner.PlanContext context, Expr expr) throws PlanningException {
      NameRefInSelectListNormalizer normalizer = new NameRefInSelectListNormalizer();
      normalizer.visit(context,new Stack<Expr>(), expr);
    }

    @Override
    public Expr visitColumnReference(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
        throws PlanningException {

      String normalized = NameResolver.resolve(ctx.plan, ctx.queryBlock, expr,
      NameResolvingMode.RELS_ONLY).getQualifiedName();
      expr.setName(normalized);

      return expr;
    }
  }
}
