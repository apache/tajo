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
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.planner.LogicalPlan.QueryBlock;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.nameresolver.NameResolvingMode;
import org.apache.tajo.engine.planner.nameresolver.NameResolver;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * It finds all relations for each block and builds base schema information.
 */
public class LogicalPlanPreprocessor extends BaseAlgebraVisitor<LogicalPlanPreprocessor.PreprocessContext, LogicalNode> {
  private TypeDeterminant typeDeterminant;
  private ExprAnnotator annotator;

  public static class PreprocessContext {
    public Session session;
    public LogicalPlan plan;
    public LogicalPlan.QueryBlock currentBlock;

    public PreprocessContext(Session session, LogicalPlan plan, LogicalPlan.QueryBlock currentBlock) {
      this.session = session;
      this.plan = plan;
      this.currentBlock = currentBlock;
    }

    public PreprocessContext(PreprocessContext context, LogicalPlan.QueryBlock currentBlock) {
      this.session = context.session;
      this.plan = context.plan;
      this.currentBlock = currentBlock;
    }
  }

  /** Catalog service */
  private CatalogService catalog;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
    this.typeDeterminant = new TypeDeterminant(catalog);
  }

  @Override
  public void preHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    ctx.currentBlock.setAlgebraicExpr(expr);
    ctx.plan.mapExprToBlock(expr, ctx.currentBlock.getName());
  }

  @Override
  public LogicalNode postHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr, LogicalNode result) throws PlanningException {
    // If non-from statement, result can be null. It avoids that case.
    if (result != null) {
      // setNode method registers each node to corresponding block and plan.
      ctx.currentBlock.registerNode(result);
      // It makes a map between an expr and a logical node.
      ctx.currentBlock.registerExprWithNode(expr, result);
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
  public static Column[] getColumns(PreprocessContext ctx, QualifiedAsteriskExpr asteriskExpr)
      throws PlanningException {
    RelationNode relationOp = null;
    QueryBlock block = ctx.currentBlock;
    Collection<QueryBlock> queryBlocks = ctx.plan.getQueryBlocks();
    if (asteriskExpr.hasQualifier()) {
      String qualifier;

      if (CatalogUtil.isFQTableName(asteriskExpr.getQualifier())) {
        qualifier = asteriskExpr.getQualifier();
      } else {
        qualifier = CatalogUtil.buildFQName(ctx.session.getCurrentDatabase(), asteriskExpr.getQualifier());
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

      Schema schema = relationOp.getTableSchema();
      Column[] resolvedColumns = new Column[schema.size()];
      return schema.getColumns().toArray(resolvedColumns);
    } else { // if a column reference is not qualified
      // columns of every relation should be resolved.
      Iterator<RelationNode> iterator = block.getRelations().iterator();
      Schema schema;
      List<Column> resolvedColumns = TUtil.newList();

      while (iterator.hasNext()) {
        relationOp = iterator.next();
        schema = relationOp.getTableSchema();
        resolvedColumns.addAll(schema.getColumns());
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
  private static List<NamedExpr> resolveAsterisk(PreprocessContext ctx, QualifiedAsteriskExpr asteriskExpr)
      throws PlanningException {
    Column[] columns = getColumns(ctx, asteriskExpr);
    List<NamedExpr> newTargetExprs = new ArrayList<NamedExpr>(columns.length);
    int i;
    for (i = 0; i < columns.length; i++) {
      newTargetExprs.add(new NamedExpr(new ColumnReferenceExpr(columns[i].getQualifier(), columns[i].getSimpleName())));
    }
    return newTargetExprs;
  }

  private static boolean hasAsterisk(Projection projection) {
    for (NamedExpr eachTarget : projection.getNamedExprs()) {
      if (eachTarget.getExpr().getType() == OpType.Asterisk) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalNode visitProjection(PreprocessContext ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    // If Non-from statement, it immediately returns.
    if (!expr.hasChild()) {
      return ctx.plan.createNode(EvalExprNode.class);
    }

    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    // Resolve the asterisk expression
    if (hasAsterisk(expr)) {
      List<NamedExpr> rewrittenTargets = TUtil.newList();
      for (NamedExpr originTarget : expr.getNamedExprs()) {
        if (originTarget.getExpr().getType() == OpType.Asterisk) {
          // rewrite targets
          rewrittenTargets.addAll(resolveAsterisk(ctx, (QualifiedAsteriskExpr) originTarget.getExpr()));
        } else {
          rewrittenTargets.add(originTarget);
        }
      }
      expr.setNamedExprs(rewrittenTargets.toArray(new NamedExpr[rewrittenTargets.size()]));
    }

    // 1) Normalize field names into full qualified names
    // 2) Register explicit column aliases to block
    NamedExpr[] projectTargetExprs = expr.getNamedExprs();
    NameRefInSelectListNormalizer normalizer = new NameRefInSelectListNormalizer();
    for (int i = 0; i < expr.getNamedExprs().length; i++) {
      NamedExpr namedExpr = projectTargetExprs[i];
      normalizer.visit(ctx, new Stack<Expr>(), namedExpr.getExpr());

      if (namedExpr.getExpr().getType() == OpType.Column && namedExpr.hasAlias()) {
        ctx.currentBlock.addColumnAlias(((ColumnReferenceExpr)namedExpr.getExpr()).getCanonicalName(),
            namedExpr.getAlias());
      }
    }

    Target [] targets;
    targets = new Target[projectTargetExprs.length];

    for (int i = 0; i < expr.getNamedExprs().length; i++) {
      NamedExpr namedExpr = expr.getNamedExprs()[i];
      TajoDataTypes.DataType dataType = typeDeterminant.determineDataType(ctx, namedExpr.getExpr());

      if (namedExpr.hasAlias()) {
        targets[i] = new Target(new FieldEval(new Column(namedExpr.getAlias(), dataType)));
      } else {
        String generatedName = ctx.plan.generateUniqueColumnName(namedExpr.getExpr());
        targets[i] = new Target(new FieldEval(new Column(generatedName, dataType)));
      }
    }
    stack.pop(); // <--- Pop

    ProjectionNode projectionNode = ctx.plan.createNode(ProjectionNode.class);
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));

    ctx.currentBlock.setSchema(projectionNode.getOutSchema());
    return projectionNode;
  }

  @Override
  public LogicalNode visitLimit(PreprocessContext ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    LimitNode limitNode = ctx.plan.createNode(LimitNode.class);
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());
    return limitNode;
  }

  @Override
  public LogicalNode visitSort(PreprocessContext ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SortNode sortNode = ctx.plan.createNode(SortNode.class);
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    return sortNode;
  }

  @Override
  public LogicalNode visitHaving(PreprocessContext ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    HavingNode havingNode = ctx.plan.createNode(HavingNode.class);
    havingNode.setInSchema(child.getOutSchema());
    havingNode.setOutSchema(child.getOutSchema());
    return havingNode;
  }

  @Override
  public LogicalNode visitGroupBy(PreprocessContext ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Projection projection = ctx.currentBlock.getSingletonExpr(OpType.Projection);
    int finalTargetNum = projection.getNamedExprs().length;
    Target [] targets = new Target[finalTargetNum];

    for (int i = 0; i < finalTargetNum; i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
      EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, namedExpr.getExpr(),
          NameResolvingMode.SUBEXPRS_AND_RELS);

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
  public LogicalNode visitUnion(PreprocessContext ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    LogicalPlan.QueryBlock leftBlock = ctx.plan.newQueryBlock();
    PreprocessContext leftContext = new PreprocessContext(ctx, leftBlock);
    LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), expr.getLeft());
    leftBlock.setRoot(leftChild);
    ctx.currentBlock.registerExprWithNode(expr.getLeft(), leftChild);

    LogicalPlan.QueryBlock rightBlock = ctx.plan.newQueryBlock();
    PreprocessContext rightContext = new PreprocessContext(ctx, rightBlock);
    LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), expr.getRight());
    rightBlock.setRoot(rightChild);
    ctx.currentBlock.registerExprWithNode(expr.getRight(), rightChild);

    UnionNode unionNode = new UnionNode(ctx.plan.newPID());
    unionNode.setLeftChild(leftChild);
    unionNode.setRightChild(rightChild);
    unionNode.setInSchema(leftChild.getOutSchema());
    unionNode.setOutSchema(leftChild.getOutSchema());

    return unionNode;
  }

  public LogicalNode visitFilter(PreprocessContext ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SelectionNode selectionNode = ctx.plan.createNode(SelectionNode.class);
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());
    return selectionNode;
  }

  @Override
  public LogicalNode visitJoin(PreprocessContext ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    LogicalNode left = visit(ctx, stack, expr.getLeft());
    LogicalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();
    JoinNode joinNode = ctx.plan.createNode(JoinNode.class);
    joinNode.setJoinType(expr.getJoinType());
    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    joinNode.setInSchema(merged);
    joinNode.setOutSchema(merged);

    ctx.currentBlock.addJoinType(expr.getJoinType());
    return joinNode;
  }

  @Override
  public LogicalNode visitRelation(PreprocessContext ctx, Stack<Expr> stack, Relation expr)
      throws PlanningException {
    Relation relation = expr;

    String actualRelationName;
    if (CatalogUtil.isFQTableName(expr.getName())) {
      actualRelationName = relation.getName();
    } else {
      actualRelationName = CatalogUtil.buildFQName(ctx.session.getCurrentDatabase(), relation.getName());
    }

    TableDesc desc = catalog.getTableDesc(actualRelationName);
    ScanNode scanNode = ctx.plan.createNode(ScanNode.class);
    if (relation.hasAlias()) {
      scanNode.init(desc, relation.getAlias());
    } else {
      scanNode.init(desc);
    }
    ctx.currentBlock.addRelation(scanNode);

    return scanNode;
  }

  @Override
  public LogicalNode visitTableSubQuery(PreprocessContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {

    PreprocessContext newContext;
    // Note: TableSubQuery always has a table name.
    // SELECT .... FROM (SELECT ...) TB_NAME <-
    QueryBlock queryBlock = ctx.plan.newQueryBlock();
    newContext = new PreprocessContext(ctx, queryBlock);
    LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);
    queryBlock.setRoot(child);

    // a table subquery should be dealt as a relation.
    TableSubQueryNode node = ctx.plan.createNode(TableSubQueryNode.class);
    node.init(CatalogUtil.buildFQName(ctx.session.getCurrentDatabase(), expr.getName()), child);
    ctx.currentBlock.addRelation(node);
    return node;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public LogicalNode visitCreateDatabase(PreprocessContext ctx, Stack<Expr> stack, CreateDatabase expr)
      throws PlanningException {
    CreateDatabaseNode createDatabaseNode = ctx.plan.createNode(CreateDatabaseNode.class);
    return createDatabaseNode;
  }

  @Override
  public LogicalNode visitDropDatabase(PreprocessContext ctx, Stack<Expr> stack, DropDatabase expr)
      throws PlanningException {
    DropDatabaseNode dropDatabaseNode = ctx.plan.createNode(DropDatabaseNode.class);
    return dropDatabaseNode;
  }

  @Override
  public LogicalNode visitCreateTable(PreprocessContext ctx, Stack<Expr> stack, CreateTable expr)
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
  public LogicalNode visitDropTable(PreprocessContext ctx, Stack<Expr> stack, DropTable expr)
      throws PlanningException {
    DropTableNode dropTable = ctx.plan.createNode(DropTableNode.class);
    return dropTable;
  }

  @Override
  public LogicalNode visitAlterTablespace(PreprocessContext ctx, Stack<Expr> stack, AlterTablespace expr)
      throws PlanningException {
    AlterTablespaceNode alterTablespace = ctx.plan.createNode(AlterTablespaceNode.class);
    return alterTablespace;
  }

  @Override
  public LogicalNode visitAlterTable(PreprocessContext ctx, Stack<Expr> stack, AlterTable expr)
      throws PlanningException {
    AlterTableNode alterTableNode = ctx.plan.createNode(AlterTableNode.class);
    return alterTableNode;
  }

  @Override
  public LogicalNode visitTruncateTable(PreprocessContext ctx, Stack<Expr> stack, TruncateTable expr)
      throws PlanningException {
    TruncateTableNode truncateTableNode = ctx.plan.createNode(TruncateTableNode.class);
    return truncateTableNode;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public LogicalNode visitInsert(PreprocessContext ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    LogicalNode child = super.visitInsert(ctx, stack, expr);

    InsertNode insertNode = new InsertNode(ctx.plan.newPID());
    insertNode.setInSchema(child.getOutSchema());
    insertNode.setOutSchema(child.getOutSchema());
    return insertNode;
  }

  class NameRefInSelectListNormalizer extends SimpleAlgebraVisitor<PreprocessContext, Object> {
    @Override
    public Expr visitColumnReference(PreprocessContext ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
        throws PlanningException {

      String normalized = NameResolver.resolve(ctx.plan, ctx.currentBlock, expr,
          NameResolvingMode.RELS_AND_SUBEXPRS).getQualifiedName();
      expr.setName(normalized);

      return expr;
    }
  }
}
