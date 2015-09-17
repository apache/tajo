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

package org.apache.tajo.plan.rewrite;

import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedColumnException;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.expr.ConstEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.nameresolver.NameResolver;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * BaseSchemaBuildPhase builds a basic schema information of tables which have pre-defined schema.
 * For example, tables like the below example have pre-defined schema.
 *
 * CREATE TABLE t1 (id int8, name text);
 * CREATE EXTERNAL TABLE t2 (id int8, score int8, dept text);
 */
public class BaseSchemaBuildPhase extends LogicalPlanPreprocessPhase {

  private final Processor processor;

  public BaseSchemaBuildPhase(CatalogService catalog, ExprAnnotator annotator) {
    super(catalog, annotator);
    processor = new Processor(catalog, annotator);
  }

  @Override
  public String getName() {
    return "Base schema build phase";
  }

  @Override
  public boolean isEligible(PlanContext context, Expr expr) {
    return true;
  }

  @Override
  public LogicalNode process(PlanContext context, Expr expr) throws TajoException {
    return processor.visit(context, new Stack<Expr>(), expr);
  }
  
  static class Processor extends BaseAlgebraVisitor<PlanContext, LogicalNode> {
    private TypeDeterminant typeDeterminant;
    private ExprAnnotator annotator;

    /** Catalog service */
    private CatalogService catalog;

    Processor(CatalogService catalog, ExprAnnotator annotator) {
      this.catalog = catalog;
      this.annotator = annotator;
      this.typeDeterminant = new TypeDeterminant(catalog);
    }

    @Override
    public void preHook(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Expr expr) throws TajoException {
      ctx.getQueryBlock().setAlgebraicExpr(expr);
      ctx.getPlan().mapExprToBlock(expr, ctx.getQueryBlock().getName());
    }

    @Override
    public LogicalNode postHook(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Expr expr, LogicalNode result)
        throws TajoException {
      // If non-from statement, result can be null. It avoids that case.
      if (result != null) {
        // setNode method registers each node to corresponding block and plan.
        ctx.getQueryBlock().registerNode(result);
        // It makes a map between an expr and a logical node.
        ctx.getQueryBlock().registerExprWithNode(expr, result);
      }
      return result;
    }

    /**
     * Get all columns of the relations correspondent to the asterisk expression.
     * @param ctx
     * @param asteriskExpr
     * @return array of columns
     * @throws TajoException
     */
    public static Column[] getColumns(LogicalPlanner.PlanContext ctx, QualifiedAsteriskExpr asteriskExpr)
        throws TajoException {
      RelationNode relationOp = null;
      QueryBlock block = ctx.getQueryBlock();
      Collection<QueryBlock> queryBlocks = ctx.getPlan().getQueryBlocks();
      if (asteriskExpr.hasQualifier()) {
        String qualifier;

        if (CatalogUtil.isFQTableName(asteriskExpr.getQualifier())) {
          qualifier = asteriskExpr.getQualifier();
        } else {
          qualifier = CatalogUtil.buildFQName(
              ctx.getQueryContext().get(SessionVars.CURRENT_DATABASE), asteriskExpr.getQualifier());
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
          throw new UndefinedColumnException(CatalogUtil.buildFQName(qualifier, "*"));
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
          if (relationOp.isNameResolveBase()) {
            schema = relationOp.getLogicalSchema();
            resolvedColumns.addAll(schema.getRootColumns());
          }
        }

        if (resolvedColumns.size() == 0) {
          throw new UndefinedColumnException(asteriskExpr.toString());
        }

        return resolvedColumns.toArray(new Column[resolvedColumns.size()]);
      }
    }

    /**
     * Resolve an asterisk expression to the real column reference expressions.
     * @param ctx context
     * @param asteriskExpr asterisk expression
     * @return a list of NamedExpr each of which has ColumnReferenceExprs as its child
     * @throws TajoException
     */
    private static List<NamedExpr> resolveAsterisk(LogicalPlanner.PlanContext ctx, QualifiedAsteriskExpr asteriskExpr)
        throws TajoException {
      Column[] columns = getColumns(ctx, asteriskExpr);
      List<NamedExpr> newTargetExprs = new ArrayList<NamedExpr>(columns.length);
      int i;
      for (i = 0; i < columns.length; i++) {
        newTargetExprs.add(new NamedExpr(new ColumnReferenceExpr(columns[i].getQualifier(), columns[i].getSimpleName())));
      }
      return newTargetExprs;
    }

    private static NamedExpr [] voidResolveAsteriskNamedExpr(LogicalPlanner.PlanContext context,
                                                             NamedExpr [] namedExprs) throws TajoException {
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
        throws TajoException {
      SetSessionNode setSession = ctx.getPlan().createNode(SetSessionNode.class);
      return setSession;
    }

    @Override
    public LogicalNode visitProjection(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Projection expr)
        throws TajoException {
      // If Non-from statement, it immediately returns.
      if (!expr.hasChild()) {
        EvalExprNode exprNode = ctx.getPlan().createNode(EvalExprNode.class);
        exprNode.setTargets(buildTargets(ctx, expr.getNamedExprs()));
        return exprNode;
      }

      stack.push(expr); // <--- push
      LogicalNode child = visit(ctx, stack, expr.getChild());

      // Resolve the asterisk expression
      if (PlannerUtil.hasAsterisk(expr.getNamedExprs())) {
        expr.setNamedExprs(voidResolveAsteriskNamedExpr(ctx, expr.getNamedExprs()));
      }

      NamedExpr[] projectTargetExprs = expr.getNamedExprs();
      for (int i = 0; i < expr.getNamedExprs().length; i++) {
        NamedExpr namedExpr = projectTargetExprs[i];

        // 1) Normalize all field names occurred in each expr into full qualified names
        NameRefInSelectListNormalizer.normalize(ctx, namedExpr.getExpr());

        // 2) Register explicit column aliases to block
        if (namedExpr.getExpr().getType() == OpType.Column && namedExpr.hasAlias()) {
          ctx.getQueryBlock().addColumnAlias(((ColumnReferenceExpr)namedExpr.getExpr()).getCanonicalName(),
              namedExpr.getAlias());
        } else if (OpType.isLiteralType(namedExpr.getExpr().getType()) && namedExpr.hasAlias()) {
          Expr constExpr = namedExpr.getExpr();
          ConstEval constEval = (ConstEval) annotator.createEvalNode(ctx, constExpr, NameResolvingMode.RELS_ONLY, true);
          ctx.getQueryBlock().addConstReference(namedExpr.getAlias(), constExpr, constEval);
        }
      }

      Target[] targets = buildTargets(ctx, expr.getNamedExprs());

      stack.pop(); // <--- Pop

      ProjectionNode projectionNode = ctx.getPlan().createNode(ProjectionNode.class);
      projectionNode.setInSchema(child.getOutSchema());
      projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));

      ctx.getQueryBlock().setSchema(projectionNode.getOutSchema());
      return projectionNode;
    }

    private Target [] buildTargets(LogicalPlanner.PlanContext context, NamedExpr [] exprs) throws TajoException {
      Target [] targets = new Target[exprs.length];
      for (int i = 0; i < exprs.length; i++) {
        NamedExpr namedExpr = exprs[i];
        TajoDataTypes.DataType dataType = typeDeterminant.determineDataType(context, namedExpr.getExpr());

        if (namedExpr.hasAlias()) {
          targets[i] = new Target(new FieldEval(new Column(namedExpr.getAlias(), dataType)));
        } else {
          String generatedName = context.getPlan().generateUniqueColumnName(namedExpr.getExpr());
          targets[i] = new Target(new FieldEval(new Column(generatedName, dataType)));
        }
      }
      return targets;
    }

    @Override
    public LogicalNode visitLimit(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Limit expr)
        throws TajoException {
      stack.push(expr);
      LogicalNode child = visit(ctx, stack, expr.getChild());
      stack.pop();

      LimitNode limitNode = ctx.getPlan().createNode(LimitNode.class);
      limitNode.setInSchema(child.getOutSchema());
      limitNode.setOutSchema(child.getOutSchema());
      return limitNode;
    }

    @Override
    public LogicalNode visitSort(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Sort expr) throws TajoException {
      stack.push(expr);
      LogicalNode child = visit(ctx, stack, expr.getChild());
      stack.pop();

      SortNode sortNode = ctx.getPlan().createNode(SortNode.class);
      sortNode.setInSchema(child.getOutSchema());
      sortNode.setOutSchema(child.getOutSchema());
      return sortNode;
    }

    @Override
    public LogicalNode visitHaving(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Having expr)
        throws TajoException {
      stack.push(expr);
      LogicalNode child = visit(ctx, stack, expr.getChild());
      stack.pop();

      HavingNode havingNode = ctx.getPlan().createNode(HavingNode.class);
      havingNode.setInSchema(child.getOutSchema());
      havingNode.setOutSchema(child.getOutSchema());
      return havingNode;
    }

    @Override
    public LogicalNode visitGroupBy(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Aggregation expr)
        throws TajoException {
      stack.push(expr); // <--- push
      LogicalNode child = visit(ctx, stack, expr.getChild());

      Projection projection = ctx.getQueryBlock().getSingletonExpr(OpType.Projection);
      int finalTargetNum = projection.getNamedExprs().length;
      Target [] targets = new Target[finalTargetNum];

      if (PlannerUtil.hasAsterisk(projection.getNamedExprs())) {
        projection.setNamedExprs(voidResolveAsteriskNamedExpr(ctx, projection.getNamedExprs()));
      }

      for (int i = 0; i < finalTargetNum; i++) {
        NamedExpr namedExpr = projection.getNamedExprs()[i];
        EvalNode evalNode = annotator.createEvalNode(ctx, namedExpr.getExpr(), NameResolvingMode.SUBEXPRS_AND_RELS, true);

        if (namedExpr.hasAlias()) {
          targets[i] = new Target(evalNode, namedExpr.getAlias());
        } else {
          targets[i] = new Target(evalNode, "?name_" + i);
        }
      }
      stack.pop();

      GroupbyNode groupByNode = ctx.getPlan().createNode(GroupbyNode.class);
      groupByNode.setInSchema(child.getOutSchema());
      groupByNode.setOutSchema(PlannerUtil.targetToSchema(targets));
      return groupByNode;
    }

    @Override
    public LogicalNode visitUnion(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, SetOperation expr)
        throws TajoException {
      LogicalPlan.QueryBlock leftBlock = ctx.getPlan().newQueryBlock();
      LogicalPlanner.PlanContext leftContext = new LogicalPlanner.PlanContext(ctx, leftBlock);
      LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), expr.getLeft());
      leftBlock.setRoot(leftChild);
      ctx.getQueryBlock().registerExprWithNode(expr.getLeft(), leftChild);

      LogicalPlan.QueryBlock rightBlock = ctx.getPlan().newQueryBlock();
      LogicalPlanner.PlanContext rightContext = new LogicalPlanner.PlanContext(ctx, rightBlock);
      LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), expr.getRight());
      rightBlock.setRoot(rightChild);
      ctx.getQueryBlock().registerExprWithNode(expr.getRight(), rightChild);

      UnionNode unionNode = new UnionNode(ctx.getPlan().newPID());
      unionNode.setLeftChild(leftChild);
      unionNode.setRightChild(rightChild);
      unionNode.setInSchema(leftChild.getOutSchema());
      unionNode.setOutSchema(leftChild.getOutSchema());
      unionNode.setDistinct(expr.isDistinct());

      return unionNode;
    }

    public LogicalNode visitFilter(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Selection expr)
        throws TajoException {
      stack.push(expr);
      // Since filter push down will be done later, it is guaranteed that in-subqueries are found at only selection.
      for (Expr eachQual : PlannerUtil.extractInSubquery(expr.getQual())) {
        InPredicate inPredicate = (InPredicate) eachQual;
        stack.push(inPredicate);
        visit(ctx, stack, inPredicate.getRight());
        stack.pop();
      }
      LogicalNode child = visit(ctx, stack, expr.getChild());
      stack.pop();

      SelectionNode selectionNode = ctx.getPlan().createNode(SelectionNode.class);
      selectionNode.setInSchema(child.getOutSchema());
      selectionNode.setOutSchema(child.getOutSchema());
      return selectionNode;
    }

    @Override
    public LogicalNode visitJoin(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Join expr) throws TajoException {
      stack.push(expr);
      LogicalNode left = visit(ctx, stack, expr.getLeft());
      LogicalNode right = visit(ctx, stack, expr.getRight());
      stack.pop();
      JoinNode joinNode = ctx.getPlan().createNode(JoinNode.class);
      joinNode.setJoinType(expr.getJoinType());
      Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
      joinNode.setInSchema(merged);
      joinNode.setOutSchema(merged);

      ctx.getQueryBlock().addJoinType(expr.getJoinType());
      return joinNode;
    }

    @Override
    public LogicalNode visitRelation(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Relation relation)
        throws TajoException {

      String actualRelationName;
      if (CatalogUtil.isFQTableName(relation.getName())) {
        actualRelationName = relation.getName();
      } else {
        actualRelationName =
            CatalogUtil.buildFQName(ctx.getQueryContext().get(SessionVars.CURRENT_DATABASE), relation.getName());
      }

      TableDesc desc = catalog.getTableDesc(actualRelationName);

      ScanNode scanNode = ctx.getPlan().createNode(ScanNode.class);
      if (relation.hasAlias()) {
        scanNode.init(desc, relation.getAlias());
      } else {
        scanNode.init(desc);
      }

      TablePropertyUtil.setTableProperty(ctx.getQueryContext(), scanNode);

      ctx.getQueryBlock().addRelation(scanNode);
      return scanNode;
    }

    @Override
    public LogicalNode visitTableSubQuery(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
        throws TajoException {

      LogicalPlanner.PlanContext newContext;
      // Note: TableSubQuery always has a table name.
      // SELECT .... FROM (SELECT ...) TB_NAME <-
      QueryBlock queryBlock = ctx.getPlan().newQueryBlock();
      newContext = new LogicalPlanner.PlanContext(ctx, queryBlock);
      LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);
      queryBlock.setRoot(child);

      // a table subquery should be dealt as a relation.
      TableSubQueryNode node = ctx.getPlan().createNode(TableSubQueryNode.class);
      node.init(CatalogUtil.buildFQName(ctx.getQueryContext().get(SessionVars.CURRENT_DATABASE), expr.getName()), child);
      ctx.getQueryBlock().addRelation(node);

      return node;
    }

    @Override
    public LogicalNode visitSimpleTableSubquery(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, SimpleTableSubquery expr)
        throws TajoException {
      LogicalPlanner.PlanContext newContext;
      // Note: TableSubQuery always has a table name.
      // SELECT .... FROM (SELECT ...) TB_NAME <-
      QueryBlock queryBlock = ctx.getPlan().newQueryBlock();
      newContext = new LogicalPlanner.PlanContext(ctx, queryBlock);
      LogicalNode child = super.visitSimpleTableSubquery(newContext, stack, expr);
      queryBlock.setRoot(child);

      // a table subquery should be dealt as a relation.
      TableSubQueryNode node = ctx.getPlan().createNode(TableSubQueryNode.class);
      node.init(CatalogUtil.buildFQName(ctx.getQueryContext().get(SessionVars.CURRENT_DATABASE),
          ctx.generateUniqueSubQueryName()), child);
      ctx.getQueryBlock().addRelation(node);
      if (stack.peek().getType() == OpType.InPredicate) {
        // In-subquery and scalar subquery cannot be the base for name resolution.
        node.setNameResolveBase(false);
      }
      return node;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Data Definition Language Section
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public LogicalNode visitCreateDatabase(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CreateDatabase expr)
        throws TajoException {
      CreateDatabaseNode createDatabaseNode = ctx.getPlan().createNode(CreateDatabaseNode.class);
      return createDatabaseNode;
    }

    @Override
    public LogicalNode visitDropDatabase(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DropDatabase expr)
        throws TajoException {
      DropDatabaseNode dropDatabaseNode = ctx.getPlan().createNode(DropDatabaseNode.class);
      return dropDatabaseNode;
    }

    @Override
    public LogicalNode visitCreateTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CreateTable expr)
        throws TajoException {

      CreateTableNode createTableNode = ctx.getPlan().createNode(CreateTableNode.class);

      if (expr.hasSubQuery()) {
        stack.push(expr);
        visit(ctx, stack, expr.getSubQuery());
        stack.pop();
      }

      return createTableNode;
    }

    @Override
    public LogicalNode visitDropTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DropTable expr)
        throws TajoException {
      DropTableNode dropTable = ctx.getPlan().createNode(DropTableNode.class);
      return dropTable;
    }

    @Override
    public LogicalNode visitAlterTablespace(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, AlterTablespace expr)
        throws TajoException {
      AlterTablespaceNode alterTablespace = ctx.getPlan().createNode(AlterTablespaceNode.class);
      return alterTablespace;
    }

    @Override
    public LogicalNode visitAlterTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, AlterTable expr)
        throws TajoException {
      AlterTableNode alterTableNode = ctx.getPlan().createNode(AlterTableNode.class);
      return alterTableNode;
    }

    @Override
    public LogicalNode visitCreateIndex(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CreateIndex expr)
        throws TajoException {
      stack.push(expr);
      LogicalNode child = visit(ctx, stack, expr.getChild());
      stack.pop();

      CreateIndexNode createIndex = ctx.getPlan().createNode(CreateIndexNode.class);
      createIndex.setInSchema(child.getOutSchema());
      createIndex.setOutSchema(child.getOutSchema());
      return createIndex;
    }

    @Override
    public LogicalNode visitDropIndex(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DropIndex expr) {
      return ctx.getPlan().createNode(DropIndexNode.class);
    }

    public LogicalNode visitTruncateTable(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TruncateTable expr)
        throws TajoException {
      TruncateTableNode truncateTableNode = ctx.getPlan().createNode(TruncateTableNode.class);
      return truncateTableNode;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Insert or Update Section
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    public LogicalNode visitInsert(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, Insert expr)
        throws TajoException {
      LogicalNode child = super.visitInsert(ctx, stack, expr);

      InsertNode insertNode = new InsertNode(ctx.getPlan().newPID());
      insertNode.setInSchema(child.getOutSchema());
      insertNode.setOutSchema(child.getOutSchema());
      return insertNode;
    }

    public static class NameRefInSelectListNormalizer extends SimpleAlgebraVisitor<PlanContext, Object> {
      private static final NameRefInSelectListNormalizer instance;

      static {
        instance = new NameRefInSelectListNormalizer();
      }

      public static void normalize(LogicalPlanner.PlanContext context, Expr expr) throws TajoException {
        NameRefInSelectListNormalizer normalizer = new NameRefInSelectListNormalizer();
        normalizer.visit(context,new Stack<Expr>(), expr);
      }

      @Override
      public Expr visitColumnReference(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
          throws TajoException {

        String normalized = NameResolver.resolve(ctx.getPlan(), ctx.getQueryBlock(), expr,
            NameResolvingMode.RELS_ONLY, true).getQualifiedName();
        expr.setName(normalized);

        return expr;
      }
    }
  }
}
