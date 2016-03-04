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

import com.google.common.base.Objects;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.nameresolver.NameResolver;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.rewrite.BaseSchemaBuildPhase.Processor.NameRefInSelectListNormalizer;
import org.apache.tajo.plan.util.ExprFinder;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.graph.DirectedGraphVisitor;
import org.apache.tajo.util.graph.SimpleDirectedGraph;

import java.util.*;

/**
 * SelfDescSchemaBuildPhase builds the schema information of tables of self-describing data formats,
 * such as JSON, Parquet, and ORC.
 */
public class SelfDescSchemaBuildPhase extends LogicalPlanPreprocessPhase {

  private Processor processor;

  public SelfDescSchemaBuildPhase(CatalogService catalog, ExprAnnotator annotator) {
    super(catalog, annotator);
  }

  @Override
  public String getName() {
    return "Self-describing schema build phase";
  }

  private static String getQualifiedName(PlanContext context, String simpleName) {
    return CatalogUtil.isFQTableName(simpleName) ?
        simpleName :
        CatalogUtil.buildFQName(context.getQueryContext().get(SessionVars.CURRENT_DATABASE), simpleName);
  }

  @Override
  public boolean isEligible(PlanContext context, Expr expr) throws TajoException {
    Set<Relation> relations = ExprFinderIncludeSubquery.finds(expr, OpType.Relation);
    for (Relation eachRelation : relations) {
      TableDesc tableDesc = catalog.getTableDesc(getQualifiedName(context, eachRelation.getName()));
      if (tableDesc.hasEmptySchema()) {
        return true;
      }
    }
    return false;
  }

  static class FinderContext<T> {
    Set<T> set = new HashSet<>();
    OpType targetType;

    FinderContext(OpType type) {
      this.targetType = type;
    }
  }

  private static class ExprFinderIncludeSubquery extends SimpleAlgebraVisitor<FinderContext, Object> {

    public static <T extends Expr> Set<T> finds(Expr expr, OpType type) throws TajoException {
      FinderContext<T> context = new FinderContext<>(type);
      ExprFinderIncludeSubquery finder = new ExprFinderIncludeSubquery();
      finder.visit(context, new Stack<Expr>(), expr);
      return context.set;
    }

    @Override
    public Object visit(FinderContext ctx, Stack<Expr> stack, Expr expr) throws TajoException {
      if (expr instanceof Selection) {
        preHook(ctx, stack, expr);
        visit(ctx, stack, ((Selection) expr).getQual());
        visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
        postHook(ctx, stack, expr, null);
      } else if (expr instanceof UnaryOperator) {
        preHook(ctx, stack, expr);
        visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
        postHook(ctx, stack, expr, null);
      } else if (expr instanceof BinaryOperator) {
        preHook(ctx, stack, expr);
        visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
        postHook(ctx, stack, expr, null);
      } else if (expr instanceof SimpleTableSubquery) {
        preHook(ctx, stack, expr);
        visit(ctx, stack, ((SimpleTableSubquery) expr).getSubQuery());
        postHook(ctx, stack, expr, null);
      } else if (expr instanceof TablePrimarySubQuery) {
        preHook(ctx, stack, expr);
        visit(ctx, stack, ((TablePrimarySubQuery) expr).getSubQuery());
        postHook(ctx, stack, expr, null);
      } else {
        super.visit(ctx, stack, expr);
      }

      if (expr != null && ctx.targetType == expr.getType()) {
        ctx.set.add(expr);
      }

      return null;
    }
  }

  @Override
  public LogicalNode process(PlanContext context, Expr expr) throws TajoException {
    if (processor == null) {
      processor = new Processor();
    }
    return processor.visit(new ProcessorContext(context), new Stack<Expr>(), expr);
  }

  static class ProcessorContext {
    final PlanContext planContext;
    final Map<String, List<ColumnReferenceExpr>> projectColumns = new HashMap<>();
    final Set<String> aliasSet = new HashSet<>();

    public ProcessorContext(PlanContext planContext) {
      this.planContext = planContext;
    }
  }

  static class Processor extends BaseAlgebraVisitor<ProcessorContext, LogicalNode> {

    private static <T extends LogicalNode> T getNodeFromExpr(LogicalPlan plan, Expr expr) {
      return plan.getBlockByExpr(expr).getNodeFromExpr(expr);
    }

    private static <T extends LogicalNode> T getNonRelationListExpr(LogicalPlan plan, Expr expr) {
      if (expr instanceof RelationList) {
        return getNodeFromExpr(plan, ((RelationList) expr).getRelations()[0]);
      } else {
        return getNodeFromExpr(plan, expr);
      }
    }

    @Override
    public LogicalNode visitProjection(ProcessorContext ctx, Stack<Expr> stack, Projection expr) throws TajoException {
      if (PlannerUtil.hasAsterisk(expr.getNamedExprs())) {
        throw new UnsupportedException("Asterisk for self-describing data formats");
      }

      for (NamedExpr eachNamedExpr : expr.getNamedExprs()) {
        Set<ColumnReferenceExpr> columns = ExprFinder.finds(eachNamedExpr, OpType.Column);
        for (ColumnReferenceExpr col : columns) {
          TUtil.putToNestedList(ctx.projectColumns, col.getQualifier(), col);
        }
        if (eachNamedExpr.hasAlias()) {
          ctx.aliasSet.add(eachNamedExpr.getAlias());
        }
      }

      super.visitProjection(ctx, stack, expr);

      ProjectionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());

      return node;
    }

    @Override
    public LogicalNode visitLimit(ProcessorContext ctx, Stack<Expr> stack, Limit expr) throws TajoException {
      super.visitLimit(ctx, stack, expr);

      LimitNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitSort(ProcessorContext ctx, Stack<Expr> stack, Sort expr) throws TajoException {
      super.visitSort(ctx, stack, expr);

      SortNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitHaving(ProcessorContext ctx, Stack<Expr> stack, Having expr) throws TajoException {
      super.visitHaving(ctx, stack, expr);

      HavingNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitGroupBy(ProcessorContext ctx, Stack<Expr> stack, Aggregation expr) throws TajoException {
      super.visitGroupBy(ctx, stack, expr);

      GroupbyNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      return node;
    }

    @Override
    public LogicalNode visitJoin(ProcessorContext ctx, Stack<Expr> stack, Join expr) throws TajoException {
      super.visitJoin(ctx, stack, expr);

      JoinNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode leftChild = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getLeft());
      LogicalNode rightChild = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getRight());
      node.setInSchema(SchemaUtil.merge(leftChild.getOutSchema(), rightChild.getOutSchema()));
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitFilter(ProcessorContext ctx, Stack<Expr> stack, Selection expr) throws TajoException {
      Set<ColumnReferenceExpr> columnSet = ExprFinder.finds(expr.getQual(), OpType.Column);
      for (ColumnReferenceExpr col : columnSet) {
        if (!ctx.aliasSet.contains(col.getName())) {
          NameRefInSelectListNormalizer.normalize(ctx.planContext, col);
          TUtil.putToNestedList(ctx.projectColumns, col.getQualifier(), col);
        }
      }

      super.visitFilter(ctx, stack, expr);

      SelectionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitUnion(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitUnion(ctx, stack, expr);

      UnionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getLeft());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitExcept(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitExcept(ctx, stack, expr);

      ExceptNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getLeft());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitIntersect(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitIntersect(ctx, stack, expr);

      IntersectNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getLeft());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitSimpleTableSubquery(ProcessorContext ctx, Stack<Expr> stack, SimpleTableSubquery expr)
        throws TajoException {
      QueryBlock childBlock = ctx.planContext.getPlan().getBlock(
          ctx.planContext.getPlan().getBlockNameByExpr(expr.getSubQuery()));
      ProcessorContext newContext = new ProcessorContext(new PlanContext(ctx.planContext, childBlock));

      super.visitSimpleTableSubquery(newContext, stack, expr);

      TableSubQueryNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getSubQuery());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitTableSubQuery(ProcessorContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
        throws TajoException {
      QueryBlock childBlock = ctx.planContext.getPlan().getBlock(
          ctx.planContext.getPlan().getBlockNameByExpr(expr.getSubQuery()));
      ProcessorContext newContext = new ProcessorContext(new PlanContext(ctx.planContext, childBlock));

      super.visitTableSubQuery(newContext, stack, expr);

      TableSubQueryNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getSubQuery());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitCreateTable(ProcessorContext ctx, Stack<Expr> stack, CreateTable expr) throws TajoException {
      super.visitCreateTable(ctx, stack, expr);
      CreateTableNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);

      if (expr.hasSubQuery()) {
        LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getSubQuery());
        node.setInSchema(child.getOutSchema());
        node.setOutSchema(node.getInSchema());
      }
      return node;
    }

    @Override
    public LogicalNode visitInsert(ProcessorContext ctx, Stack<Expr> stack, Insert expr) throws TajoException {
      super.visitInsert(ctx, stack, expr);

      InsertNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getSubQuery());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitRelation(ProcessorContext ctx, Stack<Expr> stack, Relation expr) throws TajoException {
      LogicalPlan plan = ctx.planContext.getPlan();
      QueryBlock queryBlock = plan.getBlockByExpr(expr);
      ScanNode scan = queryBlock.getNodeFromExpr(expr);
      TableDesc desc = scan.getTableDesc();

      if (desc.hasEmptySchema()) {
        Set<Column> columns = new HashSet<>();
        if (ctx.projectColumns.containsKey(getQualifiedName(ctx.planContext, expr.getName()))) {
          for (ColumnReferenceExpr col : ctx.projectColumns.get(getQualifiedName(ctx.planContext, expr.getName()))) {
            columns.add(NameResolver.resolve(plan, queryBlock, col, NameResolvingMode.RELS_ONLY, true));
          }
        }

        if (expr.hasAlias()) {
          if (ctx.projectColumns.containsKey(getQualifiedName(ctx.planContext, expr.getAlias()))) {
            for (ColumnReferenceExpr col : ctx.projectColumns.get(getQualifiedName(ctx.planContext, expr.getAlias()))) {
              columns.add(NameResolver.resolve(plan, queryBlock, col, NameResolvingMode.RELS_ONLY, true));
            }
          }
        }

        if (columns.isEmpty()) {
          // error
          throw new TajoInternalError(
              "Columns projected from " + getQualifiedName(ctx.planContext, expr.getName()) + " is not found.");
        }

        desc.setSchema(buildSchemaFromColumnSet(columns));
        scan.init(desc);
      }

      return scan;
    }

    /**
     * This method creates a schema from a set of columns.
     * For a nested column, its ancestors are guessed and added to the schema.
     * For example, given a column 'glossary.title', the columns of (glossary RECORD (title TEXT)) will be added
     * to the schema.
     *
     * @param columns a set of columns
     * @return schema build from columns
     */
    private Schema buildSchemaFromColumnSet(Set<Column> columns) throws TajoException {
      SchemaGraph schemaGraph = new SchemaGraph();
      Set<ColumnVertex> rootVertexes = new HashSet<>();
      Schema schema = new Schema();

      Set<Column> simpleColumns = new HashSet<>();
      List<Column> columnList = new ArrayList<>(columns);
      Collections.sort(columnList, new Comparator<Column>() {
        @Override
        public int compare(Column c1, Column c2) {
          return c2.getSimpleName().split(NestedPathUtil.PATH_DELIMITER).length -
              c1.getSimpleName().split(NestedPathUtil.PATH_DELIMITER).length;
        }
      });

      for (Column eachColumn : columnList) {
        String simpleName = eachColumn.getSimpleName();
        if (NestedPathUtil.isPath(simpleName)) {
          String[] paths = simpleName.split(NestedPathUtil.PATH_DELIMITER);
          for (int i = 0; i < paths.length-1; i++) {
            String parentName = paths[i];
            if (i == 0) {
              parentName = CatalogUtil.buildFQName(eachColumn.getQualifier(), parentName);
            }
            // Leaf column type is TEXT; otherwise, RECORD.
            ColumnVertex childVertex = new ColumnVertex(
                paths[i+1],
                StringUtils.join(paths, NestedPathUtil.PATH_DELIMITER, 0, i+2),
                Type.RECORD
            );
            if (i == paths.length - 2 && !schemaGraph.hasVertex(childVertex)) {
              childVertex = new ColumnVertex(childVertex.name, childVertex.path, Type.TEXT);
            }

            ColumnVertex parentVertex = new ColumnVertex(
                parentName,
                StringUtils.join(paths, NestedPathUtil.PATH_DELIMITER, 0, i+1),
                Type.RECORD);
            schemaGraph.addEdge(new ColumnEdge(childVertex, parentVertex));
            if (i == 0) {
              rootVertexes.add(parentVertex);
            }
          }
        } else {
          simpleColumns.add(eachColumn);
        }
      }

      // Build record columns
      RecordColumnBuilder builder = new RecordColumnBuilder(schemaGraph);
      for (ColumnVertex eachRoot : rootVertexes) {
        schemaGraph.accept(null, eachRoot, builder);
        schema.addColumn(eachRoot.column);
      }

      // Add simple columns
      for (Column eachColumn : simpleColumns) {
        if (!schema.contains(eachColumn)) {
          schema.addColumn(eachColumn);
        }
      }

      return schema;
    }

    private static class ColumnVertex {
      private final String path;
      private final String name;
      private final Type type;
      private Column column;

      public ColumnVertex(String name, String path, Type type) {
        this.name = name;
        this.path = path;
        this.type = type;
      }

      @Override
      public boolean equals(Object o) {
        if (o instanceof ColumnVertex) {
          ColumnVertex other = (ColumnVertex) o;
          return this.name.equals(other.name) &&
              this.path.equals(other.path);
        }
        return false;
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(name, path);
      }
    }

    private static class ColumnEdge {
      private final ColumnVertex parent;
      private final ColumnVertex child;

      public ColumnEdge(ColumnVertex child, ColumnVertex parent) {
        this.child = child;
        this.parent = parent;
      }
    }

    private static class SchemaGraph extends SimpleDirectedGraph<ColumnVertex, ColumnEdge> {
      public void addEdge(ColumnEdge edge) {
        this.addEdge(edge.child, edge.parent, edge);
      }

      public boolean hasVertex(ColumnVertex vertex) {
        return this.directedEdges.containsKey(vertex) || this.reversedEdges.containsKey(vertex);
      }
    }

    private static class RecordColumnBuilder implements DirectedGraphVisitor<Object, ColumnVertex> {
      private final SchemaGraph graph;

      public RecordColumnBuilder(SchemaGraph graph) {
        this.graph = graph;
      }

      @Override
      public void visit(Object context, Stack<ColumnVertex> stack, ColumnVertex schemaVertex) {
        if (graph.isLeaf(schemaVertex)) {
          schemaVertex.column = new Column(schemaVertex.name, schemaVertex.type);
        } else {
          Schema schema = new Schema();
          for (ColumnVertex eachChild : graph.getChilds(schemaVertex)) {
            schema.addColumn(eachChild.column);
          }
          schemaVertex.column = new Column(schemaVertex.name, new TypeDesc(schema));
        }
      }
    }
  }
}
