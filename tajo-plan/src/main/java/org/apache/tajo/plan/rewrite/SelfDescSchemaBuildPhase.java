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

import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.nameresolver.NameResolver;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.util.ExprFinder;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class SelfDescSchemaBuildPhase extends LogicalPlanPreprocessPhase {

  private Processor processor;

  public SelfDescSchemaBuildPhase(CatalogService catalog, ExprAnnotator annotator) {
    super(catalog, annotator);
  }

  @Override
  public String getName() {
    return "Self-describing schema build phase";
  }

  @Override
  public boolean isEligible(PlanContext context, Expr expr) throws TajoException {
    Set<Relation> relations = ExprFinder.finds(expr, OpType.Relation);
    for (Relation eachRelation : relations) {
      if (catalog.getTableDesc(eachRelation.getCanonicalName()).hasSelfDescSchema()) {
        return true;
      }
    }
    return false;
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

    public ProcessorContext(PlanContext planContext) {
      this.planContext = planContext;
    }
  }

  static class Processor extends BaseAlgebraVisitor<ProcessorContext, LogicalNode> {

//    @Override
//    public LogicalNode postHook(ProcessorContext ctx, Stack<Expr> stack, Expr expr, LogicalNode current)
//        throws TajoException {
//      QueryBlock queryBlock = ctx.planContext.getPlan().getBlockByExpr(expr);
//      LogicalNode currentNode = queryBlock.getNodeFromExpr(expr);
//      if (expr instanceof UnaryOperator) {
//        Expr childExpr = ((UnaryOperator) expr).getChild();
//        LogicalNode childNode = queryBlock.getNodeFromExpr(childExpr);
//        currentNode.setInSchema(childNode.getOutSchema());
//        currentNode.setOutSchema(currentNode.getInSchema());
//      } else if (expr instanceof Join) {
//        Expr leftChildExpr = ((Join) expr).getLeft();
//        Expr rightChildExpr = ((Join) expr).getRight();
//        LogicalNode leftChildNode = queryBlock.getNodeFromExpr(leftChildExpr);
//        LogicalNode rightChildNode = queryBlock.getNodeFromExpr(rightChildExpr);
//        currentNode.setInSchema(SchemaUtil.merge(leftChildNode.getOutSchema(), rightChildNode.getOutSchema()));
//        currentNode.setOutSchema(currentNode.getInSchema());
//      } else if (expr instanceof SetOperation) {
//        Expr childExpr = ((SetOperation) expr).getLeft();
//        LogicalNode childNode = queryBlock.getNodeFromExpr(childExpr);
//        currentNode.setInSchema(childNode.getOutSchema());
//        currentNode.setOutSchema(currentNode.getInSchema());
//      } else if (expr instanceof TablePrimarySubQuery) {
//        Expr subqueryExpr = ((TablePrimarySubQuery) expr).getSubQuery();
//        QueryBlock childBlock = ctx.planContext.getPlan().getBlockByExpr(subqueryExpr);
//        LogicalNode subqueryNode = childBlock.getNodeFromExpr(subqueryExpr);
//        currentNode.setInSchema(subqueryNode.getOutSchema());
//        currentNode.setOutSchema(currentNode.getInSchema());
//      }
//
//      return current;
//    }

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
      for (NamedExpr eachNamedExpr : expr.getNamedExprs()) {
        if (eachNamedExpr.getExpr() instanceof ColumnReferenceExpr) {
          ColumnReferenceExpr col = (ColumnReferenceExpr) eachNamedExpr.getExpr();
          TUtil.putToNestedList(ctx.projectColumns, col.getQualifier(), col);
        }
      }

      super.visitProjection(ctx, stack, expr);

      ProjectionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      LogicalNode child = getNonRelationListExpr(ctx.planContext.getPlan(), expr.getChild());
      node.setInSchema(child.getOutSchema());
      node.setOutSchema(node.getInSchema());

      return node;
    }

    @Override
    public LogicalNode visitLimit(ProcessorContext ctx, Stack<Expr> stack, Limit expr) throws TajoException {
      super.visitLimit(ctx, stack, expr);

      LimitNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitSort(ProcessorContext ctx, Stack<Expr> stack, Sort expr) throws TajoException {
      super.visitSort(ctx, stack, expr);

      SortNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitHaving(ProcessorContext ctx, Stack<Expr> stack, Having expr) throws TajoException {
      super.visitHaving(ctx, stack, expr);

      HavingNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitGroupBy(ProcessorContext ctx, Stack<Expr> stack, Aggregation expr) throws TajoException {
      super.visitGroupBy(ctx, stack, expr);

      GroupbyNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitJoin(ProcessorContext ctx, Stack<Expr> stack, Join expr) throws TajoException {
      super.visitJoin(ctx, stack, expr);

      JoinNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(SchemaUtil.merge(node.getLeftChild().getOutSchema(), node.getRightChild().getOutSchema()));
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitFilter(ProcessorContext ctx, Stack<Expr> stack, Selection expr) throws TajoException {
      super.visitFilter(ctx, stack, expr);

      SelectionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitUnion(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitUnion(ctx, stack, expr);

      UnionNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getLeftChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitExcept(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitExcept(ctx, stack, expr);

      ExceptNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getLeftChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitIntersect(ProcessorContext ctx, Stack<Expr> stack, SetOperation expr) throws TajoException {
      super.visitIntersect(ctx, stack, expr);

      IntersectNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getLeftChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitSimpleTableSubquery(ProcessorContext ctx, Stack<Expr> stack, SimpleTableSubquery expr)
        throws TajoException {
      super.visitSimpleTableSubquery(ctx, stack, expr);

      TableSubQueryNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getSubQuery().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitTableSubQuery(ProcessorContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
        throws TajoException {
      super.visitTableSubQuery(ctx, stack, expr);

      TableSubQueryNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getSubQuery().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitCreateTable(ProcessorContext ctx, Stack<Expr> stack, CreateTable expr) throws TajoException {
      super.visitCreateTable(ctx, stack, expr);
      CreateTableNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);

      if (expr.hasSubQuery()) {
        node.setInSchema(node.getChild().getOutSchema());
        node.setOutSchema(node.getInSchema());
      }
      return node;
    }

    @Override
    public LogicalNode visitInsert(ProcessorContext ctx, Stack<Expr> stack, Insert expr) throws TajoException {
      super.visitInsert(ctx, stack, expr);

      InsertNode node = getNodeFromExpr(ctx.planContext.getPlan(), expr);
      node.setInSchema(node.getChild().getOutSchema());
      node.setOutSchema(node.getInSchema());
      return node;
    }

    @Override
    public LogicalNode visitRelation(ProcessorContext ctx, Stack<Expr> stack, Relation expr) throws TajoException {
      LogicalPlan plan = ctx.planContext.getPlan();
      QueryBlock queryBlock = plan.getBlockByExpr(expr);
      ScanNode scan = queryBlock.getNodeFromExpr(expr);
      TableDesc desc = scan.getTableDesc();

      if (desc.hasSelfDescSchema()) {
        if (ctx.projectColumns.containsKey(expr.getCanonicalName())) {
          Schema schema = new Schema();
          for (ColumnReferenceExpr col : ctx.projectColumns.get(expr.getCanonicalName())) {
            schema.addColumn(NameResolver.resolve(plan, queryBlock, col, NameResolvingMode.RELS_ONLY));
          }
          desc.setSchema(schema);
          scan.init(desc);
//          scan.setInSchema(schema);
//          scan.setOutSchema(schema);
        } else {
          // error
          throw new RuntimeException("Error");
        }
      }

      return scan;
    }
  }
}
