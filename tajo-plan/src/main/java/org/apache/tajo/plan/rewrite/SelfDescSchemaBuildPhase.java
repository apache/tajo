package org.apache.tajo.plan.rewrite;

import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.logical.TableSubQueryNode;
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

    @Override
    public LogicalNode postHook(ProcessorContext ctx, Stack<Expr> stack, Expr expr, LogicalNode current)
        throws TajoException {
      QueryBlock queryBlock = ctx.planContext.getPlan().getBlockByExpr(expr);
      LogicalNode currentNode = queryBlock.getNodeFromExpr(expr);
      if (expr instanceof UnaryOperator) {
        Expr childExpr = ((UnaryOperator) expr).getChild();
        LogicalNode childNode = queryBlock.getNodeFromExpr(childExpr);
        currentNode.setInSchema(childNode.getOutSchema());
        currentNode.setOutSchema(currentNode.getInSchema());
      } else if (expr instanceof Join) {
        Expr leftChildExpr = ((Join) expr).getLeft();
        Expr rightChildExpr = ((Join) expr).getRight();
        LogicalNode leftChildNode = queryBlock.getNodeFromExpr(leftChildExpr);
        LogicalNode rightChildNode = queryBlock.getNodeFromExpr(rightChildExpr);
        currentNode.setInSchema(SchemaUtil.merge(leftChildNode.getOutSchema(), rightChildNode.getOutSchema()));
        currentNode.setOutSchema(currentNode.getInSchema());
      } else if (expr instanceof SetOperation) {
        Expr childExpr = ((SetOperation) expr).getLeft();
        LogicalNode childNode = queryBlock.getNodeFromExpr(childExpr);
        currentNode.setInSchema(childNode.getOutSchema());
        currentNode.setOutSchema(currentNode.getInSchema());
      } else if (expr instanceof TablePrimarySubQuery) {
        Expr subqueryExpr = ((TablePrimarySubQuery) expr).getSubQuery();
        QueryBlock childBlock = ctx.planContext.getPlan().getBlockByExpr(subqueryExpr);
        LogicalNode subqueryNode = childBlock.getNodeFromExpr(subqueryExpr);
        currentNode.setInSchema(subqueryNode.getOutSchema());
        currentNode.setOutSchema(currentNode.getInSchema());
      }

      return current;
    }

    @Override
    public LogicalNode visitProjection(ProcessorContext ctx, Stack<Expr> stack, Projection expr) throws TajoException {
      for (NamedExpr eachNamedExpr : expr.getNamedExprs()) {
        if (eachNamedExpr.getExpr() instanceof ColumnReferenceExpr) {
          ColumnReferenceExpr col = (ColumnReferenceExpr) eachNamedExpr.getExpr();
          TUtil.putToNestedList(ctx.projectColumns, col.getQualifier(), col);
        }
      }

      return null;
    }

    @Override
    public LogicalNode visitRelation(ProcessorContext ctx, Stack<Expr> stack, Relation expr) throws TajoException {
      LogicalPlan plan = ctx.planContext.getPlan();
      QueryBlock queryBlock = plan.getBlockByExpr(expr);
      ScanNode scan = queryBlock.getNodeFromExpr(expr);

      if (scan.getTableDesc().hasSelfDescSchema()) {
        if (ctx.projectColumns.containsKey(expr.getCanonicalName())) {
          Schema schema = new Schema();
          for (ColumnReferenceExpr col : ctx.projectColumns.get(expr.getCanonicalName())) {
            schema.addColumn(NameResolver.resolve(plan, queryBlock, col, NameResolvingMode.RELS_ONLY));
          }
          scan.setInSchema(schema);
          scan.setOutSchema(schema);
        } else {
          // error
          throw new RuntimeException("Error");
        }
      }

      return null;
    }
  }
}
