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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;

import java.util.Stack;

/**
 * It finds all relations for each block and builds base schema information.
 */
class LogicalPlanPreprocessor extends BaseAlgebraVisitor<LogicalPlanPreprocessor.PreprocessContext, LogicalNode> {
  private ExprAnnotator annotator;

  static class PreprocessContext {
    LogicalPlan plan;
    LogicalPlan.QueryBlock currentBlock;

    public PreprocessContext(LogicalPlan plan, LogicalPlan.QueryBlock currentBlock) {
      this.plan = plan;
      this.currentBlock = currentBlock;
    }

    public PreprocessContext(PreprocessContext context, LogicalPlan.QueryBlock currentBlock) {
      this.plan = context.plan;
      this.currentBlock = currentBlock;
    }
  }

  /** Catalog service */
  private CatalogService catalog;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
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

  @Override
  public LogicalNode visitProjection(PreprocessContext ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    // If Non-from statement, it immediately returns.
    if (!expr.hasChild()) {
      return new EvalExprNode(ctx.plan.newPID());
    }

    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Target [] targets;
    if (expr.isAllProjected()) {
      targets = PlannerUtil.schemaToTargets(child.getOutSchema());
    } else {
      targets = new Target[expr.getNamedExprs().length];

      for (int i = 0; i < expr.getNamedExprs().length; i++) {
        NamedExpr namedExpr = expr.getNamedExprs()[i];
        EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, namedExpr.getExpr());

        if (namedExpr.hasAlias()) {
          targets[i] = new Target(evalNode, namedExpr.getAlias());
        } else if (evalNode.getType() == EvalType.FIELD) {
          targets[i] = new Target((FieldEval) evalNode);
        } else {
          targets[i] = new Target(evalNode, "?name_" + i);
        }
      }
    }
    stack.pop(); // <--- Pop

    ProjectionNode projectionNode = new ProjectionNode(ctx.plan.newPID());
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return projectionNode;
  }

  @Override
  public LogicalNode visitLimit(PreprocessContext ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    LimitNode limitNode = new LimitNode(ctx.plan.newPID());
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());
    return limitNode;
  }

  @Override
  public LogicalNode visitSort(PreprocessContext ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SortNode sortNode = new SortNode(ctx.plan.newPID());
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    return sortNode;
  }

  @Override
  public LogicalNode visitHaving(PreprocessContext ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    HavingNode havingNode = new HavingNode(ctx.plan.newPID());
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
      EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, namedExpr.getExpr());

      if (namedExpr.hasAlias()) {
        targets[i] = new Target(evalNode, namedExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, "?name_" + i);
      }
    }
    stack.pop();

    GroupbyNode groupByNode = new GroupbyNode(ctx.plan.newPID());
    groupByNode.setInSchema(child.getOutSchema());
    groupByNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return groupByNode;
  }

  @Override
  public LogicalNode visitUnion(PreprocessContext ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    LogicalPlan.QueryBlock leftBlock = ctx.plan.newQueryBlock();
    PreprocessContext leftContext = new PreprocessContext(ctx, leftBlock);
    LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), expr.getLeft());
    ctx.currentBlock.registerExprWithNode(expr.getLeft(), leftChild);

    LogicalPlan.QueryBlock rightBlock = ctx.plan.newQueryBlock();
    PreprocessContext rightContext = new PreprocessContext(ctx, rightBlock);
    LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), expr.getRight());
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

    SelectionNode selectionNode = new SelectionNode(ctx.plan.newPID());
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
    JoinNode joinNode = new JoinNode(ctx.plan.newPID());
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
    TableDesc desc = catalog.getTableDesc(relation.getName());

    ScanNode scanNode;
    if (relation.hasAlias()) {
      scanNode = new ScanNode(ctx.plan.newPID(), desc, relation.getAlias());
    } else {
      scanNode = new ScanNode(ctx.plan.newPID(), desc);
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
    newContext = new PreprocessContext(ctx, ctx.plan.newAndGetBlock(expr.getName()));
    LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);

    // a table subquery should be dealt as a relation.
    TableSubQueryNode node = new TableSubQueryNode(ctx.plan.newPID(), expr.getName(), child);
    ctx.currentBlock.addRelation(node);
    return node;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public LogicalNode visitCreateTable(PreprocessContext ctx, Stack<Expr> stack, CreateTable expr)
      throws PlanningException {

    CreateTableNode createTableNode = new CreateTableNode(ctx.plan.newPID());

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
    DropTableNode dropTable = new DropTableNode(ctx.plan.newPID());
    return dropTable;
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
}
