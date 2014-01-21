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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.*;
import org.apache.tajo.algebra.CreateTable.ColumnDefinition;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.Specifier;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.LogicalPlan.QueryBlock;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

import static org.apache.tajo.algebra.Aggregation.GroupType;
import static org.apache.tajo.algebra.CreateTable.ColumnPartition;
import static org.apache.tajo.algebra.CreateTable.PartitionType;
import static org.apache.tajo.engine.planner.ExprNormalizer.ExprNormalizedResult;
import static org.apache.tajo.engine.planner.LogicalPlan.BlockType;
import static org.apache.tajo.engine.planner.LogicalPlanPreprocessor.PreprocessContext;

/**
 * This class creates a logical plan from a nested tajo algebra expression ({@link org.apache.tajo.algebra})
 */
public class LogicalPlanner extends BaseAlgebraVisitor<LogicalPlanner.PlanContext, LogicalNode> {
  private static Log LOG = LogFactory.getLog(LogicalPlanner.class);
  private final CatalogService catalog;
  private final LogicalPlanPreprocessor preprocessor;
  private final ExprAnnotator exprAnnotator;
  private final ExprNormalizer normalizer;

  public LogicalPlanner(CatalogService catalog) {
    this.catalog = catalog;
    this.exprAnnotator = new ExprAnnotator(catalog);
    this.preprocessor = new LogicalPlanPreprocessor(catalog, exprAnnotator);
    this.normalizer = new ExprNormalizer();
  }

  public class PlanContext {
    LogicalPlan plan;

    // transient data for each query block
    QueryBlock queryBlock;

    public PlanContext(LogicalPlan plan, QueryBlock block) {
      this.plan = plan;
      this.queryBlock = block;
    }

    public PlanContext(PlanContext context, QueryBlock block) {
      this.plan = context.plan;
      this.queryBlock = block;
    }

    public String toString() {
      return "block=" + queryBlock.getName() + ", relNum=" + queryBlock.getRelations().size() + ", "+
          queryBlock.namedExprsMgr.toString();
    }
  }

  /**
   * This generates a logical plan.
   *
   * @param expr A relational algebraic expression for a query.
   * @return A logical plan
   */
  public LogicalPlan createPlan(Expr expr) throws PlanningException {

    LogicalPlan plan = new LogicalPlan(this);

    QueryBlock rootBlock = plan.newAndGetBlock(LogicalPlan.ROOT_BLOCK);
    PreprocessContext preProcessorCtx = new PreprocessContext(plan, rootBlock);
    preprocessor.visit(preProcessorCtx, new Stack<Expr>(), expr);

    PlanContext context = new PlanContext(plan, plan.getRootBlock());
    LogicalNode topMostNode = this.visit(context, new Stack<Expr>(), expr);

    // Add Root Node
    LogicalRootNode root = new LogicalRootNode(plan.newPID());
    root.setInSchema(topMostNode.getOutSchema());
    root.setOutSchema(topMostNode.getOutSchema());
    root.setChild(topMostNode);
    plan.getRootBlock().setRoot(root);

    return plan;
  }

  public ExprAnnotator getExprAnnotator() {
    return this.exprAnnotator;
  }

  public void preHook(PlanContext context, Stack<Expr> stack, Expr expr) throws PlanningException {
    context.queryBlock.updateCurrentNode(expr);
  }

  public LogicalNode postHook(PlanContext context, Stack<Expr> stack, Expr expr, LogicalNode current)
      throws PlanningException {


    // Some generated logical nodes (e.g., implicit aggregation) without exprs will pass NULL as a expr parameter.
    // We should skip them.
    if (expr != null) {
      // A relation list including a single ScanNode will return a ScanNode instance that already passed postHook.
      // So, it skips the already-visited ScanNode instance.
      if (expr.getType() == OpType.RelationList && current.getType() == NodeType.SCAN) {
        return current;
      }
    }

    QueryBlock queryBlock = context.queryBlock;
    queryBlock.updateLatestNode(current);

    // if this node is the topmost
    if (stack.size() == 0) {
      queryBlock.setRoot(current);
    }

    if (!stack.empty()) {
      queryBlock.updateCurrentNode(stack.peek());
    }
    return current;
  }

  /*===============================================================================================
    Data Manupulation Language (DML) SECTION
   ===============================================================================================*/


  /*===============================================================================================
    PROJECTION SECTION
   ===============================================================================================*/
  @Override
  public LogicalNode visitProjection(PlanContext context, Stack<Expr> stack, Projection projection)
      throws PlanningException {


    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    // If a non-from statement is given
    if (!projection.hasChild()) {
      return buildPlanForNoneFromStatement(context, stack, projection);
    }

    String [] referenceNames;
    if (projection.isAllProjected()) {
      referenceNames = null;
    } else {
      // in prephase, insert all target list into NamedExprManagers.
      // Then it gets reference names, each of which points an expression in target list.
      referenceNames = doProjectionPrephase(context, projection);
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(projection);
    LogicalNode child = visit(context, stack, projection.getChild());

    // check if it is implicit aggregation. If so, it inserts group-by node to its child.
    if (block.isAggregationRequired()) {
      child = insertGroupbyNode(context, child, stack);
    }
    stack.pop();
    ////////////////////////////////////////////////////////

    ProjectionNode projectionNode;
    Target [] targets;
    if (projection.isAllProjected()) {
      // should takes all columns except for generated columns whose names are prefixed with '$'.
      targets = PlannerUtil.schemaToTargetsWithGeneratedFields(child.getOutSchema());
    } else {
      targets = buildTargets(plan, block, referenceNames);
    }

    // Set ProjectionNode
    projectionNode = context.queryBlock.getNodeFromExpr(projection);
    projectionNode.setTargets(targets);
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(projectionNode.getTargets()));
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setChild(child);

    if (projection.isDistinct() && block.hasNode(NodeType.GROUP_BY)) {
      throw new VerifyException("Cannot support grouping and distinct at the same time yet");
    } else {
      if (projection.isDistinct()) {
        insertDistinctOperator(context, projectionNode, child);
      }
    }

    // It's for debugging and unit tests purpose.
    // It sets raw targets, all of them are raw expressions instead of references.
    setRawTargets(context, targets, referenceNames, projection);

    return projectionNode;
  }

  private void setRawTargets(PlanContext context, Target[] targets, String[] referenceNames,
                             Projection projection) throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    if (projection.isAllProjected()) {
      block.setRawTargets(targets);
    } else {
      // It's for debugging or unit tests.
      Target [] rawTargets = new Target[projection.getNamedExprs().length];
      for (int i = 0; i < targets.length; i++) {
        NamedExpr namedExpr = projection.getNamedExprs()[i];
        EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr());
        rawTargets[i] = new Target(evalNode, referenceNames[i]);
      }
      // it's for debugging or unit testing
      block.setRawTargets(rawTargets);
    }
  }

  private void insertDistinctOperator(PlanContext context, ProjectionNode projectionNode, LogicalNode child) {
    LogicalPlan plan = context.plan;

    Schema outSchema = projectionNode.getOutSchema();
    GroupbyNode dupRemoval = new GroupbyNode(plan.newPID());
    dupRemoval.setGroupingColumns(outSchema.toArray());
    dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
    dupRemoval.setInSchema(projectionNode.getInSchema());
    dupRemoval.setOutSchema(outSchema);
    dupRemoval.setChild(child);
    projectionNode.setChild(dupRemoval);
    projectionNode.setInSchema(dupRemoval.getOutSchema());
  }

  private String [] doProjectionPrephase(PlanContext context, Projection projection) throws PlanningException {
    QueryBlock block = context.queryBlock;

    int finalTargetNum = projection.size();
    String [] referenceNames = new String[finalTargetNum];
    ExprNormalizedResult [] normalizedExprList = new ExprNormalizedResult[finalTargetNum];
    NamedExpr namedExpr;
    for (int i = 0; i < finalTargetNum; i++) {
      namedExpr = projection.getNamedExprs()[i];

      if (PlannerUtil.existsAggregationFunction(namedExpr)) {
        block.setAggregationRequire();
      }
      // dissect an expression into multiple parts (at most dissected into three parts)
      normalizedExprList[i] = normalizer.normalize(context, namedExpr.getExpr());
    }

    // Note: Why separate normalization and add(Named)Expr?
    //
    // ExprNormalizer internally makes use of the named exprs in NamedExprsManager.
    // If we don't separate normalization work and addExprWithName, addExprWithName will find named exprs evaluated
    // the same logical node. It will cause impossible evaluation in physical executors.
    for (int i = 0; i < finalTargetNum; i++) {
      namedExpr = projection.getNamedExprs()[i];
      // Get all projecting references
      if (namedExpr.hasAlias()) {
        NamedExpr aliasedExpr = new NamedExpr(normalizedExprList[i].baseExpr, namedExpr.getAlias());
        referenceNames[i] = block.namedExprsMgr.addNamedExpr(aliasedExpr);
      } else {
        referenceNames[i] = block.namedExprsMgr.addExpr(normalizedExprList[i].baseExpr);
      }

      // Add sub-expressions (i.e., aggregation part and scalar part) from dissected parts.
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].aggExprs);
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].scalarExprs);
    }

    return referenceNames;
  }

  /**
   * It builds non-from statement (only expressions) like '<code>SELECT 1+3 as plus</code>'.
   */
  private EvalExprNode buildPlanForNoneFromStatement(PlanContext context, Stack<Expr> stack, Projection projection)
      throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    int finalTargetNum = projection.getNamedExprs().length;
    Target [] targets = new Target[finalTargetNum];

    for (int i = 0; i < targets.length; i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
      EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr());
      if (namedExpr.hasAlias()) {
        targets[i] = new Target(evalNode, namedExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, context.plan.newGeneratedFieldName(namedExpr.getExpr()));
      }
    }
    EvalExprNode evalExprNode = context.queryBlock.getNodeFromExpr(projection);
    evalExprNode.setTargets(targets);
    evalExprNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    // it's for debugging or unit testing
    block.setRawTargets(targets);
    return evalExprNode;
  }

  private Target [] buildTargets(LogicalPlan plan, QueryBlock block, String[] referenceNames)
      throws PlanningException {
    Target [] targets = new Target[referenceNames.length];
    for (int i = 0; i < referenceNames.length; i++) {
      if (block.namedExprsMgr.isResolved(referenceNames[i])) {
        targets[i] = block.namedExprsMgr.getTarget(referenceNames[i]);
      } else {
        NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referenceNames[i]);
        EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr());
        block.namedExprsMgr.resolveExpr(referenceNames[i], evalNode);
        targets[i] = new Target(evalNode, referenceNames[i]);
      }
    }
    return targets;
  }

  /**
   * Insert a group-by operator before a sort or a projection operator.
   * It is used only when a group-by clause is not given.
   */
  private LogicalNode insertGroupbyNode(PlanContext context, LogicalNode child, Stack<Expr> stack)
      throws PlanningException {

    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;
    GroupbyNode groupbyNode = new GroupbyNode(plan.newPID());
    groupbyNode.setGroupingColumns(new Column[] {});

    Set<Target> evaluatedTargets = new LinkedHashSet<Target>();
    boolean includeDistinctFunction = false;
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getUnresolvedExprs(); it.hasNext();) {
      NamedExpr rawTarget = it.next();
      try {
        includeDistinctFunction = PlannerUtil.existsDistinctAggregationFunction(rawTarget.getExpr());
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr());
        if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
          block.namedExprsMgr.resolveExpr(rawTarget.getAlias(), evalNode);
          evaluatedTargets.add(new Target(evalNode, rawTarget.getAlias()));
        }
      } catch (VerifyException ve) {
      }
    }
    groupbyNode.setDistinct(includeDistinctFunction);
    groupbyNode.setTargets(evaluatedTargets.toArray(new Target[evaluatedTargets.size()]));
    groupbyNode.setChild(child);
    groupbyNode.setInSchema(child.getOutSchema());

    // this inserted group-by node doesn't pass through preprocessor. So manually added.
    block.registerNode(groupbyNode);
    postHook(context, stack, null, groupbyNode);
    return groupbyNode;
  }

  /*===============================================================================================
    SORT SECTION
  ===============================================================================================*/
  @Override
  public LimitNode visitLimit(PlanContext context, Stack<Expr> stack, Limit limit) throws PlanningException {
    QueryBlock block = context.queryBlock;

    EvalNode firstFetNum;
    LogicalNode child;
    if (limit.getFetchFirstNum().getType() == OpType.Literal) {
      firstFetNum = exprAnnotator.createEvalNode(context.plan, block, limit.getFetchFirstNum());

      ////////////////////////////////////////////////////////
      // Visit and Build Child Plan
      ////////////////////////////////////////////////////////
      stack.push(limit);
      child = visit(context, stack, limit.getChild());
      stack.pop();
      ////////////////////////////////////////////////////////
    } else {
      ExprNormalizedResult normalizedResult = normalizer.normalize(context, limit.getFetchFirstNum());
      String referName = block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
      block.namedExprsMgr.addNamedExprArray(normalizedResult.aggExprs);
      block.namedExprsMgr.addNamedExprArray(normalizedResult.scalarExprs);

      ////////////////////////////////////////////////////////
      // Visit and Build Child Plan
      ////////////////////////////////////////////////////////
      stack.push(limit);
      child = visit(context, stack, limit.getChild());
      stack.pop();
      ////////////////////////////////////////////////////////

      if (block.namedExprsMgr.isResolved(referName)) {
        firstFetNum = block.namedExprsMgr.getTarget(referName).getEvalTree();
      } else {
        NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referName);
        firstFetNum = exprAnnotator.createEvalNode(context.plan, block, namedExpr.getExpr());
        block.namedExprsMgr.resolveExpr(referName, firstFetNum);
      }
    }
    LimitNode limitNode = block.getNodeFromExpr(limit);
    limitNode.setChild(child);
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());

    limitNode.setFetchFirst(firstFetNum.terminate(null).asInt8());

    return limitNode;
  }

  @Override
  public SortNode visitSort(PlanContext context, Stack<Expr> stack, Sort sort) throws PlanningException {
    QueryBlock block = context.queryBlock;

    int sortKeyNum = sort.getSortSpecs().length;
    Sort.SortSpec[] sortSpecs = sort.getSortSpecs();
    String [] referNames = new String[sortKeyNum];

    ExprNormalizedResult [] normalizedExprList = new ExprNormalizedResult[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      normalizedExprList[i] = normalizer.normalize(context, sortSpecs[i].getKey());
    }
    for (int i = 0; i < sortKeyNum; i++) {
      referNames[i] = block.namedExprsMgr.addExpr(normalizedExprList[i].baseExpr);
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].aggExprs);
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].scalarExprs);
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(sort);
    LogicalNode child = visit(context, stack, sort.getChild());
    if (block.isAggregationRequired()) {
      child = insertGroupbyNode(context, child, stack);
    }
    stack.pop();
    ////////////////////////////////////////////////////////

    SortNode sortNode = block.getNodeFromExpr(sort);
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    sortNode.setChild(child);

    // Building sort keys
    Column column;
    SortSpec [] annotatedSortSpecs = new SortSpec[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      if (block.namedExprsMgr.isResolved(referNames[i])) {
        column = block.namedExprsMgr.getTarget(referNames[i]).getNamedColumn();
      } else {
        throw new IllegalStateException("Unexpected State: " + TUtil.arrayToString(sortSpecs));
      }
      annotatedSortSpecs[i] = new SortSpec(column, sortSpecs[i].isAscending(), sortSpecs[i].isNullFirst());
    }

    sortNode.setSortSpecs(annotatedSortSpecs);
    return sortNode;
  }

  /*===============================================================================================
    GROUP BY SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitHaving(PlanContext context, Stack<Expr> stack, Having expr) throws PlanningException {
    QueryBlock block = context.queryBlock;

    ExprNormalizedResult normalizedResult = normalizer.normalize(context, expr.getQual());
    String referName = block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
    block.namedExprsMgr.addNamedExprArray(normalizedResult.aggExprs);
    block.namedExprsMgr.addNamedExprArray(normalizedResult.scalarExprs);

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(expr);
    LogicalNode child = visit(context, stack, expr.getChild());
    stack.pop();
    ////////////////////////////////////////////////////////

    HavingNode having = new HavingNode(context.plan.newPID());
    having.setInSchema(child.getOutSchema());
    having.setOutSchema(child.getOutSchema());
    having.setChild(child);

    EvalNode havingCondition;
    if (block.namedExprsMgr.isResolved(referName)) {
      havingCondition = block.namedExprsMgr.getTarget(referName).getEvalTree();
    } else {
      NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referName);
      havingCondition = exprAnnotator.createEvalNode(context.plan, block, namedExpr.getExpr());
      block.namedExprsMgr.resolveExpr(referName, havingCondition);
    }

    // set having condition
    having.setQual(havingCondition);

    return having;
  }

  @Override
  public LogicalNode visitGroupBy(PlanContext context, Stack<Expr> stack, Aggregation aggregation)
      throws PlanningException {

    // Initialization Phase:
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    // Normalize grouping keys and add normalized grouping keys to NamedExprManager
    int groupingKeyNum = aggregation.getGroupSet()[0].getGroupingSets().length;
    ExprNormalizedResult [] normalizedResults = new ExprNormalizedResult[groupingKeyNum];
    for (int i = 0; i < groupingKeyNum; i++) {
      Expr groupingKey = aggregation.getGroupSet()[0].getGroupingSets()[i];
      normalizedResults[i] = normalizer.normalize(context, groupingKey);
    }
    String [] groupingKeyRefNames = new String[groupingKeyNum];
    for (int i = 0; i < groupingKeyNum; i++) {
      groupingKeyRefNames[i] = block.namedExprsMgr.addExpr(normalizedResults[i].baseExpr);
      block.namedExprsMgr.addNamedExprArray(normalizedResults[i].aggExprs);
      block.namedExprsMgr.addNamedExprArray(normalizedResults[i].scalarExprs);
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(aggregation);
    LogicalNode child = visit(context, stack, aggregation.getChild());
    stack.pop();
    ////////////////////////////////////////////////////////
    GroupbyNode groupingNode = context.queryBlock.getNodeFromExpr(aggregation);
    groupingNode.setChild(child);
    groupingNode.setInSchema(child.getOutSchema());


    // create EvalNodes and check if each EvalNode can be evaluated here.
    Set<Target> evaluatedTargets = new LinkedHashSet<Target>();
    boolean includeDistinctFunction = false;
    for (NamedExpr rawTarget : block.namedExprsMgr.getAllNamedExprs()) {
      try {
        includeDistinctFunction = PlannerUtil.existsDistinctAggregationFunction(rawTarget.getExpr());
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr());
        if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
          block.namedExprsMgr.resolveExpr(rawTarget.getAlias(), evalNode);
          evaluatedTargets.add(new Target(evalNode, rawTarget.getAlias()));
        }
      } catch (VerifyException ve) {
      }
    }
    // If there is at least one distinct aggregation function
    groupingNode.setDistinct(includeDistinctFunction);


    // Set grouping sets
    List<Target> targets = new ArrayList<Target>();
    Aggregation.GroupElement [] groupElements = aggregation.getGroupSet();

    // Currently, single ordinary grouping set is only available.
    if (groupElements[0].getType() == GroupType.OrdinaryGroup) {
      Column [] groupingColumns = new Column[aggregation.getGroupSet()[0].getGroupingSets().length];
      for (int i = 0; i < groupingColumns.length; i++) {
        if (block.namedExprsMgr.isResolved(groupingKeyRefNames[i])) {
          groupingColumns[i] = block.namedExprsMgr.getTarget(groupingKeyRefNames[i]).getNamedColumn();
        } else {
          throw new PlanningException("Each grouping column expression must be a scalar expression.");
        }
      }

      for (Column column : groupingColumns) {
        if (child.getOutSchema().contains(column)) {
          targets.add(new Target(new FieldEval(child.getOutSchema().getColumn(column))));
        }
      }
      groupingNode.setGroupingColumns(groupingColumns);
    } else {
      throw new InvalidQueryException("Not support grouping");
    }

    targets.addAll(evaluatedTargets);
    groupingNode.setTargets(targets.toArray(new Target[targets.size()]));
    block.unsetAggregationRequire();
    return groupingNode;
  }

  public static final Column[] ALL= Lists.newArrayList().toArray(new Column[0]);

  public static List<Column[]> generateCuboids(Column[] columns) {
    int numCuboids = (int) Math.pow(2, columns.length);
    int maxBits = columns.length;

    List<Column[]> cube = Lists.newArrayList();
    List<Column> cuboidCols;

    cube.add(ALL);
    for (int cuboidId = 1; cuboidId < numCuboids; cuboidId++) {
      cuboidCols = Lists.newArrayList();
      for (int j = 0; j < maxBits; j++) {
        int bit = 1 << j;
        if ((cuboidId & bit) == bit) {
          cuboidCols.add(columns[j]);
        }
      }
      cube.add(cuboidCols.toArray(new Column[cuboidCols.size()]));
    }
    return cube;
  }

  @Override
  public SelectionNode visitFilter(PlanContext context, Stack<Expr> stack, Selection selection)
      throws PlanningException {
    QueryBlock block = context.queryBlock;

    ExprNormalizedResult normalizedResult = normalizer.normalize(context, selection.getQual());
    block.namedExprsMgr.addReferences(normalizedResult.baseExpr);
    if (normalizedResult.aggExprs.size() > 0 || normalizedResult.scalarExprs.size() > 0) {
      throw new VerifyException("Filter condition cannot include aggregation function");
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(selection);
    LogicalNode child = visit(context, stack, selection.getChild());
    stack.pop();
    ////////////////////////////////////////////////////////

    SelectionNode selectionNode = context.queryBlock.getNodeFromExpr(selection);
    selectionNode.setChild(child);
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());

    // Create EvalNode for a search condition.
    EvalNode searchCondition = exprAnnotator.createEvalNode(context.plan, block, selection.getQual());
    EvalNode simplified = AlgebraicUtil.eliminateConstantExprs(searchCondition);
    // set selection condition
    selectionNode.setQual(simplified);

    return selectionNode;
  }

  /*===============================================================================================
    JOIN SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitJoin(PlanContext context, Stack<Expr> stack, Join join)
      throws PlanningException {
    // Phase 1: Init
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    if (join.hasQual()) {
      ExprNormalizedResult normalizedResult = normalizer.normalize(context, join.getQual());
      block.namedExprsMgr.addReferences(normalizedResult.baseExpr);
      if (normalizedResult.aggExprs.size() > 0 || normalizedResult.scalarExprs.size() > 0) {
        throw new VerifyException("Filter condition cannot include aggregation function");
      }
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(join);
    LogicalNode left = visit(context, stack, join.getLeft());
    LogicalNode right = visit(context, stack, join.getRight());
    stack.pop();
    ////////////////////////////////////////////////////////

    JoinNode joinNode = context.queryBlock.getNodeFromExpr(join);
    joinNode.setJoinType(join.getJoinType());
    joinNode.setLeftChild(left);
    joinNode.setRightChild(right);

    // Set A merged input schema
    Schema merged;
    if (join.isNatural()) {
      merged = getNaturalJoinSchema(left, right);
    } else {
      merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    }
    joinNode.setInSchema(merged);

    // Create EvalNode for a search condition.
    EvalNode joinCondition = null;
    if (join.hasQual()) {
      EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, block, join.getQual());
      joinCondition = AlgebraicUtil.eliminateConstantExprs(evalNode);
    }

    List<Expr> newlyEvaluatedExprs = getNewlyEvaluatedExprsForJoin(plan, block, joinNode, stack);
    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));

    for (Expr newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }
    joinNode.setTargets(targets.toArray(new Target[targets.size()]));

    // Determine join conditions
    if (join.isNatural()) { // if natural join, it should have the equi-join conditions by common column names
      EvalNode njCond = getNaturalJoinCondition(joinNode);
      joinNode.setJoinQual(njCond);
    } else if (join.hasQual()) { // otherwise, the given join conditions are set
      joinNode.setJoinQual(joinCondition);
    }

    return joinNode;
  }

  private List<Expr> getNewlyEvaluatedExprsForJoin(LogicalPlan plan, QueryBlock block, JoinNode joinNode,
                                                   Stack<Expr> stack) {
    EvalNode evalNode;
    List<Expr> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getUnresolvedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr());
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, evalNode, joinNode, stack.peek().getType() != OpType.Join)) {
          block.namedExprsMgr.resolveExpr(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getExpr());
        }
      } catch (VerifyException ve) {} catch (PlanningException e) {
        e.printStackTrace();
      }
    }
    return newlyEvaluatedExprs;
  }

  private static Schema getNaturalJoinSchema(LogicalNode left, LogicalNode right) {
    Schema joinSchema = new Schema();
    Schema commons = SchemaUtil.getNaturalJoinColumns(left.getOutSchema(), right.getOutSchema());
    joinSchema.addColumns(commons);
    for (Column c : left.getOutSchema().getColumns()) {
      if (!joinSchema.contains(c.getQualifiedName())) {
        joinSchema.addColumn(c);
      }
    }

    for (Column c : right.getOutSchema().getColumns()) {
      if (!joinSchema.contains(c.getQualifiedName())) {
        joinSchema.addColumn(c);
      }
    }
    return joinSchema;
  }

  private static EvalNode getNaturalJoinCondition(JoinNode joinNode) {
    Schema leftSchema = joinNode.getLeftChild().getInSchema();
    Schema rightSchema = joinNode.getRightChild().getInSchema();
    Schema commons = SchemaUtil.getNaturalJoinColumns(leftSchema, rightSchema);

    EvalNode njQual = null;
    EvalNode equiQual;
    Column leftJoinKey;
    Column rightJoinKey;

    for (Column common : commons.getColumns()) {
      leftJoinKey = leftSchema.getColumnByName(common.getColumnName());
      rightJoinKey = rightSchema.getColumnByName(common.getColumnName());
      equiQual = new BinaryEval(EvalType.EQUAL,
          new FieldEval(leftJoinKey), new FieldEval(rightJoinKey));
      if (njQual == null) {
        njQual = equiQual;
      } else {
        njQual = new BinaryEval(EvalType.AND, njQual, equiQual);
      }
    }

    return njQual;
  }

  private LogicalNode createCartesianProduct(PlanContext context, LogicalNode left, LogicalNode right)
      throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    EvalNode evalNode;
    List<Expr> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getUnresolvedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr());
        if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() == 0) {
          block.namedExprsMgr.resolveExpr(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getExpr());
        }
      } catch (VerifyException ve) {}
    }

    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));
    for (Expr newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }

    JoinNode join = new JoinNode(plan.newPID(), JoinType.CROSS, left, right);
    join.setTargets(targets.toArray(new Target[targets.size()]));
    join.setInSchema(merged);
    return join;
  }

  @Override
  public LogicalNode visitRelationList(PlanContext context, Stack<Expr> stack, RelationList relations)
      throws PlanningException {

    LogicalNode current = visit(context, stack, relations.getRelations()[0]);

    LogicalNode left;
    LogicalNode right;
    if (relations.size() > 1) {

      for (int i = 1; i < relations.size(); i++) {
        left = current;
        right = visit(context, stack, relations.getRelations()[i]);
        current = createCartesianProduct(context, left, right);
      }
    }
    context.queryBlock.registerNode(current);

    return current;
  }

  @Override
  public ScanNode visitRelation(PlanContext context, Stack<Expr> stack, Relation expr)
      throws PlanningException {
    QueryBlock block = context.queryBlock;

    ScanNode scanNode = block.getNodeFromExpr(expr);
    updatePhysicalInfo(scanNode.getTableDesc());

    // Add additional expressions required in upper nodes, such as sort key, grouping columns,
    // column references included in filter condition
    //
    // newlyEvaluatedExprsRef keeps
    Set<String> newlyEvaluatedExprsReferences = new LinkedHashSet<String>();
    for (NamedExpr rawTarget : block.namedExprsMgr.getAllNamedExprs()) {
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr());
        if (checkIfBeEvaluatedAtRelation(block, evalNode, scanNode)) {
          block.namedExprsMgr.resolveExpr(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprsReferences.add(rawTarget.getAlias()); // newly added exr
        }
      } catch (VerifyException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    List<Target> targets = new ArrayList<Target>();
    for (Column column : scanNode.getInSchema().getColumns()) {
      ColumnReferenceExpr columnRef = new ColumnReferenceExpr(column.getQualifier(), column.getColumnName());
      if (block.namedExprsMgr.contains(columnRef)) {
        String referenceName = block.namedExprsMgr.getName(columnRef);
        targets.add(new Target(new FieldEval(column), referenceName));
        newlyEvaluatedExprsReferences.remove(column.getQualifiedName());
      } else {
        targets.add(new Target(new FieldEval(column)));
      }
    }

    // The fact the some expr is included in newlyEvaluatedExprsReferences means that it is already resolved.
    // So, we get a raw expression and then creates a target.
    for (String reference : newlyEvaluatedExprsReferences) {
      NamedExpr refrrer = block.namedExprsMgr.getNamedExpr(reference);
      EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, block, refrrer.getExpr());
      targets.add(new Target(evalNode, reference));
    }

    scanNode.setTargets(targets.toArray(new Target[targets.size()]));

    return scanNode;
  }

  private void updatePhysicalInfo(TableDesc desc) {
    if (desc.getPath() != null) {
      try {
        FileSystem fs = desc.getPath().getFileSystem(new Configuration());
        FileStatus status = fs.getFileStatus(desc.getPath());
        if (desc.getStats() != null && (status.isDirectory() || status.isFile())) {
          ContentSummary summary = fs.getContentSummary(desc.getPath());
          if (summary != null) {
            long volume = summary.getLength();
            desc.getStats().setNumBytes(volume);
          }
        }
      } catch (Throwable t) {
        LOG.warn(t);
      }
    }
  }

  public TableSubQueryNode visitTableSubQuery(PlanContext context, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {
    QueryBlock block = context.queryBlock;

    QueryBlock childBlock = context.plan.getBlock(context.plan.getBlockNameByExpr(expr.getSubQuery()));
    PlanContext newContext = new PlanContext(context, childBlock);
    LogicalNode child = visit(newContext, new Stack<Expr>(), expr.getSubQuery());
    TableSubQueryNode subQueryNode = context.queryBlock.getNodeFromExpr(expr);
    context.plan.connectBlocks(childBlock, context.queryBlock, BlockType.TableSubQuery);
    subQueryNode.setSubQuery(child);

    // Add additional expressions required in upper nodes.
    Set<Expr> newlyEvaluatedExprs = new LinkedHashSet<Expr>();
    for (NamedExpr rawTarget : block.namedExprsMgr.getAllNamedExprs()) {
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr());
        if (checkIfBeEvaluatedAtRelation(block, evalNode, subQueryNode)) {
          block.namedExprsMgr.resolveExpr(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprs.add(rawTarget.getExpr()); // newly added exr
        }
      } catch (VerifyException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    List<Target> targets = new ArrayList<Target>();
    for (Column column : subQueryNode.getInSchema().getColumns()) {
      ColumnReferenceExpr columnRef = new ColumnReferenceExpr(column.getQualifier(), column.getColumnName());
      if (block.namedExprsMgr.contains(columnRef)) {
        String referenceName = block.namedExprsMgr.getName(columnRef);
        targets.add(new Target(new FieldEval(column), referenceName));
        newlyEvaluatedExprs.remove(columnRef);
      } else {
        targets.add(new Target(new FieldEval(column)));
      }
    }

    for (Expr newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }

    subQueryNode.setTargets(targets.toArray(new Target[targets.size()]));

    return subQueryNode;
  }

    /*===============================================================================================
    SET OPERATION SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitUnion(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws PlanningException {
    return buildSetPlan(context, stack, setOperation);
  }

  @Override
  public LogicalNode visitExcept(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws PlanningException {
    return buildSetPlan(context, stack, setOperation);
  }

  @Override
  public LogicalNode visitIntersect(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws PlanningException {
    return buildSetPlan(context, stack, setOperation);
  }

  private LogicalNode buildSetPlan(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws PlanningException {

    // 1. Init Phase
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    ////////////////////////////////////////////////////////
    // Visit and Build Left Child Plan
    ////////////////////////////////////////////////////////
    QueryBlock leftBlock = context.plan.getBlockByExpr(setOperation.getLeft());
    PlanContext leftContext = new PlanContext(context, leftBlock);
    stack.push(setOperation);
    LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), setOperation.getLeft());
    stack.pop();
    // Connect left child and current blocks
    context.plan.connectBlocks(leftContext.queryBlock, context.queryBlock, BlockType.TableSubQuery);

    ////////////////////////////////////////////////////////
    // Visit and Build Right Child Plan
    ////////////////////////////////////////////////////////
    QueryBlock rightBlock = context.plan.getBlockByExpr(setOperation.getRight());
    PlanContext rightContext = new PlanContext(context, rightBlock);
    stack.push(setOperation);
    LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), setOperation.getRight());
    stack.pop();
    // Connect right child and current blocks
    context.plan.connectBlocks(rightContext.queryBlock, context.queryBlock, BlockType.TableSubQuery);

    BinaryNode setOp;
    if (setOperation.getType() == OpType.Union) {
      setOp = new UnionNode(plan.newPID());
    } else if (setOperation.getType() == OpType.Except) {
      setOp = new ExceptNode(plan.newPID(), leftChild, rightChild);
    } else if (setOperation.getType() == OpType.Intersect) {
      setOp = new IntersectNode(plan.newPID(), leftChild, rightChild);
    } else {
      throw new VerifyException("Invalid Type: " + setOperation.getType());
    }
    setOp.setLeftChild(leftChild);
    setOp.setRightChild(rightChild);

    // An union statement can be derived from two query blocks.
    // For one union statement between both relations, we can ensure that each corresponding data domain of both
    // relations are the same. However, if necessary, the schema of left query block will be used as a base schema.
    Target [] leftStrippedTargets = PlannerUtil.stripTarget(
        PlannerUtil.schemaToTargets(leftBlock.getRoot().getOutSchema()));

    Schema outSchema = PlannerUtil.targetToSchema(leftStrippedTargets);
    setOp.setInSchema(leftChild.getOutSchema());
    setOp.setOutSchema(outSchema);

    return setOp;
  }

  /*===============================================================================================
    INSERT SECTION
   ===============================================================================================*/

  public LogicalNode visitInsert(PlanContext context, Stack<Expr> stack, Insert expr) throws PlanningException {
    stack.push(expr);
    QueryBlock newQueryBlock = context.plan.getBlockByExpr(expr.getSubQuery());
    PlanContext newContext = new PlanContext(context, newQueryBlock);
    Stack<Expr> subStack = new Stack<Expr>();
    LogicalNode subQuery = visit(newContext, subStack, expr.getSubQuery());
    context.plan.connectBlocks(newQueryBlock, context.queryBlock, BlockType.TableSubQuery);
    stack.pop();

    InsertNode insertNode = null;
    if (expr.hasTableName()) {
      TableDesc desc = catalog.getTableDesc(expr.getTableName());
      context.queryBlock.addRelation(new ScanNode(context.plan.newPID(), desc));

      Schema targetSchema = new Schema();
      if (expr.hasTargetColumns()) {
        // INSERT OVERWRITE INTO TABLE tbl(col1 type, col2 type) SELECT ...
        String[] targetColumnNames = expr.getTargetColumns();
        for (int i = 0; i < targetColumnNames.length; i++) {
          Column targetColumn = context.plan.resolveColumn(context.queryBlock,
              new ColumnReferenceExpr(targetColumnNames[i]));
          targetSchema.addColumn(targetColumn);
        }
      } else {
        // use the output schema of select clause as target schema
        // if didn't specific target columns like the way below,
        // INSERT OVERWRITE INTO TABLE tbl SELECT ...
        Schema targetTableSchema = desc.getSchema();
        for (int i = 0; i < subQuery.getOutSchema().getColumnNum(); i++) {
          targetSchema.addColumn(targetTableSchema.getColumn(i));
        }
      }

      insertNode = context.queryBlock.getNodeFromExpr(expr);
      insertNode.setTargetTableDesc(desc);
      insertNode.setSubQuery(subQuery);
      insertNode.setTargetSchema(targetSchema);
      insertNode.setOutSchema(targetSchema);
    }

    if (expr.hasLocation()) {
      insertNode = context.queryBlock.getNodeFromExpr(expr);
      insertNode.setTargetLocation(new Path(expr.getLocation()));
      insertNode.setSubQuery(subQuery);
      if (expr.hasStorageType()) {
        insertNode.setStorageType(CatalogUtil.getStoreType(expr.getStorageType()));
      }
      if (expr.hasParams()) {
        Options options = new Options();
        options.putAll(expr.getParams());
        insertNode.setOptions(options);
      }
    }

    insertNode.setOverwrite(expr.isOverwrite());

    return insertNode;
  }

  /*===============================================================================================
    Data Definition Language (DDL) SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitCreateTable(PlanContext context, Stack<Expr> stack, CreateTable expr)
      throws PlanningException {

    String tableName = expr.getTableName();

    if (expr.hasSubQuery()) {
      stack.add(expr);
      LogicalNode subQuery = visit(context, stack, expr.getSubQuery());
      stack.pop();
      StoreTableNode storeNode = new StoreTableNode(context.plan.newPID(), tableName);
      storeNode.setCreateTable();
      storeNode.setChild(subQuery);

      storeNode.setInSchema(subQuery.getOutSchema());
      if(!expr.hasTableElements()) {
        // CREATE TABLE tbl AS SELECT ...
        expr.setTableElements(convertSchemaToTableElements(subQuery.getOutSchema()));
      }
      // else CREATE TABLE tbl(col1 type, col2 type) AS SELECT ...
      storeNode.setOutSchema(convertTableElementsSchema(expr.getTableElements()));

      if (expr.hasStorageType()) {
        storeNode.setStorageType(CatalogUtil.getStoreType(expr.getStorageType()));
      } else {
        // default type
        storeNode.setStorageType(CatalogProtos.StoreType.CSV);
      }

      if (expr.hasParams()) {
        Options options = new Options();
        options.putAll(expr.getParams());
        storeNode.setOptions(options);
      }

      if (expr.hasPartition()) {
        storeNode.setPartitions(convertTableElementsPartition(context, expr));
      }

      return storeNode;
    } else {
      Schema tableSchema;
      boolean mergedPartition = false;
      if (expr.hasPartition()) {
        if (expr.getPartition().getPartitionType().equals(PartitionType.COLUMN)) {
          if (((ColumnPartition)expr.getPartition()).isOmitValues()) {
            mergedPartition = true;
          }
        } else {
          throw new PlanningException(String.format("Not supported PartitonType: %s",
              expr.getPartition().getPartitionType()));
        }
      }

      if (mergedPartition) {
        ColumnDefinition [] merged = TUtil.concat(expr.getTableElements(),
            ((ColumnPartition)expr.getPartition()).getColumns());
        tableSchema = convertTableElementsSchema(merged);
      } else {
        tableSchema = convertTableElementsSchema(expr.getTableElements());
      }

      CreateTableNode createTableNode = context.queryBlock.getNodeFromExpr(expr);
      createTableNode.setTableName(expr.getTableName());
      createTableNode.setSchema(tableSchema);

      if (expr.isExternal()) {
        createTableNode.setExternal(true);
      }

      if (expr.hasStorageType()) {
        createTableNode.setStorageType(CatalogUtil.getStoreType(expr.getStorageType()));
      } else {
        // default type
        // TODO - it should be configurable.
        createTableNode.setStorageType(CatalogProtos.StoreType.CSV);
      }

      if (expr.hasParams()) {
        Options options = new Options();
        options.putAll(expr.getParams());
        createTableNode.setOptions(options);
      }

      if (expr.hasLocation()) {
        createTableNode.setPath(new Path(expr.getLocation()));
      }

      if (expr.hasPartition()) {
        if (expr.getPartition().getPartitionType().equals(PartitionType.COLUMN)) {
          createTableNode.setPartitions(convertTableElementsPartition(context, expr));
        } else {
          throw new PlanningException(String.format("Not supported PartitonType: %s",
              expr.getPartition().getPartitionType()));
        }
      }

      return createTableNode;
    }
  }

  /**
   * convert table elements into Partition.
   *
   * @param context
   * @param expr
   * @return
   * @throws PlanningException
   */
  private PartitionDesc convertTableElementsPartition(PlanContext context,
                                                      CreateTable expr) throws PlanningException {
    Schema schema = convertTableElementsSchema(expr.getTableElements());
    PartitionDesc partitionDesc = null;
    List<Specifier> specifiers = null;
    if (expr.hasPartition()) {
      partitionDesc = new PartitionDesc();
      specifiers = TUtil.newList();

      partitionDesc.setPartitionsType(CatalogProtos.PartitionsType.valueOf(expr.getPartition()
          .getPartitionType().name()));

      if (expr.getPartition().getPartitionType().equals(PartitionType.HASH)) {
        CreateTable.HashPartition hashPartition = expr.getPartition();

        partitionDesc.setColumns(convertTableElementsColumns(expr.getTableElements()
            , hashPartition.getColumns()));

        if (hashPartition.getColumns() != null) {
          if (hashPartition.getQuantifier() != null) {
            String quantity = ((LiteralValue)hashPartition.getQuantifier()).getValue();
            partitionDesc.setNumPartitions(Integer.parseInt(quantity));
          }

          if (hashPartition.getSpecifiers() != null) {
            for(CreateTable.PartitionSpecifier eachSpec: hashPartition.getSpecifiers()) {
              specifiers.add(new Specifier(eachSpec.getName()));
            }
          }

          if (specifiers.isEmpty() && partitionDesc.getNumPartitions() > 0) {
            for (int i = 0; i < partitionDesc.getNumPartitions(); i++) {
              String partitionName = partitionDesc.getPartitionsType().name() + "_" + expr
                  .getTableName() + "_" + i;
              specifiers.add(new Specifier(partitionName));
            }
          }

          if (!specifiers.isEmpty())
            partitionDesc.setSpecifiers(specifiers);
        }
      } else if (expr.getPartition().getPartitionType().equals(PartitionType.LIST)) {
        CreateTable.ListPartition listPartition = expr.getPartition();

        partitionDesc.setColumns(convertTableElementsColumns(expr.getTableElements()
            , listPartition.getColumns()));

        if (listPartition.getSpecifiers() != null) {
          StringBuffer sb = new StringBuffer();

          for(CreateTable.ListPartitionSpecifier eachSpec: listPartition.getSpecifiers()) {
            Specifier specifier = new Specifier(eachSpec.getName());
            sb.delete(0, sb.length());
            for(Expr eachExpr : eachSpec.getValueList().getValues()) {
              context.queryBlock.setSchema(schema);
              EvalNode eval = exprAnnotator.createEvalNode(context.plan, context.queryBlock, eachExpr);
              if(sb.length() > 1)
                sb.append(",");

              sb.append(eval.toString());
            }
            specifier.setExpressions(sb.toString());
            specifiers.add(specifier);
          }
          if (!specifiers.isEmpty())
            partitionDesc.setSpecifiers(specifiers);
        }
      } else if (expr.getPartition().getPartitionType().equals(PartitionType.RANGE)) {
        CreateTable.RangePartition rangePartition = expr.getPartition();

        partitionDesc.setColumns(convertTableElementsColumns(expr.getTableElements()
            , rangePartition.getColumns()));

        if (rangePartition.getSpecifiers() != null) {
          for(CreateTable.RangePartitionSpecifier eachSpec: rangePartition.getSpecifiers()) {
            Specifier specifier = new Specifier();

            if (eachSpec.getName() != null)
              specifier.setName(eachSpec.getName());

            if (eachSpec.getEnd() != null) {
              context.queryBlock.setSchema(schema);
              EvalNode eval = exprAnnotator.createEvalNode(context.plan, context.queryBlock, eachSpec.getEnd());
              specifier.setExpressions(eval.toString());
            }

            if(eachSpec.isEndMaxValue()) {
              specifier.setExpressions(null);
            }
            specifiers.add(specifier);
          }
          if (!specifiers.isEmpty())
            partitionDesc.setSpecifiers(specifiers);
        }
      } else if (expr.getPartition().getPartitionType() == PartitionType.COLUMN) {
        ColumnPartition columnPartition = expr.getPartition();
        partitionDesc.setColumns(convertTableElementsSchema(columnPartition.getColumns()).getColumns());
        partitionDesc.setOmitValues(columnPartition.isOmitValues());
      }
    }

    return partitionDesc;
  }


  /**
   * It transforms table definition elements to schema.
   *
   * @param elements to be transformed
   * @return schema transformed from table definition elements
   */
  private Schema convertTableElementsSchema(CreateTable.ColumnDefinition[] elements) {
    Schema schema = new Schema();

    for (CreateTable.ColumnDefinition columnDefinition: elements) {
      schema.addColumn(convertColumn(columnDefinition));
    }

    return schema;
  }

  private ColumnDefinition[] convertSchemaToTableElements(Schema schema) {
    List<Column> columns = schema.getColumns();
    ColumnDefinition[] columnDefinitions = new ColumnDefinition[columns.size()];
    for(int i = 0; i < columns.size(); i ++) {
      Column col = columns.get(i);
      columnDefinitions[i] = new ColumnDefinition(col.getColumnName(), col.getDataType().getType().name());
    }

    return columnDefinitions;
  }

  private Collection<Column> convertTableElementsColumns(CreateTable.ColumnDefinition [] elements,
                                                         ColumnReferenceExpr[] references) {
    List<Column> columnList = TUtil.newList();

    for(CreateTable.ColumnDefinition columnDefinition: elements) {
      for(ColumnReferenceExpr eachReference: references) {
        if (columnDefinition.getColumnName().equalsIgnoreCase(eachReference.getName())) {
          columnList.add(convertColumn(columnDefinition));
        }
      }
    }

    return columnList;
  }

  private Column convertColumn(ColumnDefinition columnDefinition) {
    return new Column(columnDefinition.getColumnName(), convertDataType(columnDefinition));
  }

  static TajoDataTypes.DataType convertDataType(DataTypeExpr dataType) {
    TajoDataTypes.Type type = TajoDataTypes.Type.valueOf(dataType.getTypeName());

    TajoDataTypes.DataType.Builder builder = TajoDataTypes.DataType.newBuilder();
    builder.setType(type);
    if (dataType.hasLengthOrPrecision()) {
      builder.setLength(dataType.getLengthOrPrecision());
    }
    return builder.build();
  }


  @Override
  public LogicalNode visitDropTable(PlanContext context, Stack<Expr> stack, DropTable dropTable) {
    DropTableNode dropTableNode = context.queryBlock.getNodeFromExpr(dropTable);
    dropTableNode.set(dropTable.getTableName(), dropTable.isPurge());
    return dropTableNode;
  }

  /*===============================================================================================
    Util SECTION
  ===============================================================================================*/

  public static boolean checkIfBeEvaluatedAtGroupBy(EvalNode evalNode, GroupbyNode groupbyNode) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(evalNode);

    if (!groupbyNode.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    Set<String> tableIds = Sets.newHashSet();
    // getting distinct table references
    for (Column col : columnRefs) {
      if (!tableIds.contains(col.getQualifier())) {
        tableIds.add(col.getQualifier());
      }
    }

    if (tableIds.size() > 1) {
      return false;
    }

    return true;
  }

  public static boolean checkIfBeEvaluatedAtJoin(QueryBlock block, EvalNode evalNode, JoinNode joinNode,
                                                 boolean isTopMostJoin) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(evalNode);

    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    if (!joinNode.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    // When a 'case-when' is used with outer join, the case-when expression must be evaluated
    // at the topmost join operator.
    // TODO - It's also valid that case-when is evalauted at the topmost outer operator.
    //        But, how can we know there is no further outer join operator after this node?
    if (checkCaseWhenWithOuterJoin(block, evalNode, isTopMostJoin)) {
      return true;
    } else {
      return false;
    }
  }

  private static boolean checkCaseWhenWithOuterJoin(QueryBlock block, EvalNode evalNode, boolean isTopMostJoin) {
    if (block.containsJoinType(JoinType.LEFT_OUTER) || block.containsJoinType(JoinType.RIGHT_OUTER)) {
      Collection<CaseWhenEval> caseWhenEvals = EvalTreeUtil.findEvalsByType(evalNode, EvalType.CASE);
      if (caseWhenEvals.size() > 0 && !isTopMostJoin) {
        return false;
      }
    }
    return true;
  }

  public static boolean checkIfBeEvaluatedAtRelation(QueryBlock block, EvalNode evalNode, RelationNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(evalNode);

    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    if (node.getInSchema().containsAll(columnRefs)) {
      // Why? - When a {case when} is used with outer join, case when must be evaluated at topmost outer join.
      if (block.containsJoinType(JoinType.LEFT_OUTER) || block.containsJoinType(JoinType.RIGHT_OUTER)) {
        Collection<CaseWhenEval> found = EvalTreeUtil.findEvalsByType(evalNode, EvalType.CASE);
        if (found.size() > 0) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public static boolean checkIfBeEvaluateAtThis(EvalNode evalNode, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(evalNode);
    if (!node.getInSchema().containsAll(columnRefs)) {
      return false;
    }
    return true;
  }
}
