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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
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
import org.apache.tajo.algebra.WindowSpec;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.LogicalPlan.QueryBlock;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.nameresolver.NameResolvingMode;
import org.apache.tajo.engine.planner.rewrite.ProjectionPushDownRule;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.util.*;

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

  public static class PlanContext {
    Session session;
    LogicalPlan plan;

    // transient data for each query block
    QueryBlock queryBlock;

    boolean debugOrUnitTests;

    public PlanContext(Session session, LogicalPlan plan, QueryBlock block, boolean debugOrUnitTests) {
      this.session = session;
      this.plan = plan;
      this.queryBlock = block;
      this.debugOrUnitTests = debugOrUnitTests;
    }

    public PlanContext(PlanContext context, QueryBlock block) {
      this.session = context.session;
      this.plan = context.plan;
      this.queryBlock = block;
      this.debugOrUnitTests = context.debugOrUnitTests;
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
  public LogicalPlan createPlan(Session session, Expr expr) throws PlanningException {
    return createPlan(session, expr, false);
  }

  @VisibleForTesting
  public LogicalPlan createPlan(Session session, Expr expr, boolean debug) throws PlanningException {

    LogicalPlan plan = new LogicalPlan(session.getCurrentDatabase(), this);

    QueryBlock rootBlock = plan.newAndGetBlock(LogicalPlan.ROOT_BLOCK);
    PreprocessContext preProcessorCtx = new PreprocessContext(session, plan, rootBlock);
    preprocessor.visit(preProcessorCtx, new Stack<Expr>(), expr);
    plan.resetGeneratedId();

    PlanContext context = new PlanContext(session, plan, plan.getRootBlock(), debug);
    LogicalNode topMostNode = this.visit(context, new Stack<Expr>(), expr);

    // Add Root Node
    LogicalRootNode root = plan.createNode(LogicalRootNode.class);
    root.setInSchema(topMostNode.getOutSchema());
    root.setChild(topMostNode);
    root.setOutSchema(topMostNode.getOutSchema());
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

  public LogicalNode visitExplain(PlanContext ctx, Stack<Expr> stack, Explain expr) throws PlanningException {
    ctx.plan.setExplain();
    return visit(ctx, stack, expr.getChild());
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
    // in prephase, insert all target list into NamedExprManagers.
    // Then it gets reference names, each of which points an expression in target list.
    Pair<String [], ExprNormalizer.WindowSpecReferences []> referencesPair = doProjectionPrephase(context, projection);
    referenceNames = referencesPair.getFirst();

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(projection);
    LogicalNode child = visit(context, stack, projection.getChild());

    // check if it is implicit aggregation. If so, it inserts group-by node to its child.
    if (block.isAggregationRequired()) {
      child = insertGroupbyNode(context, child, stack);
    }

    if (block.hasWindowSpecs()) {
      LogicalNode windowAggNode =
          insertWindowAggNode(context, child, stack, referenceNames, referencesPair.getSecond());
      if (windowAggNode != null) {
        child = windowAggNode;
      }
    }
    stack.pop();
    ////////////////////////////////////////////////////////

    ProjectionNode projectionNode;
    Target [] targets;
    targets = buildTargets(plan, block, referenceNames);

    // Set ProjectionNode
    projectionNode = context.queryBlock.getNodeFromExpr(projection);
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setTargets(targets);
    projectionNode.setChild(child);

    if (projection.isDistinct() && block.hasNode(NodeType.GROUP_BY)) {
      throw new VerifyException("Cannot support grouping and distinct at the same time yet");
    } else {
      if (projection.isDistinct()) {
        insertDistinctOperator(context, projectionNode, child, stack);
      }
    }

    // It's for debugging and unit tests purpose.
    // It sets raw targets, all of them are raw expressions instead of references.
    if (context.debugOrUnitTests) {
      setRawTargets(context, targets, referenceNames, projection);
    }

    verifyProjectedFields(block, projectionNode);
    return projectionNode;
  }

  private void setRawTargets(PlanContext context, Target[] targets, String[] referenceNames,
                             Projection projection) throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    // It's for debugging or unit tests.
    Target [] rawTargets = new Target[projection.getNamedExprs().length];
    for (int i = 0; i < projection.getNamedExprs().length; i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
      EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr(),
      NameResolvingMode.RELS_AND_SUBEXPRS);
      rawTargets[i] = new Target(evalNode, referenceNames[i]);
    }
    // it's for debugging or unit testing
    block.setRawTargets(rawTargets);
  }

  private void insertDistinctOperator(PlanContext context, ProjectionNode projectionNode, LogicalNode child,
                                      Stack<Expr> stack) throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    Schema outSchema = projectionNode.getOutSchema();
    GroupbyNode dupRemoval = context.plan.createNode(GroupbyNode.class);
    dupRemoval.setChild(child);
    dupRemoval.setInSchema(projectionNode.getInSchema());
    dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
    dupRemoval.setGroupingColumns(outSchema.toArray());

    block.registerNode(dupRemoval);
    postHook(context, stack, null, dupRemoval);

    projectionNode.setChild(dupRemoval);
    projectionNode.setInSchema(dupRemoval.getOutSchema());
  }

  private Pair<String [], ExprNormalizer.WindowSpecReferences []> doProjectionPrephase(PlanContext context,
                                                                                    Projection projection)
      throws PlanningException {

    QueryBlock block = context.queryBlock;

    int finalTargetNum = projection.size();
    String [] referenceNames = new String[finalTargetNum];
    ExprNormalizedResult [] normalizedExprList = new ExprNormalizedResult[finalTargetNum];

    List<ExprNormalizer.WindowSpecReferences> windowSpecReferencesList = TUtil.newList();

    List<Integer> targetsIds = normalize(context, projection, normalizedExprList, new Matcher() {
      @Override
      public boolean isMatch(Expr expr) {
        return ExprFinder.finds(expr, OpType.WindowFunction).size() == 0;
      }
    });

    // Note: Why separate normalization and add(Named)Expr?
    //
    // ExprNormalizer internally makes use of the named exprs in NamedExprsManager.
    // If we don't separate normalization work and addExprWithName, addExprWithName will find named exprs evaluated
    // the same logical node. It will cause impossible evaluation in physical executors.
    addNamedExprs(block, referenceNames, normalizedExprList, windowSpecReferencesList, projection, targetsIds);

    targetsIds = normalize(context, projection, normalizedExprList, new Matcher() {
      @Override
      public boolean isMatch(Expr expr) {
        return ExprFinder.finds(expr, OpType.WindowFunction).size() > 0;
      }
    });
    addNamedExprs(block, referenceNames, normalizedExprList, windowSpecReferencesList, projection, targetsIds);

    return new Pair<String[], ExprNormalizer.WindowSpecReferences []>(referenceNames,
        windowSpecReferencesList.toArray(new ExprNormalizer.WindowSpecReferences[windowSpecReferencesList.size()]));
  }

  private interface Matcher {
    public boolean isMatch(Expr expr);
  }

  public List<Integer> normalize(PlanContext context, Projection projection, ExprNormalizedResult [] normalizedExprList,
                                 Matcher matcher) throws PlanningException {
    List<Integer> targetIds = new ArrayList<Integer>();
    for (int i = 0; i < projection.size(); i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];

      if (PlannerUtil.existsAggregationFunction(namedExpr)) {
        context.queryBlock.setAggregationRequire();
      }

      if (matcher.isMatch(namedExpr.getExpr())) {
        // dissect an expression into multiple parts (at most dissected into three parts)
        normalizedExprList[i] = normalizer.normalize(context, namedExpr.getExpr());
        targetIds.add(i);
      }
    }

    return targetIds;
  }

  private void addNamedExprs(QueryBlock block, String [] referenceNames, ExprNormalizedResult [] normalizedExprList,
                             List<ExprNormalizer.WindowSpecReferences> windowSpecReferencesList, Projection projection,
                             List<Integer> targetIds) throws PlanningException {
    for (int i : targetIds) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
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
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].windowAggExprs);

      windowSpecReferencesList.addAll(normalizedExprList[i].windowSpecs);
    }
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
      EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr(), NameResolvingMode.RELS_ONLY);
      if (namedExpr.hasAlias()) {
        targets[i] = new Target(evalNode, namedExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, context.plan.generateUniqueColumnName(namedExpr.getExpr()));
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
      if (block.namedExprsMgr.isEvaluated(referenceNames[i])) {
        targets[i] = block.namedExprsMgr.getTarget(referenceNames[i]);
      } else {
        NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referenceNames[i]);
        EvalNode evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr(),
            NameResolvingMode.RELS_AND_SUBEXPRS);
        block.namedExprsMgr.markAsEvaluated(referenceNames[i], evalNode);
        targets[i] = new Target(evalNode, referenceNames[i]);
      }
    }
    return targets;
  }

  /**
   * It checks if all targets of Projectable plan node can be evaluated from the child node.
   * It can avoid potential errors which possibly occur in physical operators.
   *
   * @param block QueryBlock which includes the Projectable node
   * @param projectable Projectable node to be valid
   * @throws PlanningException
   */
  public static void verifyProjectedFields(QueryBlock block, Projectable projectable) throws PlanningException {
    if (projectable instanceof GroupbyNode) {
      GroupbyNode groupbyNode = (GroupbyNode) projectable;

      if (!groupbyNode.isEmptyGrouping()) { // it should be targets instead of
        int groupingKeyNum = groupbyNode.getGroupingColumns().length;

        for (int i = 0; i < groupingKeyNum; i++) {
          Target target = groupbyNode.getTargets()[i];
          if (groupbyNode.getTargets()[i].getEvalTree().getType() == EvalType.FIELD) {
            FieldEval grpKeyEvalNode = target.getEvalTree();
            if (!groupbyNode.getInSchema().contains(grpKeyEvalNode.getColumnRef())) {
              throwCannotEvaluateException(projectable, grpKeyEvalNode.getName());
            }
          }
        }
      }

      if (groupbyNode.hasAggFunctions()) {
        verifyIfEvalNodesCanBeEvaluated(projectable, groupbyNode.getAggFunctions());
      }

    } else if (projectable instanceof WindowAggNode) {
      WindowAggNode windowAggNode = (WindowAggNode) projectable;

      if (windowAggNode.hasPartitionKeys()) {
        verifyIfColumnCanBeEvaluated(projectable.getInSchema(), projectable, windowAggNode.getPartitionKeys());
      }

      if (windowAggNode.hasAggFunctions()) {
        verifyIfEvalNodesCanBeEvaluated(projectable, windowAggNode.getWindowFunctions());
      }

      if (windowAggNode.hasSortSpecs()) {
        Column [] sortKeys = PlannerUtil.sortSpecsToSchema(windowAggNode.getSortSpecs()).toArray();
        verifyIfColumnCanBeEvaluated(projectable.getInSchema(), projectable, sortKeys);
      }

      // verify targets except for function slots
      for (int i = 0; i < windowAggNode.getTargets().length - windowAggNode.getWindowFunctions().length; i++) {
        Target target = windowAggNode.getTargets()[i];
        Set<Column> columns = EvalTreeUtil.findUniqueColumns(target.getEvalTree());
        for (Column c : columns) {
          if (!windowAggNode.getInSchema().contains(c)) {
            throwCannotEvaluateException(projectable, c.getQualifiedName());
          }
        }
      }

    } else if (projectable instanceof RelationNode) {
      RelationNode relationNode = (RelationNode) projectable;
      verifyIfTargetsCanBeEvaluated(relationNode.getTableSchema(), (Projectable) relationNode);

    } else {
      verifyIfTargetsCanBeEvaluated(projectable.getInSchema(), projectable);
    }
  }

  public static void verifyIfEvalNodesCanBeEvaluated(Projectable projectable, EvalNode[] evalNodes)
      throws PlanningException {
    for (EvalNode e : evalNodes) {
      Set<Column> columns = EvalTreeUtil.findUniqueColumns(e);
      for (Column c : columns) {
        if (!projectable.getInSchema().contains(c)) {
          throwCannotEvaluateException(projectable, c.getQualifiedName());
        }
      }
    }
  }

  public static void verifyIfTargetsCanBeEvaluated(Schema baseSchema, Projectable projectable)
      throws PlanningException {
    for (Target target : projectable.getTargets()) {
      Set<Column> columns = EvalTreeUtil.findUniqueColumns(target.getEvalTree());
      for (Column c : columns) {
        if (!baseSchema.contains(c)) {
          throwCannotEvaluateException(projectable, c.getQualifiedName());
        }
      }
    }
  }

  public static void verifyIfColumnCanBeEvaluated(Schema baseSchema, Projectable projectable, Column [] columns)
      throws PlanningException {
    for (Column c : columns) {
      if (!baseSchema.contains(c)) {
        throwCannotEvaluateException(projectable, c.getQualifiedName());
      }
    }
  }

  public static void throwCannotEvaluateException(Projectable projectable, String columnName) throws PlanningException {
    if (projectable instanceof UnaryNode && ((UnaryNode) projectable).getChild().getType() == NodeType.GROUP_BY) {
      throw new PlanningException(columnName
          + " must appear in the GROUP BY clause or be used in an aggregate function at node ("
          + projectable.getPID() + ")");
    } else {
      throw new PlanningException(String.format("Cannot evaluate the field \"%s\" at node (%d)",
          columnName, projectable.getPID()));
    }
  }

  private LogicalNode insertWindowAggNode(PlanContext context, LogicalNode child, Stack<Expr> stack,
                                          String [] referenceNames,
                                          ExprNormalizer.WindowSpecReferences [] windowSpecReferenceses)
      throws PlanningException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;
    WindowAggNode windowAggNode = context.plan.createNode(WindowAggNode.class);
    if (child.getType() == NodeType.LIMIT) {
      LimitNode limitNode = (LimitNode) child;
      windowAggNode.setChild(limitNode.getChild());
      windowAggNode.setInSchema(limitNode.getChild().getOutSchema());
      limitNode.setChild(windowAggNode);
    } else if (child.getType() == NodeType.SORT) {
      SortNode sortNode = (SortNode) child;
      windowAggNode.setChild(sortNode.getChild());
      windowAggNode.setInSchema(sortNode.getChild().getOutSchema());
      sortNode.setChild(windowAggNode);
    } else {
      windowAggNode.setChild(child);
      windowAggNode.setInSchema(child.getOutSchema());
    }

    List<String> winFuncRefs = new ArrayList<String>();
    List<WindowFunctionEval> winFuncs = new ArrayList<WindowFunctionEval>();
    List<WindowSpec> rawWindowSpecs = Lists.newArrayList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr rawTarget = it.next();
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.WINDOW_FUNCTION) {
          winFuncRefs.add(rawTarget.getAlias());
          winFuncs.add((WindowFunctionEval) evalNode);
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);

          // TODO - Later, we also consider the possibility that a window function contains only a window name.
          rawWindowSpecs.add(((WindowFunctionExpr) (rawTarget.getExpr())).getWindowSpec());
        }
      } catch (VerifyException ve) {
      }
    }

    // we only consider one window definition.
    if (windowSpecReferenceses[0].hasPartitionKeys()) {
      Column [] partitionKeyColumns = new Column[windowSpecReferenceses[0].getPartitionKeys().length];
      int i = 0;
      for (String partitionKey : windowSpecReferenceses[0].getPartitionKeys()) {
        if (block.namedExprsMgr.isEvaluated(partitionKey)) {
          partitionKeyColumns[i++] = block.namedExprsMgr.getTarget(partitionKey).getNamedColumn();
        } else {
          throw new PlanningException("Each grouping column expression must be a scalar expression.");
        }
      }
      windowAggNode.setPartitionKeys(partitionKeyColumns);
    }

    SortSpec [][] sortGroups = new SortSpec[rawWindowSpecs.size()][];

    for (int winSpecIdx = 0; winSpecIdx < rawWindowSpecs.size(); winSpecIdx++) {
      WindowSpec spec = rawWindowSpecs.get(winSpecIdx);
      if (spec.hasOrderBy()) {
        Sort.SortSpec [] sortSpecs = spec.getSortSpecs();
        int sortNum = sortSpecs.length;
        String [] sortKeyRefNames = windowSpecReferenceses[winSpecIdx].getOrderKeys();
        SortSpec [] annotatedSortSpecs = new SortSpec[sortNum];

        Column column;
        for (int i = 0; i < sortNum; i++) {
          if (block.namedExprsMgr.isEvaluated(sortKeyRefNames[i])) {
            column = block.namedExprsMgr.getTarget(sortKeyRefNames[i]).getNamedColumn();
          } else {
            throw new IllegalStateException("Unexpected State: " + TUtil.arrayToString(sortSpecs));
          }
          annotatedSortSpecs[i] = new SortSpec(column, sortSpecs[i].isAscending(), sortSpecs[i].isNullFirst());
        }

        sortGroups[winSpecIdx] = annotatedSortSpecs;
      } else {
        sortGroups[winSpecIdx] = null;
      }
    }

    for (int i = 0; i < winFuncRefs.size(); i++) {
      WindowFunctionEval winFunc = winFuncs.get(i);
      if (sortGroups[i] != null) {
        winFunc.setSortSpecs(sortGroups[i]);
      }
    }

    Target [] targets = new Target[referenceNames.length];
    List<Integer> windowFuncIndices = Lists.newArrayList();
    Projection projection = (Projection) stack.peek();
    int windowFuncIdx = 0;
    for (NamedExpr expr : projection.getNamedExprs()) {
      if (expr.getExpr().getType() == OpType.WindowFunction) {
        windowFuncIndices.add(windowFuncIdx);
      }
      windowFuncIdx++;
    }
    windowAggNode.setWindowFunctions(winFuncs.toArray(new WindowFunctionEval[winFuncs.size()]));

    int targetIdx = 0;
    for (int i = 0; i < referenceNames.length ; i++) {
      if (!windowFuncIndices.contains(i)) {
        targets[targetIdx++] = block.namedExprsMgr.getTarget(referenceNames[i]);
      }
    }
    for (int i = 0; i < winFuncRefs.size(); i++) {
      targets[targetIdx++] = block.namedExprsMgr.getTarget(winFuncRefs.get(i));
    }
    windowAggNode.setTargets(targets);
    verifyProjectedFields(block, windowAggNode);

    block.registerNode(windowAggNode);
    postHook(context, stack, null, windowAggNode);

    if (child.getType() == NodeType.LIMIT) {
      LimitNode limitNode = (LimitNode) child;
      limitNode.setInSchema(windowAggNode.getOutSchema());
      limitNode.setOutSchema(windowAggNode.getOutSchema());
      return null;
    } else if (child.getType() == NodeType.SORT) {
      SortNode sortNode = (SortNode) child;
      sortNode.setInSchema(windowAggNode.getOutSchema());
      sortNode.setOutSchema(windowAggNode.getOutSchema());
      return null;
    } else {
      return windowAggNode;
    }
  }

  /**
   * Insert a group-by operator before a sort or a projection operator.
   * It is used only when a group-by clause is not given.
   */
  private LogicalNode insertGroupbyNode(PlanContext context, LogicalNode child, Stack<Expr> stack)
      throws PlanningException {

    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;
    GroupbyNode groupbyNode = context.plan.createNode(GroupbyNode.class);
    groupbyNode.setChild(child);
    groupbyNode.setInSchema(child.getOutSchema());

    groupbyNode.setGroupingColumns(new Column[] {});

    Set<String> aggEvalNames = new LinkedHashSet<String>();
    Set<AggregationFunctionCallEval> aggEvals = new LinkedHashSet<AggregationFunctionCallEval>();
    boolean includeDistinctFunction = false;
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr rawTarget = it.next();
      try {
        // check if at least distinct aggregation function
        includeDistinctFunction |= PlannerUtil.existsDistinctAggregationFunction(rawTarget.getExpr());
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.AGG_FUNCTION) {
          aggEvalNames.add(rawTarget.getAlias());
          aggEvals.add((AggregationFunctionCallEval) evalNode);
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
        }
      } catch (VerifyException ve) {
      }
    }

    groupbyNode.setDistinct(includeDistinctFunction);
    groupbyNode.setAggFunctions(aggEvals.toArray(new AggregationFunctionCallEval[aggEvals.size()]));
    Target [] targets = ProjectionPushDownRule.buildGroupByTarget(groupbyNode, null,
        aggEvalNames.toArray(new String[aggEvalNames.size()]));
    groupbyNode.setTargets(targets);

    // this inserted group-by node doesn't pass through preprocessor. So manually added.
    block.registerNode(groupbyNode);
    postHook(context, stack, null, groupbyNode);

    verifyProjectedFields(block, groupbyNode);
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
      firstFetNum = exprAnnotator.createEvalNode(context.plan, block, limit.getFetchFirstNum(),
          NameResolvingMode.RELS_ONLY);

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

      if (block.namedExprsMgr.isEvaluated(referName)) {
        firstFetNum = block.namedExprsMgr.getTarget(referName).getEvalTree();
      } else {
        NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referName);
        firstFetNum = exprAnnotator.createEvalNode(context.plan, block, namedExpr.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        block.namedExprsMgr.markAsEvaluated(referName, firstFetNum);
      }
    }
    LimitNode limitNode = block.getNodeFromExpr(limit);
    limitNode.setChild(child);
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());

    limitNode.setFetchFirst(firstFetNum.eval(null, null).asInt8());

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
    sortNode.setChild(child);
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());


    // Building sort keys
    Column column;
    SortSpec [] annotatedSortSpecs = new SortSpec[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      if (block.namedExprsMgr.isEvaluated(referNames[i])) {
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

    HavingNode having = context.queryBlock.getNodeFromExpr(expr);
    having.setChild(child);
    having.setInSchema(child.getOutSchema());
    having.setOutSchema(child.getOutSchema());

    EvalNode havingCondition;
    if (block.namedExprsMgr.isEvaluated(referName)) {
      havingCondition = block.namedExprsMgr.getTarget(referName).getEvalTree();
    } else {
      NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(referName);
      havingCondition = exprAnnotator.createEvalNode(context.plan, block, namedExpr.getExpr(),
          NameResolvingMode.SUBEXPRS_AND_RELS);
      block.namedExprsMgr.markAsEvaluated(referName, havingCondition);
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

    // Set grouping sets
    Column [] groupingColumns = new Column[aggregation.getGroupSet()[0].getGroupingSets().length];
    for (int i = 0; i < groupingColumns.length; i++) {
      if (block.namedExprsMgr.isEvaluated(groupingKeyRefNames[i])) {
        groupingColumns[i] = block.namedExprsMgr.getTarget(groupingKeyRefNames[i]).getNamedColumn();
      } else {
        throw new PlanningException("Each grouping column expression must be a scalar expression.");
      }
    }
    groupingNode.setGroupingColumns(groupingColumns);

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////

    // create EvalNodes and check if each EvalNode can be evaluated here.
    List<String> aggEvalNames = TUtil.newList();
    List<AggregationFunctionCallEval> aggEvalNodes = TUtil.newList();
    boolean includeDistinctFunction = false;
    for (Iterator<NamedExpr> iterator = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); iterator.hasNext();) {
      NamedExpr namedExpr = iterator.next();
      try {
        includeDistinctFunction |= PlannerUtil.existsDistinctAggregationFunction(namedExpr.getExpr());
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, namedExpr.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.AGG_FUNCTION) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          aggEvalNames.add(namedExpr.getAlias());
          aggEvalNodes.add((AggregationFunctionCallEval) evalNode);
        }
      } catch (VerifyException ve) {
      }
    }
    // if there is at least one distinct aggregation function
    groupingNode.setDistinct(includeDistinctFunction);
    groupingNode.setAggFunctions(aggEvalNodes.toArray(new AggregationFunctionCallEval[aggEvalNodes.size()]));

    Target [] targets = new Target[groupingKeyNum + aggEvalNames.size()];

    // In target, grouping columns will be followed by aggregation evals.
    //
    // col1, col2, col3,   sum(..),  agv(..)
    // ^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^
    //  grouping keys      aggregation evals

    // Build grouping keys
    for (int i = 0; i < groupingKeyNum; i++) {
      Target target = block.namedExprsMgr.getTarget(groupingNode.getGroupingColumns()[i].getQualifiedName());
      targets[i] = target;
    }

    for (int i = 0, targetIdx = groupingKeyNum; i < aggEvalNodes.size(); i++, targetIdx++) {
      targets[targetIdx] = block.namedExprsMgr.getTarget(aggEvalNames.get(i));
    }

    groupingNode.setTargets(targets);
    block.unsetAggregationRequire();

    verifyProjectedFields(block, groupingNode);
    return groupingNode;
  }

  private static final Column[] ALL= Lists.newArrayList().toArray(new Column[0]);

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
    block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
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
    EvalNode searchCondition = exprAnnotator.createEvalNode(context.plan, block, selection.getQual(),
        NameResolvingMode.RELS_AND_SUBEXPRS);
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
      ExprNormalizedResult normalizedResult = normalizer.normalize(context, join.getQual(), true);
      block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
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
      EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, block, join.getQual(),
          NameResolvingMode.LEGACY);
      joinCondition = AlgebraicUtil.eliminateConstantExprs(evalNode);
    }

    List<String> newlyEvaluatedExprs = getNewlyEvaluatedExprsForJoin(plan, block, joinNode, stack);
    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));

    for (String newAddedExpr : newlyEvaluatedExprs) {
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

  private List<String> getNewlyEvaluatedExprsForJoin(LogicalPlan plan, QueryBlock block, JoinNode joinNode,
                                                   Stack<Expr> stack) {
    EvalNode evalNode;
    List<String> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr(), NameResolvingMode.LEGACY);
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, evalNode, joinNode, stack.peek().getType() != OpType.Join)) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getAlias());
        }
      } catch (VerifyException ve) {
      } catch (PlanningException e) {
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
      leftJoinKey = leftSchema.getColumn(common.getQualifiedName());
      rightJoinKey = rightSchema.getColumn(common.getQualifiedName());
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

    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    JoinNode join = plan.createNode(JoinNode.class);
    join.init(JoinType.CROSS, left, right);
    join.setInSchema(merged);

    EvalNode evalNode;
    List<String> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(plan, block, namedExpr.getExpr(), NameResolvingMode.LEGACY);
        if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() == 0) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getAlias());
        }
      } catch (VerifyException ve) {}
    }

    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));
    for (String newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }
    join.setTargets(targets.toArray(new Target[targets.size()]));
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

    // Find expression which can be evaluated at this relation node.
    // Except for column references, additional expressions used in select list, where clause, order-by clauses
    // can be evaluated here. Their reference names are kept in newlyEvaluatedExprsRef.
    Set<String> newlyEvaluatedExprsReferences = new LinkedHashSet<String>();
    for (Iterator<NamedExpr> iterator = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); iterator.hasNext();) {
      NamedExpr rawTarget = iterator.next();
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr(),
            NameResolvingMode.RELS_ONLY);
        if (checkIfBeEvaluatedAtRelation(block, evalNode, scanNode)) {
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprsReferences.add(rawTarget.getAlias()); // newly added exr
        }
      } catch (VerifyException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    LinkedHashSet<Target> targets = createFieldTargetsFromRelation(block, scanNode, newlyEvaluatedExprsReferences);

    // The fact the some expr is included in newlyEvaluatedExprsReferences means that it is already evaluated.
    // So, we get a raw expression and then creates a target.
    for (String reference : newlyEvaluatedExprsReferences) {
      NamedExpr refrer = block.namedExprsMgr.getNamedExpr(reference);
      EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, block, refrer.getExpr(),
          NameResolvingMode.RELS_ONLY);
      targets.add(new Target(evalNode, reference));
    }

    scanNode.setTargets(targets.toArray(new Target[targets.size()]));

    verifyProjectedFields(block, scanNode);
    return scanNode;
  }

  private static LinkedHashSet<Target> createFieldTargetsFromRelation(QueryBlock block, RelationNode relationNode,
                                                      Set<String> newlyEvaluatedRefNames) {
    LinkedHashSet<Target> targets = Sets.newLinkedHashSet();
    for (Column column : relationNode.getTableSchema().getColumns()) {
      String aliasName = block.namedExprsMgr.checkAndGetIfAliasedColumn(column.getQualifiedName());
      if (aliasName != null) {
        targets.add(new Target(new FieldEval(column), aliasName));
        newlyEvaluatedRefNames.remove(aliasName);
      } else {
        targets.add(new Target(new FieldEval(column)));
      }
    }
    return targets;
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
    Set<String> newlyEvaluatedExprs = TUtil.newHashSet();
    for (NamedExpr rawTarget : block.namedExprsMgr.getAllNamedExprs()) {
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context.plan, context.queryBlock, rawTarget.getExpr(),
            NameResolvingMode.RELS_ONLY);
        if (checkIfBeEvaluatedAtRelation(block, evalNode, subQueryNode)) {
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprs.add(rawTarget.getAlias()); // newly added exr
        }
      } catch (VerifyException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    LinkedHashSet<Target> targets = createFieldTargetsFromRelation(block, subQueryNode, newlyEvaluatedExprs);

    for (String newAddedExpr : newlyEvaluatedExprs) {
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
      setOp = block.getNodeFromExpr(setOperation);
    } else if (setOperation.getType() == OpType.Except) {
      setOp = block.getNodeFromExpr(setOperation);
    } else if (setOperation.getType() == OpType.Intersect) {
      setOp = block.getNodeFromExpr(setOperation);
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

    setOp.setInSchema(leftChild.getOutSchema());
    Schema outSchema = PlannerUtil.targetToSchema(leftStrippedTargets);
    setOp.setOutSchema(outSchema);

    return setOp;
  }

  /*===============================================================================================
    INSERT SECTION
   ===============================================================================================*/

  public LogicalNode visitInsert(PlanContext context, Stack<Expr> stack, Insert expr) throws PlanningException {
    stack.push(expr);
    LogicalNode subQuery = super.visitInsert(context, stack, expr);
    stack.pop();

    InsertNode insertNode = context.queryBlock.getNodeFromExpr(expr);
    insertNode.setOverwrite(expr.isOverwrite());
    insertNode.setSubQuery(subQuery);

    if (expr.hasTableName()) { // INSERT (OVERWRITE) INTO TABLE ...
      return buildInsertIntoTablePlan(context, insertNode, expr);
    } else if (expr.hasLocation()) { // INSERT (OVERWRITE) INTO LOCATION ...
      return buildInsertIntoLocationPlan(context, insertNode, expr);
    } else {
      throw new IllegalStateException("Invalid Query");
    }
  }

  /**
   * Builds a InsertNode with a target table.
   *
   * ex) INSERT OVERWRITE INTO TABLE ...
   * <br />
   *
   * We use the following terms, such target table, target column
   * <pre>
   * INSERT INTO    [DATABASE_NAME.]TB_NAME        (col1, col2)          SELECT    c1,   c2        FROM ...
   *                 ^^^^^^^^^^^^^^ ^^^^^^^        ^^^^^^^^^^^^                  ^^^^^^^^^^^^
   *               target database target table  target columns (or schema)     projected columns (or schema)
   * </pre>
   */
  private InsertNode buildInsertIntoTablePlan(PlanContext context, InsertNode insertNode, Insert expr)
      throws PlanningException {
    // Get and set a target table
    String databaseName;
    String tableName;
    if (CatalogUtil.isFQTableName(expr.getTableName())) {
      databaseName = CatalogUtil.extractQualifier(expr.getTableName());
      tableName = CatalogUtil.extractSimpleName(expr.getTableName());
    } else {
      databaseName = context.session.getCurrentDatabase();
      tableName = expr.getTableName();
    }
    TableDesc desc = catalog.getTableDesc(databaseName, tableName);
    insertNode.setTargetTable(desc);

    //
    // When we use 'INSERT (OVERWIRTE) INTO TABLE statements, there are two cases.
    //
    // First, when a user specified target columns
    // INSERT (OVERWRITE)? INTO table_name (col1 type, col2 type) SELECT ...
    //
    // Second, when a user do not specified target columns
    // INSERT (OVERWRITE)? INTO table_name SELECT ...
    //
    // In the former case is, target columns' schema and corresponding projected columns' schema
    // must be equivalent or be available to cast implicitly.
    //
    // In the later case, the target table's schema and projected column's
    // schema of select clause can be different to each other. In this case,
    // we use only a sequence of preceding columns of target table's schema
    // as target columns.
    //
    // For example, consider a target table and an 'insert into' query are give as follows:
    //
    // CREATE TABLE TB1                  (col1 int,  col2 int, col3 long);
    //                                      ||          ||
    // INSERT OVERWRITE INTO TB1 SELECT  order_key,  part_num               FROM ...
    //
    // In this example, only col1 and col2 are used as target columns.

    if (expr.hasTargetColumns()) { // when a user specified target columns

      if (expr.getTargetColumns().length > insertNode.getChild().getOutSchema().size()) {
        throw new PlanningException("Target columns and projected columns are mismatched to each other");
      }

      // See PreLogicalPlanVerifier.visitInsert.
      // It guarantees that the equivalence between the numbers of target and projected columns.
      String [] targets = expr.getTargetColumns();
      Schema targetColumns = new Schema();
      for (int i = 0; i < targets.length; i++) {
        Column targetColumn = desc.getLogicalSchema().getColumn(targets[i]);
        targetColumns.addColumn(targetColumn);
      }
      insertNode.setTargetSchema(targetColumns);
      insertNode.setOutSchema(targetColumns);
      buildProjectedInsert(context, insertNode);

    } else { // when a user do not specified target columns

      // The output schema of select clause determines the target columns.
      Schema tableSchema = desc.getLogicalSchema();
      Schema projectedSchema = insertNode.getChild().getOutSchema();

      Schema targetColumns = new Schema();
      for (int i = 0; i < projectedSchema.size(); i++) {
        targetColumns.addColumn(tableSchema.getColumn(i));
      }
      insertNode.setTargetSchema(targetColumns);
      buildProjectedInsert(context, insertNode);
    }

    if (desc.hasPartition()) {
      insertNode.setPartitionMethod(desc.getPartitionMethod());
    }
    return insertNode;
  }

  private void buildProjectedInsert(PlanContext context, InsertNode insertNode) {
    Schema tableSchema = insertNode.getTableSchema();
    Schema targetColumns = insertNode.getTargetSchema();

    LogicalNode child = insertNode.getChild();

    if (child.getType() == NodeType.UNION) {
      child = makeProjectionForInsertUnion(context, insertNode);
    }

    if (child instanceof Projectable) {
      Projectable projectionNode = (Projectable) insertNode.getChild();

      // Modifying projected columns by adding NULL constants
      // It is because that table appender does not support target columns to be written.
      List<Target> targets = TUtil.newList();
      for (int i = 0, j = 0; i < tableSchema.size(); i++) {
        Column column = tableSchema.getColumn(i);

        if(targetColumns.contains(column) && j < projectionNode.getTargets().length) {
          targets.add(projectionNode.getTargets()[j++]);
        } else {
          targets.add(new Target(new ConstEval(NullDatum.get()), column.getSimpleName()));
        }
      }
      projectionNode.setTargets(targets.toArray(new Target[targets.size()]));

      insertNode.setInSchema(projectionNode.getOutSchema());
      insertNode.setOutSchema(projectionNode.getOutSchema());
      insertNode.setProjectedSchema(PlannerUtil.targetToSchema(targets));
    } else {
      throw new RuntimeException("Wrong child node type: " +  child.getType() + " for insert");
    }
  }

  private ProjectionNode makeProjectionForInsertUnion(PlanContext context, InsertNode insertNode) {
    LogicalNode child = insertNode.getChild();
    // add (projection - subquery) to RootBlock and create new QueryBlock for UnionNode
    TableSubQueryNode subQueryNode = context.plan.createNode(TableSubQueryNode.class);
    subQueryNode.init(context.queryBlock.getName(), child);
    subQueryNode.setTargets(PlannerUtil.schemaToTargets(subQueryNode.getOutSchema()));

    ProjectionNode projectionNode = context.plan.createNode(ProjectionNode.class);
    projectionNode.setChild(subQueryNode);
    projectionNode.setInSchema(subQueryNode.getInSchema());
    projectionNode.setTargets(subQueryNode.getTargets());

    context.queryBlock.registerNode(projectionNode);
    context.queryBlock.registerNode(subQueryNode);

    // add child QueryBlock to the UnionNode's QueryBlock
    UnionNode unionNode = (UnionNode)child;
    context.queryBlock.unregisterNode(unionNode);

    QueryBlock unionBlock = context.plan.newQueryBlock();
    unionBlock.registerNode(unionNode);
    unionBlock.setRoot(unionNode);

    QueryBlock leftBlock = context.plan.getBlock(unionNode.getLeftChild());
    QueryBlock rightBlock = context.plan.getBlock(unionNode.getRightChild());

    context.plan.disconnectBlocks(leftBlock, context.queryBlock);
    context.plan.disconnectBlocks(rightBlock, context.queryBlock);

    context.plan.connectBlocks(unionBlock, context.queryBlock, BlockType.TableSubQuery);
    context.plan.connectBlocks(leftBlock, unionBlock, BlockType.TableSubQuery);
    context.plan.connectBlocks(rightBlock, unionBlock, BlockType.TableSubQuery);

    // set InsertNode's child with ProjectionNode which is created.
    insertNode.setChild(projectionNode);

    return projectionNode;
  }

  /**
   * Build a InsertNode with a location.
   *
   * ex) INSERT OVERWRITE INTO LOCATION 'hdfs://....' ..
   */
  private InsertNode buildInsertIntoLocationPlan(PlanContext context, InsertNode insertNode, Insert expr) {
    // INSERT (OVERWRITE)? INTO LOCATION path (USING file_type (param_clause)?)? query_expression

    LogicalNode child = insertNode.getChild();

    if (child.getType() == NodeType.UNION) {
      child = makeProjectionForInsertUnion(context, insertNode);
    }

    Schema childSchema = child.getOutSchema();
    insertNode.setInSchema(childSchema);
    insertNode.setOutSchema(childSchema);
    insertNode.setTableSchema(childSchema);
    insertNode.setTargetLocation(new Path(expr.getLocation()));

    if (expr.hasStorageType()) {
      insertNode.setStorageType(CatalogUtil.getStoreType(expr.getStorageType()));
    }
    if (expr.hasParams()) {
      KeyValueSet options = new KeyValueSet();
      options.putAll(expr.getParams());
      insertNode.setOptions(options);
    }
    return insertNode;
  }

  /*===============================================================================================
    Data Definition Language (DDL) SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitCreateDatabase(PlanContext context, Stack<Expr> stack, CreateDatabase expr)
      throws PlanningException {
    CreateDatabaseNode createDatabaseNode = context.queryBlock.getNodeFromExpr(expr);
    createDatabaseNode.init(expr.getDatabaseName(), expr.isIfNotExists());
    return createDatabaseNode;
  }

  @Override
  public LogicalNode visitDropDatabase(PlanContext context, Stack<Expr> stack, DropDatabase expr)
      throws PlanningException {
    DropDatabaseNode dropDatabaseNode = context.queryBlock.getNodeFromExpr(expr);
    dropDatabaseNode.init(expr.getDatabaseName(), expr.isIfExists());
    return dropDatabaseNode;
  }

  public LogicalNode handleCreateTableLike(PlanContext context, CreateTable expr, CreateTableNode createTableNode)
    throws PlanningException {
    String parentTableName = expr.getLikeParentTableName();

    if (CatalogUtil.isFQTableName(parentTableName) == false) {
      parentTableName =
	CatalogUtil.buildFQName(context.session.getCurrentDatabase(),
				parentTableName);
    }
    TableDesc parentTableDesc = catalog.getTableDesc(parentTableName);
    if(parentTableDesc == null)
      throw new PlanningException("Table '"+parentTableName+"' does not exist");
    PartitionMethodDesc partitionDesc = parentTableDesc.getPartitionMethod();
    createTableNode.setTableSchema(parentTableDesc.getSchema());
    createTableNode.setPartitionMethod(partitionDesc);

    createTableNode.setStorageType(parentTableDesc.getMeta().getStoreType());
    createTableNode.setOptions(parentTableDesc.getMeta().getOptions());

    createTableNode.setExternal(parentTableDesc.isExternal());
    if(parentTableDesc.isExternal()) {
      createTableNode.setPath(parentTableDesc.getPath());
    }
    return createTableNode;
  }


  @Override
  public LogicalNode visitCreateTable(PlanContext context, Stack<Expr> stack, CreateTable expr)
      throws PlanningException {

    CreateTableNode createTableNode = context.queryBlock.getNodeFromExpr(expr);
    createTableNode.setIfNotExists(expr.isIfNotExists());

    // Set a table name to be created.
    if (CatalogUtil.isFQTableName(expr.getTableName())) {
      createTableNode.setTableName(expr.getTableName());
    } else {
      createTableNode.setTableName(
          CatalogUtil.buildFQName(context.session.getCurrentDatabase(), expr.getTableName()));
    }
    // This is CREATE TABLE <tablename> LIKE <parentTable>
    if(expr.getLikeParentTableName() != null)
      return handleCreateTableLike(context, expr, createTableNode);

    if (expr.hasStorageType()) { // If storage type (using clause) is specified
      createTableNode.setStorageType(CatalogUtil.getStoreType(expr.getStorageType()));
    } else { // otherwise, default type
      createTableNode.setStorageType(CatalogProtos.StoreType.CSV);
    }

    // Set default storage properties to be created.
    KeyValueSet keyValueSet = StorageUtil.newPhysicalProperties(createTableNode.getStorageType());
    if (expr.hasParams()) {
      keyValueSet.putAll(expr.getParams());
    }

    createTableNode.setOptions(keyValueSet);

    if (expr.hasPartition()) {
      if (expr.getPartitionMethod().getPartitionType().equals(PartitionType.COLUMN)) {
        createTableNode.setPartitionMethod(getPartitionMethod(context, expr.getTableName(), expr.getPartitionMethod()));
      } else {
        throw new PlanningException(String.format("Not supported PartitonType: %s",
            expr.getPartitionMethod().getPartitionType()));
      }
    }

    if (expr.hasSubQuery()) { // CREATE TABLE .. AS SELECT
      stack.add(expr);
      LogicalNode subQuery = visit(context, stack, expr.getSubQuery());
      stack.pop();
      createTableNode.setChild(subQuery);
      createTableNode.setInSchema(subQuery.getOutSchema());

      // If the table schema is defined
      // ex) CREATE TABLE tbl(col1 type, col2 type) AS SELECT ...
      if (expr.hasTableElements()) {
        createTableNode.setOutSchema(convertTableElementsSchema(expr.getTableElements()));
        createTableNode.setTableSchema(convertTableElementsSchema(expr.getTableElements()));
      } else {
        // if no table definition, the select clause's output schema will be used.
        // ex) CREATE TABLE tbl AS SELECT ...

        if (expr.hasPartition()) {
          PartitionMethodDesc partitionMethod = createTableNode.getPartitionMethod();

          Schema queryOutputSchema = subQuery.getOutSchema();
          Schema partitionExpressionSchema = partitionMethod.getExpressionSchema();
          if (partitionMethod.getPartitionType() == CatalogProtos.PartitionType.COLUMN &&
              queryOutputSchema.size() < partitionExpressionSchema.size()) {
            throw new VerifyException("Partition columns cannot be more than table columns.");
          }
          Schema tableSchema = new Schema();
          for (int i = 0; i < queryOutputSchema.size() - partitionExpressionSchema.size(); i++) {
            tableSchema.addColumn(queryOutputSchema.getColumn(i));
          }
          createTableNode.setOutSchema(tableSchema);
          createTableNode.setTableSchema(tableSchema);
        } else {
          // Convert the schema of subquery into the target table's one.
          Schema schema = new Schema(subQuery.getOutSchema());
          schema.setQualifier(createTableNode.getTableName());
          createTableNode.setOutSchema(schema);
          createTableNode.setTableSchema(schema);
        }
      }

      return createTableNode;

    } else { // if CREATE AN EMPTY TABLE
      Schema tableSchema = convertColumnsToSchema(expr.getTableElements());
      createTableNode.setTableSchema(tableSchema);

      if (expr.isExternal()) {
        createTableNode.setExternal(true);
      }

      if (expr.hasLocation()) {
        createTableNode.setPath(new Path(expr.getLocation()));
      }

      return createTableNode;
    }
  }

  private PartitionMethodDesc getPartitionMethod(PlanContext context,
                                                 String tableName,
                                                 CreateTable.PartitionMethodDescExpr expr) throws PlanningException {
    PartitionMethodDesc partitionMethodDesc;

    if(expr.getPartitionType() == PartitionType.COLUMN) {
      CreateTable.ColumnPartition partition = (CreateTable.ColumnPartition) expr;
      String partitionExpression = Joiner.on(',').join(partition.getColumns());

      partitionMethodDesc = new PartitionMethodDesc(context.session.getCurrentDatabase(), tableName,
          CatalogProtos.PartitionType.COLUMN, partitionExpression, convertColumnsToSchema(partition.getColumns()));
    } else {
      throw new PlanningException(String.format("Not supported PartitonType: %s", expr.getPartitionType()));
    }
    return partitionMethodDesc;
  }

  /**
   * It transforms table definition elements to schema.
   *
   * @param elements to be transformed
   * @return schema transformed from table definition elements
   */
  private Schema convertColumnsToSchema(ColumnDefinition[] elements) {
    Schema schema = new Schema();

    for (ColumnDefinition columnDefinition: elements) {
      schema.addColumn(convertColumn(columnDefinition));
    }

    return schema;
  }

  /**
   * It transforms table definition elements to schema.
   *
   * @param elements to be transformed
   * @return schema transformed from table definition elements
   */
  private Schema convertTableElementsSchema(ColumnDefinition[] elements) {
    Schema schema = new Schema();

    for (ColumnDefinition columnDefinition: elements) {
      schema.addColumn(convertColumn(columnDefinition));
    }

    return schema;
  }

  private Column convertColumn(ColumnDefinition columnDefinition) {
    return new Column(columnDefinition.getColumnName(), convertDataType(columnDefinition));
  }

  public static TajoDataTypes.DataType convertDataType(DataTypeExpr dataType) {
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
    String qualified;
    if (CatalogUtil.isFQTableName(dropTable.getTableName())) {
      qualified = dropTable.getTableName();
    } else {
      qualified = CatalogUtil.buildFQName(context.session.getCurrentDatabase(), dropTable.getTableName());
    }
    dropTableNode.init(qualified, dropTable.isIfExists(), dropTable.isPurge());
    return dropTableNode;
  }

  public LogicalNode visitAlterTablespace(PlanContext context, Stack<Expr> stack, AlterTablespace alterTablespace) {
    AlterTablespaceNode alter = context.queryBlock.getNodeFromExpr(alterTablespace);
    alter.setTablespaceName(alterTablespace.getTablespaceName());
    alter.setLocation(alterTablespace.getLocation());
    return alter;
  }

  @Override
  public LogicalNode visitAlterTable(PlanContext context, Stack<Expr> stack, AlterTable alterTable) {
    AlterTableNode alterTableNode = context.queryBlock.getNodeFromExpr(alterTable);
    alterTableNode.setTableName(alterTable.getTableName());
    alterTableNode.setNewTableName(alterTable.getNewTableName());
    alterTableNode.setColumnName(alterTable.getColumnName());
    alterTableNode.setNewColumnName(alterTable.getNewColumnName());

    if (null != alterTable.getAddNewColumn()) {
      alterTableNode.setAddNewColumn(convertColumn(alterTable.getAddNewColumn()));
    }
    alterTableNode.setAlterTableOpType(alterTable.getAlterTableOpType());
    return alterTableNode;
  }

  @Override
  public LogicalNode visitTruncateTable(PlanContext context, Stack<Expr> stack, TruncateTable truncateTable)
      throws PlanningException {
    TruncateTableNode truncateTableNode = context.queryBlock.getNodeFromExpr(truncateTable);
    truncateTableNode.setTableNames(truncateTable.getTableNames());
    return truncateTableNode;
  }

  /*===============================================================================================
    Util SECTION
  ===============================================================================================*/

  public static boolean checkIfBeEvaluatedAtWindowAgg(EvalNode evalNode, WindowAggNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

    if (columnRefs.size() > 0 && !node.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    return true;
  }

  public static boolean checkIfBeEvaluatedAtGroupBy(EvalNode evalNode, GroupbyNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

    if (columnRefs.size() > 0 && !node.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }

    return true;
  }

  public static boolean checkIfBeEvaluatedAtJoin(QueryBlock block, EvalNode evalNode, JoinNode node,
                                                 boolean isTopMostJoin) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }

    if (columnRefs.size() > 0 && !node.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    // When a 'case-when' is used with outer join, the case-when expression must be evaluated
    // at the topmost join operator.
    // TODO - It's also valid that case-when is evalauted at the topmost outer operator.
    //        But, how can we know there is no further outer join operator after this node?
    if (containsOuterJoin(block)) {
      if (!isTopMostJoin) {
        Collection<EvalNode> found = EvalTreeUtil.findOuterJoinSensitiveEvals(evalNode);
        if (found.size() > 0) {
          return false;
        }
      }
    }

    return true;
  }

  public static boolean isOuterJoin(JoinType joinType) {
    return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER || joinType==JoinType.FULL_OUTER;
  }

  public static boolean containsOuterJoin(QueryBlock block) {
    return block.containsJoinType(JoinType.LEFT_OUTER) || block.containsJoinType(JoinType.RIGHT_OUTER) ||
        block.containsJoinType(JoinType.FULL_OUTER);
  }

  /**
   * It checks if evalNode can be evaluated at this @{link RelationNode}.
   */
  public static boolean checkIfBeEvaluatedAtRelation(QueryBlock block, EvalNode evalNode, RelationNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

    // aggregation functions cannot be evaluated in scan node
    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    // aggregation functions cannot be evaluated in scan node
    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }

    if (columnRefs.size() > 0 && !node.getTableSchema().containsAll(columnRefs)) {
      return false;
    }

    // Why? - When a {case when} is used with outer join, case when must be evaluated at topmost outer join.
    if (containsOuterJoin(block)) {
      Collection<EvalNode> found = EvalTreeUtil.findOuterJoinSensitiveEvals(evalNode);
      if (found.size() > 0) {
        return false;
      }
    }

    return true;
  }

  public static boolean checkIfBeEvaluatedAtThis(EvalNode evalNode, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);
    if (columnRefs.size() > 0 && !node.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    return true;
  }
}
