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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.algebra.WindowSpec;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.exprrewrite.EvalTreeOptimizer;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.rewrite.rules.ProjectionPushDownRule;
import org.apache.tajo.plan.util.ExprFinder;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.StorageService;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.net.URI;
import java.util.*;

import static org.apache.tajo.algebra.CreateTable.PartitionType;
import static org.apache.tajo.plan.ExprNormalizer.ExprNormalizedResult;
import static org.apache.tajo.plan.LogicalPlan.BlockType;
import static org.apache.tajo.plan.verifier.SyntaxErrorUtil.makeSyntaxError;

/**
 * This class creates a logical plan from a nested tajo algebra expression ({@link org.apache.tajo.algebra})
 */
public class LogicalPlanner extends BaseAlgebraVisitor<LogicalPlanner.PlanContext, LogicalNode> {
  private static Log LOG = LogFactory.getLog(LogicalPlanner.class);
  private final CatalogService catalog;
  private final StorageService storage;

  private final LogicalPlanPreprocessor preprocessor;
  private final EvalTreeOptimizer evalOptimizer;
  private final ExprAnnotator exprAnnotator;
  private final ExprNormalizer normalizer;

  public LogicalPlanner(CatalogService catalog, StorageService storage) {
    this.catalog = catalog;
    this.storage = storage;

    this.exprAnnotator = new ExprAnnotator(catalog);
    this.preprocessor = new LogicalPlanPreprocessor(catalog, exprAnnotator);
    this.normalizer = new ExprNormalizer();
    this.evalOptimizer = new EvalTreeOptimizer();
  }

  public static class PlanContext {
    OverridableConf queryContext;
    LogicalPlan plan;
    QueryBlock queryBlock;
    EvalTreeOptimizer evalOptimizer;
    TimeZone timeZone;
    List<Expr> unplannedExprs = TUtil.newList();
    boolean debugOrUnitTests;
    Integer noNameSubqueryId = 0;

    public PlanContext(OverridableConf context, LogicalPlan plan, QueryBlock block, EvalTreeOptimizer evalOptimizer,
                       boolean debugOrUnitTests) {
      this.queryContext = context;
      this.plan = plan;
      this.queryBlock = block;
      this.evalOptimizer = evalOptimizer;

      // session's time zone
      if (context.containsKey(SessionVars.TIMEZONE)) {
        String timezoneId = context.get(SessionVars.TIMEZONE);
        timeZone = TimeZone.getTimeZone(timezoneId);
      }

      this.debugOrUnitTests = debugOrUnitTests;
    }

    public PlanContext(PlanContext context, QueryBlock block) {
      this.queryContext = context.queryContext;
      this.plan = context.plan;
      this.queryBlock = block;
      this.evalOptimizer = context.evalOptimizer;
      this.debugOrUnitTests = context.debugOrUnitTests;
    }

    public QueryBlock getQueryBlock() {
      return queryBlock;
    }

    public LogicalPlan getPlan() {
      return plan;
    }

    public OverridableConf getQueryContext() {
      return queryContext;
    }

    public String toString() {
      return "block=" + queryBlock.getName() + ", relNum=" + queryBlock.getRelations().size() + ", "+
          queryBlock.namedExprsMgr.toString();
    }

    /**
     * It generates a unique table subquery name
     */
    public String generateUniqueSubQueryName() {
      return LogicalPlan.NONAME_SUBQUERY_PREFIX + noNameSubqueryId++;
    }
  }

  /**
   * This generates a logical plan.
   *
   * @param expr A relational algebraic expression for a query.
   * @return A logical plan
   */
  public LogicalPlan createPlan(OverridableConf context, Expr expr) throws TajoException {
    return createPlan(context, expr, false);
  }

  @VisibleForTesting
  public LogicalPlan createPlan(OverridableConf queryContext, Expr expr, boolean debug) throws TajoException {

    LogicalPlan plan = new LogicalPlan();

    QueryBlock rootBlock = plan.newAndGetBlock(LogicalPlan.ROOT_BLOCK);
    PlanContext context = new PlanContext(queryContext, plan, rootBlock, evalOptimizer, debug);
    preprocessor.process(context, expr);
    plan.resetGeneratedId();
    LogicalNode topMostNode = this.visit(context, new Stack<>(), expr);

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

  public void preHook(PlanContext context, Stack<Expr> stack, Expr expr) throws TajoException {
    context.queryBlock.updateCurrentNode(expr);
  }

  public LogicalNode postHook(PlanContext context, Stack<Expr> stack, Expr expr, LogicalNode current)
      throws TajoException {


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

  @Override
  public LogicalNode visitSetSession(PlanContext context, Stack<Expr> stack, SetSession expr) throws TajoException {
    QueryBlock block = context.queryBlock;

    SetSessionNode setSessionNode = block.getNodeFromExpr(expr);
    setSessionNode.init(expr.getName(), expr.getValue());

    return setSessionNode;
  }

  public LogicalNode visitExplain(PlanContext ctx, Stack<Expr> stack, Explain expr) throws TajoException {
    ctx.plan.setExplain(expr.isGlobal());
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
      throws TajoException {

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
    List<Target> targets;
    targets = buildTargets(context, referenceNames);

    // Set ProjectionNode
    projectionNode = context.queryBlock.getNodeFromExpr(projection);
    projectionNode.init(projection.isDistinct(), targets);
    projectionNode.setChild(child);
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));

    if (projection.isDistinct() && block.hasNode(NodeType.GROUP_BY)) {
      throw makeSyntaxError("Cannot support grouping and distinct at the same time yet");
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

  private void setRawTargets(PlanContext context, List<Target> targets, String[] referenceNames,
                             Projection projection) throws TajoException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    // It's for debugging or unit tests.
    List<Target> rawTargets = new ArrayList<>();
    for (int i = 0; i < projection.getNamedExprs().size(); i++) {
      NamedExpr namedExpr = projection.getNamedExprs().get(i);
      EvalNode evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(),
          NameResolvingMode.RELS_AND_SUBEXPRS);
      rawTargets.add(new Target(evalNode, referenceNames[i]));
    }
    // it's for debugging or unit testing
    block.setRawTargets(rawTargets);
  }

  private void insertDistinctOperator(PlanContext context, ProjectionNode projectionNode, LogicalNode child,
                                      Stack<Expr> stack) throws TajoException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    if (child.getType() == NodeType.SORT) {
      SortNode sortNode = (SortNode) child;

      GroupbyNode dupRemoval = context.plan.createNode(GroupbyNode.class);
      dupRemoval.setForDistinctBlock();
      dupRemoval.setChild(sortNode.getChild());
      dupRemoval.setInSchema(sortNode.getInSchema());
      dupRemoval.setTargets(PlannerUtil.schemaToTargets(sortNode.getInSchema()));
      dupRemoval.setGroupingColumns(sortNode.getInSchema().toArray());

      block.registerNode(dupRemoval);
      postHook(context, stack, null, dupRemoval);

      sortNode.setChild(dupRemoval);
      sortNode.setInSchema(dupRemoval.getOutSchema());
      sortNode.setOutSchema(dupRemoval.getOutSchema());
      projectionNode.setInSchema(sortNode.getOutSchema());
      projectionNode.setChild(sortNode);

    } else {
      Schema outSchema = projectionNode.getOutSchema();
      GroupbyNode dupRemoval = context.plan.createNode(GroupbyNode.class);
      dupRemoval.setForDistinctBlock();
      dupRemoval.setChild(child);
      dupRemoval.setInSchema(projectionNode.getInSchema());
      dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
      dupRemoval.setGroupingColumns(outSchema.toArray());

      block.registerNode(dupRemoval);
      postHook(context, stack, null, dupRemoval);

      projectionNode.setChild(dupRemoval);
      projectionNode.setInSchema(dupRemoval.getOutSchema());
    }
  }

  private Pair<String [], ExprNormalizer.WindowSpecReferences []> doProjectionPrephase(PlanContext context,
                                                                                    Projection projection)
      throws TajoException {

    QueryBlock block = context.queryBlock;

    int finalTargetNum = projection.size();
    String [] referenceNames = new String[finalTargetNum];
    ExprNormalizedResult[] normalizedExprList = new ExprNormalizedResult[finalTargetNum];

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

    return new Pair<>(referenceNames,
            windowSpecReferencesList.toArray(new ExprNormalizer.WindowSpecReferences[windowSpecReferencesList.size()]));
  }

  private interface Matcher {
    boolean isMatch(Expr expr);
  }

  public List<Integer> normalize(PlanContext context, Projection projection, ExprNormalizedResult [] normalizedExprList,
                                 Matcher matcher) throws TajoException {
    List<Integer> targetIds = new ArrayList<>();
    for (int i = 0; i < projection.size(); i++) {
      NamedExpr namedExpr = projection.getNamedExprs().get(i);

      if (PlannerUtil.existsAggregationFunction(namedExpr)) {
        context.queryBlock.setAggregationRequire();
      }

      if (matcher.isMatch(namedExpr.getExpr())) {
        // If a value is constant value, it adds the constant value with a proper name to the constant map
        // of the current block
        if (!namedExpr.hasAlias() && OpType.isLiteralType(namedExpr.getExpr().getType())) {
          String generatedName = context.plan.generateUniqueColumnName(namedExpr.getExpr());
          ConstEval constEval = (ConstEval) exprAnnotator.createEvalNode(context, namedExpr.getExpr(),
              NameResolvingMode.RELS_ONLY);
          context.getQueryBlock().addConstReference(generatedName, namedExpr.getExpr(), constEval);
          normalizedExprList[i] = new ExprNormalizedResult(context, false);
          normalizedExprList[i].baseExpr = new ColumnReferenceExpr(generatedName);

        } else {
          // dissect an expression into multiple parts (at most dissected into three parts)
          normalizedExprList[i] = normalizer.normalize(context, namedExpr.getExpr());
        }
        targetIds.add(i);
      }
    }

    return targetIds;
  }

  private void addNamedExprs(QueryBlock block, String [] referenceNames, ExprNormalizedResult [] normalizedExprList,
                             List<ExprNormalizer.WindowSpecReferences> windowSpecReferencesList, Projection projection,
                             List<Integer> targetIds) throws TajoException {
    for (int i : targetIds) {
      NamedExpr namedExpr = projection.getNamedExprs().get(i);
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
      throws TajoException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    int finalTargetNum = projection.getNamedExprs().size();
    List<Target> targets = new ArrayList<>();

    for (int i = 0; i < finalTargetNum; i++) {
      NamedExpr namedExpr = projection.getNamedExprs().get(i);
      EvalNode evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(), NameResolvingMode.RELS_ONLY);
      if (namedExpr.hasAlias()) {
        targets.add(new Target(evalNode, namedExpr.getAlias()));
      } else {
        targets.add(new Target(evalNode, context.plan.generateUniqueColumnName(namedExpr.getExpr())));
      }
    }
    EvalExprNode evalExprNode = context.queryBlock.getNodeFromExpr(projection);
    evalExprNode.setTargets(targets);
    evalExprNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    // it's for debugging or unit testing
    block.setRawTargets(targets);
    return evalExprNode;
  }

  private List<Target> buildTargets(PlanContext context, String[] referenceNames)
      throws TajoException {
    QueryBlock block = context.queryBlock;

    List<Target> targets = new ArrayList<>();

    for (int i = 0; i < referenceNames.length; i++) {
      String refName = referenceNames[i];
      if (block.isConstReference(refName)) {
        targets.add(new Target(block.getConstByReference(refName), refName));
      } else if (block.namedExprsMgr.isEvaluated(refName)) {
        targets.add(block.namedExprsMgr.getTarget(refName));
      } else {
        NamedExpr namedExpr = block.namedExprsMgr.getNamedExpr(refName);
        EvalNode evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(),
            NameResolvingMode.RELS_AND_SUBEXPRS);
        block.namedExprsMgr.markAsEvaluated(refName, evalNode);
        targets.add(new Target(evalNode, refName));
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
   * @throws TajoException
   */
  public static void verifyProjectedFields(QueryBlock block, Projectable projectable) throws TajoException {
    if (projectable instanceof GroupbyNode) {
      GroupbyNode groupbyNode = (GroupbyNode) projectable;

      if (!groupbyNode.isEmptyGrouping()) { // it should be targets instead of
        int groupingKeyNum = groupbyNode.getGroupingColumns().length;

        for (int i = 0; i < groupingKeyNum; i++) {
          Target target = groupbyNode.getTargets().get(i);
          if (groupbyNode.getTargets().get(i).getEvalTree().getType() == EvalType.FIELD) {
            FieldEval grpKeyEvalNode = target.getEvalTree();
            if (!groupbyNode.getInSchema().contains(grpKeyEvalNode.getColumnRef())) {
              throwCannotEvaluateException(projectable, grpKeyEvalNode.getName());
            }
          }
        }
      }

      if (groupbyNode.hasAggFunctions()) {
        verifyIfEvalNodesCanBeEvaluated(projectable, (List<EvalNode>)(List<?>) groupbyNode.getAggFunctions());
      }

    } else if (projectable instanceof WindowAggNode) {
      WindowAggNode windowAggNode = (WindowAggNode) projectable;

      if (windowAggNode.hasPartitionKeys()) {
        verifyIfColumnCanBeEvaluated(projectable.getInSchema(), projectable, windowAggNode.getPartitionKeys());
      }

      if (windowAggNode.hasAggFunctions()) {
        verifyIfEvalNodesCanBeEvaluated(projectable, Arrays.asList(windowAggNode.getWindowFunctions()));
      }

      if (windowAggNode.hasSortSpecs()) {
        Column [] sortKeys = PlannerUtil.sortSpecsToSchema(windowAggNode.getSortSpecs()).toArray();
        verifyIfColumnCanBeEvaluated(projectable.getInSchema(), projectable, sortKeys);
      }

      // verify targets except for function slots
      for (int i = 0; i < windowAggNode.getTargets().size() - windowAggNode.getWindowFunctions().length; i++) {
        Target target = windowAggNode.getTargets().get(i);
        Set<Column> columns = EvalTreeUtil.findUniqueColumns(target.getEvalTree());
        for (Column c : columns) {
          if (!windowAggNode.getInSchema().contains(c)) {
            throwCannotEvaluateException(projectable, c.getQualifiedName());
          }
        }
      }

    } else if (projectable instanceof RelationNode) {
      RelationNode relationNode = (RelationNode) projectable;
      prohibitNestedRecordProjection((Projectable) relationNode);
      verifyIfTargetsCanBeEvaluated(relationNode.getLogicalSchema(), (Projectable) relationNode);

    } else {
      prohibitNestedRecordProjection(projectable);
      verifyIfTargetsCanBeEvaluated(projectable.getInSchema(), projectable);
    }
  }

  public static void prohibitNestedRecordProjection(Projectable projectable)
      throws TajoException {
    for (Target t : projectable.getTargets()) {
      if (t.getEvalTree().getValueType().getType() == TajoDataTypes.Type.RECORD) {
        throw new NotImplementedException("record field projection");
      }
    }
  }

  public static void verifyIfEvalNodesCanBeEvaluated(Projectable projectable, List<EvalNode> evalNodes)
      throws TajoException {
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
      throws TajoException {
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
      throws TajoException {
    for (Column c : columns) {
      if (!baseSchema.contains(c)) {
        throwCannotEvaluateException(projectable, c.getQualifiedName());
      }
    }
  }

  public static void throwCannotEvaluateException(Projectable projectable, String columnName) throws TajoException {
    if (projectable instanceof UnaryNode && ((UnaryNode) projectable).getChild().getType() == NodeType.GROUP_BY) {
      throw makeSyntaxError(columnName
          + " must appear in the GROUP BY clause or be used in an aggregate function at node ("
          + projectable.getPID() + ")");
    } else {
      throw makeSyntaxError(String.format("Cannot evaluate the field \"%s\" at node (%d)",
          columnName, projectable.getPID()));
    }
  }

  private LogicalNode insertWindowAggNode(PlanContext context, LogicalNode child, Stack<Expr> stack,
                                          String [] referenceNames,
                                          ExprNormalizer.WindowSpecReferences [] windowSpecReferenceses)
      throws TajoException {
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

    List<String> winFuncRefs = new ArrayList<>();
    List<WindowFunctionEval> winFuncs = new ArrayList<>();
    List<WindowSpec> rawWindowSpecs = Lists.newArrayList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr rawTarget = it.next();
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context, rawTarget.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.WINDOW_FUNCTION) {
          winFuncRefs.add(rawTarget.getAlias());
          winFuncs.add((WindowFunctionEval) evalNode);
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);

          // TODO - Later, we also consider the possibility that a window function contains only a window name.
          rawWindowSpecs.add(((WindowFunctionExpr) (rawTarget.getExpr())).getWindowSpec());
        }
      } catch (UndefinedColumnException uc) {
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
          throw makeSyntaxError("Each grouping column expression must be a scalar expression.");
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
            throw new IllegalStateException("Unexpected State: " + StringUtils.join(sortSpecs));
          }
          annotatedSortSpecs[i] = new SortSpec(column, sortSpecs[i].isAscending(), sortSpecs[i].isNullsFirst());
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

    List<Target> targets = new ArrayList<>();
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

    for (int i = 0; i < referenceNames.length ; i++) {
      if (!windowFuncIndices.contains(i)) {
        if (block.isConstReference(referenceNames[i])) {
          targets.add(new Target(block.getConstByReference(referenceNames[i]), referenceNames[i]));
        } else {
          targets.add(block.namedExprsMgr.getTarget(referenceNames[i]));
        }
      }
    }
    for (int i = 0; i < winFuncRefs.size(); i++) {
      targets.add(block.namedExprsMgr.getTarget(winFuncRefs.get(i)));
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
      throws TajoException {

    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    // The limit operation must affect to the number of results, not the number of input records.
    // Thus, the aggregation must be carried out before the limit operation.
    if (child.getType() == NodeType.LIMIT) {
      child = ((LimitNode)child).getChild();
    }

    GroupbyNode groupbyNode = context.plan.createNode(GroupbyNode.class);
    groupbyNode.setChild(child);
    groupbyNode.setInSchema(child.getOutSchema());

    groupbyNode.setGroupingColumns(new Column[] {});

    Set<String> aggEvalNames = new LinkedHashSet<>();
    Set<AggregationFunctionCallEval> aggEvals = new LinkedHashSet<>();
    boolean includeDistinctFunction = false;
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr rawTarget = it.next();
      try {
        // check if at least distinct aggregation function
        includeDistinctFunction |= PlannerUtil.existsDistinctAggregationFunction(rawTarget.getExpr());
        EvalNode evalNode = exprAnnotator.createEvalNode(context, rawTarget.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.AGG_FUNCTION) {
          aggEvalNames.add(rawTarget.getAlias());
          aggEvals.add((AggregationFunctionCallEval) evalNode);
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
        }
      } catch (UndefinedColumnException ve) {
      }
    }

    groupbyNode.setDistinct(includeDistinctFunction);
    groupbyNode.setAggFunctions(new ArrayList<>(aggEvals));
    List<Target> targets = ProjectionPushDownRule.buildGroupByTarget(groupbyNode, null,
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
  public LimitNode visitLimit(PlanContext context, Stack<Expr> stack, Limit limit) throws TajoException {
    QueryBlock block = context.queryBlock;

    EvalNode firstFetNum;
    LogicalNode child;
    if (limit.getFetchFirstNum().getType() == OpType.Literal) {
      firstFetNum = exprAnnotator.createEvalNode(context, limit.getFetchFirstNum(),
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
        firstFetNum = exprAnnotator.createEvalNode(context, namedExpr.getExpr(), NameResolvingMode.SUBEXPRS_AND_RELS);
        block.namedExprsMgr.markAsEvaluated(referName, firstFetNum);
      }
    }
    LimitNode limitNode = block.getNodeFromExpr(limit);
    limitNode.setChild(child);
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());

    firstFetNum.bind(null, null);
    limitNode.setFetchFirst(firstFetNum.eval(null).asInt8());

    return limitNode;
  }

  @Override
  public LogicalNode visitSort(PlanContext context, Stack<Expr> stack, Sort sort) throws TajoException {
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
    SortSpec[] annotatedSortSpecs = annotateSortSpecs(block, referNames, sortSpecs);
    if (annotatedSortSpecs.length == 0) {
      return child;
    } else {
      sortNode.setSortSpecs(annotatedSortSpecs);
      return sortNode;
    }
  }

  private static SortSpec[] annotateSortSpecs(QueryBlock block, String [] referNames, Sort.SortSpec[] rawSortSpecs) {
    int sortKeyNum = rawSortSpecs.length;
    Column column;
    List<SortSpec> annotatedSortSpecs = Lists.newArrayList();
    for (int i = 0; i < sortKeyNum; i++) {
      String refName = referNames[i];
      if (block.isConstReference(refName)) {
        continue;
      } else if (block.namedExprsMgr.isEvaluated(refName)) {
        column = block.namedExprsMgr.getTarget(refName).getNamedColumn();
      } else {
        throw new IllegalStateException("Unexpected State: " + StringUtils.join(rawSortSpecs));
      }
      annotatedSortSpecs.add(new SortSpec(column, rawSortSpecs[i].isAscending(), rawSortSpecs[i].isNullsFirst()));
    }
    return annotatedSortSpecs.toArray(new SortSpec[annotatedSortSpecs.size()]);
  }

  /*===============================================================================================
    GROUP BY SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitHaving(PlanContext context, Stack<Expr> stack, Having expr) throws TajoException {
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
      havingCondition = exprAnnotator.createEvalNode(context, namedExpr.getExpr(),
          NameResolvingMode.SUBEXPRS_AND_RELS);
      block.namedExprsMgr.markAsEvaluated(referName, havingCondition);
    }

    // set having condition
    having.setQual(havingCondition);

    return having;
  }

  @Override
  public LogicalNode visitGroupBy(PlanContext context, Stack<Expr> stack, Aggregation aggregation)
      throws TajoException {

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
    List<Column> groupingColumns = Lists.newArrayList();
    for (int i = 0; i < groupingKeyRefNames.length; i++) {
      String refName = groupingKeyRefNames[i];
      if (context.getQueryBlock().isConstReference(refName)) {
        continue;
      } else if (block.namedExprsMgr.isEvaluated(groupingKeyRefNames[i])) {
        groupingColumns.add(block.namedExprsMgr.getTarget(groupingKeyRefNames[i]).getNamedColumn());
      } else {
        throw makeSyntaxError("Each grouping column expression must be a scalar expression.");
      }
    }

    int effectiveGroupingKeyNum = groupingColumns.size();
    groupingNode.setGroupingColumns(groupingColumns.toArray(new Column[effectiveGroupingKeyNum]));

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
        EvalNode evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(),
            NameResolvingMode.SUBEXPRS_AND_RELS);
        if (evalNode.getType() == EvalType.AGG_FUNCTION) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          aggEvalNames.add(namedExpr.getAlias());
          aggEvalNodes.add((AggregationFunctionCallEval) evalNode);
        }
      } catch (UndefinedColumnException ve) {
      }
    }
    // if there is at least one distinct aggregation function
    groupingNode.setDistinct(includeDistinctFunction);
    groupingNode.setAggFunctions(aggEvalNodes);

    List<Target> targets = new ArrayList<>();

    // In target, grouping columns will be followed by aggregation evals.
    //
    // col1, col2, col3,   sum(..),  agv(..)
    // ^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^
    //  grouping keys      aggregation evals

    // Build grouping keys
    for (int i = 0; i < effectiveGroupingKeyNum; i++) {
      targets.add(block.namedExprsMgr.getTarget(groupingNode.getGroupingColumns()[i].getQualifiedName()));
    }

    for (int i = 0; i < aggEvalNodes.size(); i++) {
      targets.add(block.namedExprsMgr.getTarget(aggEvalNames.get(i)));
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
      throws TajoException {
    QueryBlock block = context.queryBlock;

    ExprNormalizedResult normalizedResult = normalizer.normalize(context, selection.getQual());
    block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
    if (normalizedResult.aggExprs.size() > 0 || normalizedResult.scalarExprs.size() > 0) {
      throw makeSyntaxError("Filter condition cannot include aggregation function");
    }

    ////////////////////////////////////////////////////////
    // Visit and Build Child Plan
    ////////////////////////////////////////////////////////
    stack.push(selection);
    // Since filter push down will be done later, it is guaranteed that in-subqueries are found at only selection.
    for (Expr eachQual : PlannerUtil.extractInSubquery(selection.getQual())) {
      InPredicate inPredicate = (InPredicate) eachQual;
      visit(context, stack, inPredicate.getInValue());
      context.unplannedExprs.add(inPredicate.getInValue());
    }
    LogicalNode child = visit(context, stack, selection.getChild());
    stack.pop();
    ////////////////////////////////////////////////////////

    SelectionNode selectionNode = context.queryBlock.getNodeFromExpr(selection);
    selectionNode.setChild(child);
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());

    // Create EvalNode for a search condition.
    EvalNode searchCondition = exprAnnotator.createEvalNode(context, selection.getQual(),
        NameResolvingMode.RELS_AND_SUBEXPRS);
    EvalNode simplified = context.evalOptimizer.optimize(context, searchCondition);
    // set selection condition
    selectionNode.setQual(simplified);

    return selectionNode;
  }

  /*===============================================================================================
    JOIN SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitJoin(PlanContext context, Stack<Expr> stack, Join join)
      throws TajoException {
    // Phase 1: Init
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    if (join.hasQual()) {
      ExprNormalizedResult normalizedResult = normalizer.normalize(context, join.getQual(), true);
      block.namedExprsMgr.addExpr(normalizedResult.baseExpr);
      if (normalizedResult.aggExprs.size() > 0 || normalizedResult.scalarExprs.size() > 0) {
        throw makeSyntaxError("Filter condition cannot include aggregation function");
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
      EvalNode evalNode = exprAnnotator.createEvalNode(context, join.getQual(), NameResolvingMode.LEGACY);
      joinCondition = context.evalOptimizer.optimize(context, evalNode);
    }

    // If the query involves a subquery, the stack can be empty.
    // In this case, this join is the top most one within a query block.
    boolean isTopMostJoin = stack.isEmpty() ? true : stack.peek().getType() != OpType.Join;
    List<String> newlyEvaluatedExprs = getNewlyEvaluatedExprsForJoin(context, joinNode, isTopMostJoin);
    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));

    for (String newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }
    joinNode.setTargets(targets);

    // Determine join conditions
    if (join.isNatural()) { // if natural join, it should have the equi-join conditions by common column names
      EvalNode njCond = getNaturalJoinCondition(joinNode);
      joinNode.setJoinQual(njCond);
    } else if (join.hasQual()) { // otherwise, the given join conditions are set
      joinNode.setJoinQual(joinCondition);
    }

    return joinNode;
  }

  private List<String> getNewlyEvaluatedExprsForJoin(PlanContext context, JoinNode joinNode, boolean isTopMostJoin)
      throws TajoException {

    QueryBlock block = context.queryBlock;

    EvalNode evalNode;
    List<String> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(), NameResolvingMode.LEGACY);
        // the predicates specified in the on clause are already processed in visitJoin()
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(context.queryBlock, evalNode, joinNode, isTopMostJoin)) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getAlias());
        }
      } catch (UndefinedColumnException ve) {
      }
    }
    return newlyEvaluatedExprs;
  }

  private static Schema getNaturalJoinSchema(LogicalNode left, LogicalNode right) {
    Schema joinSchema = new Schema();
    Schema commons = SchemaUtil.getNaturalJoinColumns(left.getOutSchema(), right.getOutSchema());
    joinSchema.addColumns(commons);
    for (Column c : left.getOutSchema().getRootColumns()) {
      if (!joinSchema.contains(c.getQualifiedName())) {
        joinSchema.addColumn(c);
      }
    }

    for (Column c : right.getOutSchema().getRootColumns()) {
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

    for (Column common : commons.getRootColumns()) {
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
      throws TajoException {
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    JoinNode join = plan.createNode(JoinNode.class);
    join.init(JoinType.CROSS, left, right);
    join.setInSchema(merged);
    block.addJoinType(join.getJoinType());

    EvalNode evalNode;
    List<String> newlyEvaluatedExprs = TUtil.newList();
    for (Iterator<NamedExpr> it = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); it.hasNext();) {
      NamedExpr namedExpr = it.next();
      try {
        evalNode = exprAnnotator.createEvalNode(context, namedExpr.getExpr(), NameResolvingMode.LEGACY);
        if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() == 0
            && EvalTreeUtil.findWindowFunction(evalNode).size() == 0) {
          block.namedExprsMgr.markAsEvaluated(namedExpr.getAlias(), evalNode);
          newlyEvaluatedExprs.add(namedExpr.getAlias());
        }
      } catch (UndefinedColumnException ve) {}
    }

    List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(merged));
    for (String newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }
    join.setTargets(targets);
    return join;
  }

  @Override
  public LogicalNode visitRelationList(PlanContext context, Stack<Expr> stack, RelationList relations)
      throws TajoException {

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
      throws TajoException {
    QueryBlock block = context.queryBlock;

    ScanNode scanNode = block.getNodeFromExpr(expr);

    // Find expression which can be evaluated at this relation node.
    // Except for column references, additional expressions used in select list, where clause, order-by clauses
    // can be evaluated here. Their reference names are kept in newlyEvaluatedExprsRef.
    Set<String> newlyEvaluatedExprsReferences = new LinkedHashSet<>();
    for (Iterator<NamedExpr> iterator = block.namedExprsMgr.getIteratorForUnevaluatedExprs(); iterator.hasNext();) {
      NamedExpr rawTarget = iterator.next();
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context, rawTarget.getExpr(),
            NameResolvingMode.RELS_ONLY);
        if (checkIfBeEvaluatedAtRelation(block, evalNode, scanNode)) {
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprsReferences.add(rawTarget.getAlias()); // newly added exr
        }
      } catch (UndefinedColumnException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    LinkedHashSet<Target> targets = createFieldTargetsFromRelation(block, scanNode, newlyEvaluatedExprsReferences);

    // The fact the some expr is included in newlyEvaluatedExprsReferences means that it is already evaluated.
    // So, we get a raw expression and then creates a target.
    for (String reference : newlyEvaluatedExprsReferences) {
      NamedExpr refrer = block.namedExprsMgr.getNamedExpr(reference);
      EvalNode evalNode = exprAnnotator.createEvalNode(context, refrer.getExpr(), NameResolvingMode.RELS_ONLY);
      targets.add(new Target(evalNode, reference));
    }

    scanNode.setTargets(new ArrayList<>(targets));

    verifyProjectedFields(block, scanNode);
    return scanNode;
  }

  private static LinkedHashSet<Target> createFieldTargetsFromRelation(QueryBlock block, RelationNode relationNode,
                                                                      Set<String> newlyEvaluatedRefNames) {
    LinkedHashSet<Target> targets = Sets.newLinkedHashSet();
    for (Column column : relationNode.getLogicalSchema().getAllColumns()) {

      // TODO - Currently, EvalNode has DataType as a return type. So, RECORD cannot be used for any target.
      // The following line is a kind of hack, preventing RECORD to be used for target in the logical planning phase.
      // This problem should be resolved after TAJO-1402.
      if (column.getTypeDesc().getDataType().getType() == TajoDataTypes.Type.RECORD) {
        continue;
      }

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

  @Override
  public TableSubQueryNode visitTableSubQuery(PlanContext context, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws TajoException {
    return visitCommonTableSubquery(context, stack, expr);
  }

  @Override
  public TableSubQueryNode visitSimpleTableSubquery(PlanContext context, Stack<Expr> stack, SimpleTableSubquery expr)
      throws TajoException {
    return visitCommonTableSubquery(context, stack, expr);
  }

  private TableSubQueryNode visitCommonTableSubquery(PlanContext context, Stack<Expr> stack, CommonSubquery expr)
      throws TajoException {
    QueryBlock currentBlock = context.queryBlock;
    QueryBlock childBlock = context.plan.getBlock(context.plan.getBlockNameByExpr(expr.getSubQuery()));
    context.plan.connectBlocks(childBlock, currentBlock, BlockType.TableSubQuery);

    PlanContext newContext = new PlanContext(context, childBlock);
    context.plan.connectBlocks(childBlock, context.queryBlock, BlockType.TableSubQuery);
    LogicalNode child = visit(newContext, new Stack<>(), expr.getSubQuery());
    TableSubQueryNode subQueryNode = currentBlock.getNodeFromExpr(expr);

    subQueryNode.setSubQuery(child);
    setTargetOfTableSubQuery(context, currentBlock, subQueryNode);

    return subQueryNode;
  }

  private void setTargetOfTableSubQuery (PlanContext context, QueryBlock block, TableSubQueryNode subQueryNode)
      throws TajoException {
    // Add additional expressions required in upper nodes.
    Set<String> newlyEvaluatedExprs = new HashSet<>();
    for (NamedExpr rawTarget : block.namedExprsMgr.getAllNamedExprs()) {
      try {
        EvalNode evalNode = exprAnnotator.createEvalNode(context, rawTarget.getExpr(),
            NameResolvingMode.RELS_ONLY);
        if (checkIfBeEvaluatedAtRelation(block, evalNode, subQueryNode)) {
          block.namedExprsMgr.markAsEvaluated(rawTarget.getAlias(), evalNode);
          newlyEvaluatedExprs.add(rawTarget.getAlias()); // newly added exr
        }
      } catch (UndefinedColumnException ve) {
      }
    }

    // Assume that each unique expr is evaluated once.
    LinkedHashSet<Target> targets = createFieldTargetsFromRelation(block, subQueryNode, newlyEvaluatedExprs);

    for (String newAddedExpr : newlyEvaluatedExprs) {
      targets.add(block.namedExprsMgr.getTarget(newAddedExpr, true));
    }

    subQueryNode.setTargets(new ArrayList<>(targets));
  }

    /*===============================================================================================
    SET OPERATION SECTION
   ===============================================================================================*/

  @Override
  public LogicalNode visitUnion(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws TajoException {
    UnionNode unionNode = (UnionNode)buildSetPlan(context, stack, setOperation);
    LogicalNode resultingNode = unionNode;

    /**
     *  if the given node is Union (Distinct), it adds group by node
     *    change
     *     from
     *             union
     *
     *       to
     *           projection
     *               |
     *            group by
     *               |
     *         table subquery
     *               |
     *             union
     */
    if (unionNode.isDistinct()) {
      return insertProjectionGroupbyBeforeSetOperation(context, unionNode);
    }

    return resultingNode;
  }

  private ProjectionNode insertProjectionGroupbyBeforeSetOperation(PlanContext context,
                                                                   SetOperationNode setOperationNode)
      throws TajoException {
    QueryBlock currentBlock = context.queryBlock;

    // make table subquery node which has set operation as its subquery
    TableSubQueryNode setOpTableSubQueryNode = context.plan.createNode(TableSubQueryNode.class);
    setOpTableSubQueryNode.init(CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE),
        context.generateUniqueSubQueryName()), setOperationNode);
    setTargetOfTableSubQuery(context, currentBlock, setOpTableSubQueryNode);
    currentBlock.registerNode(setOpTableSubQueryNode);
    currentBlock.addRelation(setOpTableSubQueryNode);

    Schema setOpSchema = setOpTableSubQueryNode.getOutSchema();
    List<Target> setOpTarget = setOpTableSubQueryNode.getTargets();

    // make group by node whose grouping keys are all columns of set operation
    GroupbyNode setOpGroupbyNode = context.plan.createNode(GroupbyNode.class);
    setOpGroupbyNode.setInSchema(setOpSchema);
    setOpGroupbyNode.setGroupingColumns(setOpSchema.toArray());
    setOpGroupbyNode.setTargets(setOpTarget);
    setOpGroupbyNode.setChild(setOpTableSubQueryNode);
    currentBlock.registerNode(setOpGroupbyNode);

    // make projection node which projects all the union columns
    ProjectionNode setOpProjectionNode = context.plan.createNode(ProjectionNode.class);
    setOpProjectionNode.setInSchema(setOpSchema);
    setOpProjectionNode.setTargets(setOpTarget);
    setOpProjectionNode.setChild(setOpGroupbyNode);
    currentBlock.registerNode(setOpProjectionNode);

    // changing query block chain: at below, ( ) indicates query block
    // (... + set operation ) - (...) ==> (... + projection + group by + table subquery) - (set operation) - (...)
    QueryBlock setOpBlock = context.plan.newQueryBlock();
    setOpBlock.registerNode(setOperationNode);
    setOpBlock.setRoot(setOperationNode);

    QueryBlock leftBlock = context.plan.getBlock(setOperationNode.getLeftChild());
    QueryBlock rightBlock = context.plan.getBlock(setOperationNode.getRightChild());

    context.plan.disconnectBlocks(leftBlock, context.queryBlock);
    context.plan.disconnectBlocks(rightBlock, context.queryBlock);

    context.plan.connectBlocks(setOpBlock, context.queryBlock, BlockType.TableSubQuery);
    context.plan.connectBlocks(leftBlock, setOpBlock, BlockType.TableSubQuery);
    context.plan.connectBlocks(rightBlock, setOpBlock, BlockType.TableSubQuery);

    // projection node (not original set operation node) will be a new child of parent node
    return setOpProjectionNode;
  }

  @Override
  public LogicalNode visitExcept(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws TajoException {
    return buildSetPlan(context, stack, setOperation);
  }

  @Override
  public LogicalNode visitIntersect(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws TajoException {
    return buildSetPlan(context, stack, setOperation);
  }

  private LogicalNode buildSetPlan(PlanContext context, Stack<Expr> stack, SetOperation setOperation)
      throws TajoException {

    // 1. Init Phase
    LogicalPlan plan = context.plan;
    QueryBlock block = context.queryBlock;

    ////////////////////////////////////////////////////////
    // Visit and Build Left Child Plan
    ////////////////////////////////////////////////////////
    QueryBlock leftBlock = context.plan.getBlockByExpr(setOperation.getLeft());
    PlanContext leftContext = new PlanContext(context, leftBlock);
    stack.push(setOperation);
    LogicalNode leftChild = visit(leftContext, new Stack<>(), setOperation.getLeft());
    stack.pop();
    // Connect left child and current blocks
    context.plan.connectBlocks(leftContext.queryBlock, context.queryBlock, BlockType.TableSubQuery);

    ////////////////////////////////////////////////////////
    // Visit and Build Right Child Plan
    ////////////////////////////////////////////////////////
    QueryBlock rightBlock = context.plan.getBlockByExpr(setOperation.getRight());
    PlanContext rightContext = new PlanContext(context, rightBlock);
    stack.push(setOperation);
    LogicalNode rightChild = visit(rightContext, new Stack<>(), setOperation.getRight());
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
      throw new TajoInternalError("Unknown set type: " + setOperation.getType());
    }
    setOp.setLeftChild(leftChild);
    setOp.setRightChild(rightChild);

    // An union statement can be derived from two query blocks.
    // For one union statement between both relations, we can ensure that each corresponding data domain of both
    // relations are the same. However, if necessary, the schema of left query block will be used as a base schema.
    List<Target> leftStrippedTargets = PlannerUtil.stripTarget(
        PlannerUtil.schemaToTargets(leftBlock.getRoot().getOutSchema()));

    setOp.setInSchema(leftChild.getOutSchema());
    Schema outSchema = PlannerUtil.targetToSchema(leftStrippedTargets);
    setOp.setOutSchema(outSchema);

    return setOp;
  }

  /*===============================================================================================
    INSERT SECTION
   ===============================================================================================*/

  public LogicalNode visitInsert(PlanContext context, Stack<Expr> stack, Insert expr) throws TajoException {
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
      throws TajoException {
    // Get and set a target table
    String databaseName;
    String tableName;
    if (CatalogUtil.isFQTableName(expr.getTableName())) {
      databaseName = CatalogUtil.extractQualifier(expr.getTableName());
      tableName = CatalogUtil.extractSimpleName(expr.getTableName());
    } else {
      databaseName = context.queryContext.get(SessionVars.CURRENT_DATABASE);
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
    // For example, consider a target table and an 'insert into' query are given as follows:
    //
    // CREATE TABLE TB1                  (col1 int,  col2 int, col3 long);
    //                                      ||          ||
    // INSERT OVERWRITE INTO TB1 SELECT  order_key,  part_num               FROM ...
    //
    // In this example, only col1 and col2 are used as target columns.

    if (expr.hasTargetColumns()) { // when a user specified target columns

      if (expr.getTargetColumns().length > insertNode.getChild().getOutSchema().size()) {
        throw makeSyntaxError("Target columns and projected columns are mismatched to each other");
      }

      // See PreLogicalPlanVerifier.visitInsert.
      // It guarantees that the equivalence between the numbers of target and projected columns.
      ColumnReferenceExpr [] targets = expr.getTargetColumns();
      Schema targetColumns = new Schema();
      for (int i = 0; i < targets.length; i++) {
        Column targetColumn = desc.getLogicalSchema().getColumn(targets[i].getCanonicalName().replace(".", "/"));

        if (targetColumn == null) {
          throw makeSyntaxError("column '" + targets[i] + "' of relation '" + desc.getName() + "' does not exist");
        }

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
      Projectable projectionNode = insertNode.getChild();

      // Modifying projected columns by adding NULL constants
      // It is because that table appender does not support target columns to be written.
      List<Target> targets = TUtil.newList();

      for (Column column : tableSchema.getAllColumns()) {
        int idxInProjectionNode = targetColumns.getIndex(column);

        // record type itself cannot be projected yet.
        if (column.getDataType().getType() == TajoDataTypes.Type.RECORD) {
          continue;
        }

        if (idxInProjectionNode >= 0 && idxInProjectionNode < projectionNode.getTargets().size()) {
          targets.add(projectionNode.getTargets().get(idxInProjectionNode));
        } else {
          targets.add(new Target(new ConstEval(NullDatum.get()), column.getSimpleName()));
        }
      }
      projectionNode.setTargets(targets);

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

    // Rewrite
    URI targetUri = URI.create(expr.getLocation());
    if (targetUri.getScheme() == null) {
      targetUri = URI.create(context.getQueryContext().get(QueryVars.DEFAULT_SPACE_ROOT_URI) + "/" + targetUri);
    }
    insertNode.setUri(targetUri);

    if (expr.hasStorageType()) {
      insertNode.setDataFormat(CatalogUtil.getBackwardCompitableDataFormat(expr.getDataFormat()));
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
      throws TajoException {
    CreateDatabaseNode createDatabaseNode = context.queryBlock.getNodeFromExpr(expr);
    createDatabaseNode.init(expr.getDatabaseName(), expr.isIfNotExists());
    return createDatabaseNode;
  }

  @Override
  public LogicalNode visitDropDatabase(PlanContext context, Stack<Expr> stack, DropDatabase expr)
      throws TajoException {
    DropDatabaseNode dropDatabaseNode = context.queryBlock.getNodeFromExpr(expr);
    dropDatabaseNode.init(expr.getDatabaseName(), expr.isIfExists());
    return dropDatabaseNode;
  }

  public LogicalNode handleCreateTableLike(PlanContext context, CreateTable expr, CreateTableNode createTableNode)
    throws TajoException {
    String parentTableName = expr.getLikeParentTableName();

    if (CatalogUtil.isFQTableName(parentTableName) == false) {
      parentTableName = CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE),
          parentTableName);
    }
    TableDesc baseTable = catalog.getTableDesc(parentTableName);
    if(baseTable == null) {
      throw new UndefinedTableException(parentTableName);
    }

    PartitionMethodDesc partitionDesc = baseTable.getPartitionMethod();
    createTableNode.setTableSchema(baseTable.getSchema());
    createTableNode.setPartitionMethod(partitionDesc);

    createTableNode.setDataFormat(CatalogUtil.getBackwardCompitableDataFormat(baseTable.getMeta().getDataFormat()));
    createTableNode.setOptions(baseTable.getMeta().getPropertySet());

    createTableNode.setExternal(baseTable.isExternal());
    if(baseTable.isExternal()) {
      createTableNode.setUri(baseTable.getUri());
    }
    return createTableNode;
  }

  @Override
  public LogicalNode visitCreateTable(PlanContext context, Stack<Expr> stack, CreateTable expr)
      throws TajoException {

    CreateTableNode createTableNode = context.queryBlock.getNodeFromExpr(expr);
    createTableNode.setIfNotExists(expr.isIfNotExists());

    // Set a table name to be created.
    if (CatalogUtil.isFQTableName(expr.getTableName())) {
      createTableNode.setTableName(expr.getTableName());
    } else {
      createTableNode.setTableName(
          CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE), expr.getTableName()));
    }
    // This is CREATE TABLE <tablename> LIKE <parentTable>
    if(expr.getLikeParentTableName() != null) {
      return handleCreateTableLike(context, expr, createTableNode);
    }

    if (expr.hasTableSpaceName()) {
      createTableNode.setTableSpaceName(expr.getTableSpaceName());
    }

    createTableNode.setUri(getCreatedTableURI(context, expr));

    if (expr.hasStorageType()) { // If storage type (using clause) is specified
      createTableNode.setDataFormat(CatalogUtil.getBackwardCompitableDataFormat(expr.getStorageType()));
    } else { // otherwise, default type
      createTableNode.setDataFormat(BuiltinStorages.TEXT);
    }

    // Set default storage properties to table
    createTableNode.setOptions(CatalogUtil.newDefaultProperty(createTableNode.getStorageType()));

    // Priority to apply table properties
    // 1. Explicit table properties specified in WITH clause
    // 2. Session variables

    // Set session variables to properties
    TablePropertyUtil.setTableProperty(context.queryContext, createTableNode);

    // Set table property specified in WITH clause and it will override all others
    if (expr.hasParams()) {
      createTableNode.getOptions().putAll(expr.getParams());
    }


    if (expr.hasPartition()) {
      if (expr.getPartitionMethod().getPartitionType().equals(PartitionType.COLUMN)) {
        createTableNode.setPartitionMethod(getPartitionMethod(context, expr.getTableName(), expr.getPartitionMethod()));
      } else {
        throw ExceptionUtil.makeNotSupported(
            String.format("PartitonType " + expr.getPartitionMethod().getPartitionType()));
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
            throw makeSyntaxError("Partition columns cannot be more than table columns.");
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
      if (!expr.hasSelfDescSchema()) {
        Schema tableSchema = convertColumnsToSchema(expr.getTableElements());
        createTableNode.setTableSchema(tableSchema);
      } else {
        createTableNode.setSelfDescSchema(true);
      }

      if (expr.isExternal()) {
        createTableNode.setExternal(true);
      }

      return createTableNode;
    }
  }

  /**
   * Return a table uri to be created
   *
   * @param context PlanContext
   * @param createTable An algebral expression for create table
   * @return a Table uri to be created on a given table space
   */
  private URI getCreatedTableURI(PlanContext context, CreateTable createTable) {

    if (createTable.hasLocation()) {
      URI tableUri = URI.create(createTable.getLocation());
      if (tableUri.getScheme() == null) { // if a given table URI is a just path, the default tablespace will be added.
        tableUri = URI.create(context.queryContext.get(QueryVars.DEFAULT_SPACE_ROOT_URI) + createTable.getLocation());
      }
      return tableUri;

    } else {

      String tableName = createTable.getTableName();
      String databaseName = CatalogUtil.isFQTableName(tableName) ?
          CatalogUtil.extractQualifier(tableName) : context.queryContext.get(SessionVars.CURRENT_DATABASE);

      return storage.getTableURI(
          createTable.getTableSpaceName(), databaseName, CatalogUtil.extractSimpleName(tableName));
    }
  }

  private PartitionMethodDesc getPartitionMethod(PlanContext context,
                                                 String tableName,
                                                 CreateTable.PartitionMethodDescExpr expr) throws TajoException {
    PartitionMethodDesc partitionMethodDesc;

    if(expr.getPartitionType() == PartitionType.COLUMN) {
      CreateTable.ColumnPartition partition = (CreateTable.ColumnPartition) expr;
      String partitionExpression = Joiner.on(',').join(partition.getColumns());

      partitionMethodDesc = new PartitionMethodDesc(context.queryContext.get(SessionVars.CURRENT_DATABASE), tableName,
          CatalogProtos.PartitionType.COLUMN, partitionExpression, convertColumnsToSchema(partition.getColumns()));
    } else {
      throw new NotImplementedException("partition type '" + expr.getPartitionType() + "'");
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
  private static Schema convertTableElementsSchema(ColumnDefinition[] elements) {
    Schema schema = new Schema();

    for (ColumnDefinition columnDefinition: elements) {
      schema.addColumn(convertColumn(columnDefinition));
    }

    return schema;
  }

  /**
   * It transforms ColumnDefinition array to String array.
   *
   * @param columnReferenceExprs
   * @return
   */
  private static String[] convertColumnsToStrings(ColumnReferenceExpr[] columnReferenceExprs) {
    int columnCount = columnReferenceExprs.length;
    String[] columns = new String[columnCount];

    for(int i = 0; i < columnCount; i++) {
      ColumnReferenceExpr columnReferenceExpr = columnReferenceExprs[i];
      columns[i] = columnReferenceExpr.getName();
    }

    return columns;
  }

  /**
   * It transforms Expr array to String array.
   *
   * @param exprs
   * @return
   */
  private static String[] convertExprsToStrings(Expr[] exprs) {
    int exprCount = exprs.length;
    String[] values = new String[exprCount];

    for(int i = 0; i < exprCount; i++) {
      LiteralValue expr = (LiteralValue)exprs[i];
      values[i] = expr.getValue();
    }

    return values;
  }

  private static Column convertColumn(ColumnDefinition columnDefinition) {
    return new Column(columnDefinition.getColumnName(), convertDataType(columnDefinition));
  }

  public static TypeDesc convertDataType(DataTypeExpr dataType) {
    TajoDataTypes.Type type = TajoDataTypes.Type.valueOf(dataType.getTypeName());

    TajoDataTypes.DataType.Builder builder = TajoDataTypes.DataType.newBuilder();
    builder.setType(type);

    if (dataType.hasLengthOrPrecision()) {
      builder.setLength(dataType.getLengthOrPrecision());
    } else {
      if (type == TajoDataTypes.Type.CHAR) {
        builder.setLength(1);
      }
    }

    TypeDesc typeDesc;
    if (type == TajoDataTypes.Type.RECORD) {
      Schema nestedRecordSchema = convertTableElementsSchema(dataType.getNestedRecordTypes());
      typeDesc = new TypeDesc(nestedRecordSchema);
    } else {
      typeDesc = new TypeDesc(builder.build());
    }

    return typeDesc;
  }


  @Override
  public LogicalNode visitDropTable(PlanContext context, Stack<Expr> stack, DropTable dropTable) {
    DropTableNode dropTableNode = context.queryBlock.getNodeFromExpr(dropTable);
    String qualified;
    if (CatalogUtil.isFQTableName(dropTable.getTableName())) {
      qualified = dropTable.getTableName();
    } else {
      qualified = CatalogUtil.buildFQName(
          context.queryContext.get(SessionVars.CURRENT_DATABASE), dropTable.getTableName());
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
    alterTableNode.setProperties(new KeyValueSet(alterTable.getParams()));

    if (null != alterTable.getAddNewColumn()) {
      alterTableNode.setAddNewColumn(convertColumn(alterTable.getAddNewColumn()));
    }

    if (alterTable.getColumns() != null) {
      alterTableNode.setPartitionColumns(convertColumnsToStrings(alterTable.getColumns()));
    }

    if (alterTable.getValues() != null) {
      alterTableNode.setPartitionValues(convertExprsToStrings(alterTable.getValues()));
    }

    if (alterTable.getLocation() != null) {
      alterTableNode.setLocation(alterTable.getLocation());
    }

    alterTableNode.setPurge(alterTable.isPurge());
    alterTableNode.setIfNotExists(alterTable.isIfNotExists());
    alterTableNode.setIfExists(alterTable.isIfExists());
    alterTableNode.setAlterTableOpType(alterTable.getAlterTableOpType());
    return alterTableNode;
  }

  private static URI getIndexPath(PlanContext context, String databaseName, String indexName) {
    return new Path(TajoConf.getWarehouseDir(context.queryContext.getConf()),
        databaseName + "/" + indexName + "/").toUri();
  }

  @Override
  public LogicalNode visitCreateIndex(PlanContext context, Stack<Expr> stack, CreateIndex createIndex)
      throws TajoException {
    stack.push(createIndex);
    LogicalNode child = visit(context, stack, createIndex.getChild());
    stack.pop();

    QueryBlock block = context.queryBlock;
    CreateIndexNode createIndexNode = block.getNodeFromExpr(createIndex);
    if (CatalogUtil.isFQTableName(createIndex.getIndexName())) {
      createIndexNode.setIndexName(createIndex.getIndexName());
    } else {
      createIndexNode.setIndexName(
          CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE), createIndex.getIndexName()));
    }
    createIndexNode.setUnique(createIndex.isUnique());
    Sort.SortSpec[] sortSpecs = createIndex.getSortSpecs();
    int sortKeyNum = sortSpecs.length;
    String[] referNames = new String[sortKeyNum];

    ExprNormalizedResult[] normalizedExprList = new ExprNormalizedResult[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      normalizedExprList[i] = normalizer.normalize(context, sortSpecs[i].getKey());
    }
    for (int i = 0; i < sortKeyNum; i++) {
      // even if base expressions don't have their name,
      // reference names should be identifiable for the later sort spec creation.
      referNames[i] = block.namedExprsMgr.addExpr(normalizedExprList[i].baseExpr);
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].aggExprs);
      block.namedExprsMgr.addNamedExprArray(normalizedExprList[i].scalarExprs);
    }

    Collection<RelationNode> relations = block.getRelations();
    if (relations.size() > 1) {
      throw new UnsupportedException("Index on multiple relations");
    } else if (relations.size() == 0) {
      throw new TajoInternalError("No relation for indexing");
    }
    RelationNode relationNode = relations.iterator().next();
    if (!(relationNode instanceof ScanNode)) {
      throw new UnsupportedException("Index on subquery");
    }

    createIndexNode.setExternal(createIndex.isExternal());
    createIndexNode.setKeySortSpecs(relationNode.getLogicalSchema(), annotateSortSpecs(block, referNames, sortSpecs));
    createIndexNode.setIndexMethod(IndexMethod.valueOf(createIndex.getMethodSpec().getName().toUpperCase()));
    if (createIndex.isExternal()) {
      createIndexNode.setIndexPath(new Path(createIndex.getIndexPath()).toUri());
    } else {
      createIndexNode.setIndexPath(
          getIndexPath(context, context.queryContext.get(SessionVars.CURRENT_DATABASE), createIndex.getIndexName()));
    }

    if (createIndex.getParams() != null) {
      KeyValueSet keyValueSet = new KeyValueSet();
      keyValueSet.putAll(createIndex.getParams());
      createIndexNode.setOptions(keyValueSet);
    }

    createIndexNode.setChild(child);
    return createIndexNode;
  }

  @Override
  public LogicalNode visitDropIndex(PlanContext context, Stack<Expr> stack, DropIndex dropIndex) {
    DropIndexNode dropIndexNode = context.queryBlock.getNodeFromExpr(dropIndex);
    dropIndexNode.setIndexName(dropIndex.getIndexName());
    return dropIndexNode;
  }

  @Override
  public LogicalNode visitTruncateTable(PlanContext context, Stack<Expr> stack, TruncateTable truncateTable)
      throws TajoException {
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

  public static boolean isEvaluatableJoinQual(QueryBlock block, EvalNode evalNode, JoinNode node,
                                              boolean isOnPredicate, boolean isTopMostJoin) {

    if (checkIfBeEvaluatedAtJoin(block, evalNode, node, isTopMostJoin)) {

      if (isNonEquiThetaJoinQual(block, node, evalNode)) {
        return false;
      }

      if (PlannerUtil.isOuterJoinType(node.getJoinType())) {
        /*
         * For outer joins, only predicates which are specified at the on clause can be evaluated during processing join.
         * Other predicates from the where clause must be evaluated after the join.
         * The below code will be modified after improving join operators to keep join filters by themselves (TAJO-1310).
         */
        if (!isOnPredicate) {
          return false;
        }
      } else {
        /*
         * Only join predicates should be evaluated at join if the join type is inner or cross. (TAJO-1445)
         */
        if (!EvalTreeUtil.isJoinQual(block, node.getLeftChild().getOutSchema(), node.getRightChild().getOutSchema(),
            evalNode, false)) {
          return false;
        }
      }

      return true;
    }

    return false;
  }

  public static boolean isNonEquiThetaJoinQual(final LogicalPlan.QueryBlock block,
                                               final JoinNode joinNode,
                                               final EvalNode evalNode) {
    if (EvalTreeUtil.isJoinQual(block, joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema(), evalNode, true) &&
        evalNode.getType() != EvalType.EQUAL) {
      return true;
    } else {
      return false;
    }
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

    if (columnRefs.size() > 0 && !node.getLogicalSchema().containsAll(columnRefs)) {
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
