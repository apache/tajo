/*
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
import org.apache.commons.lang.ObjectUtils;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.NotThreadSafe;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.graph.DirectedGraphCursor;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.util.TUtil;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * This represents and keeps every information about a query plan for a query.
 */
@NotThreadSafe
public class LogicalPlan {
  /** the prefix character for virtual tables */
  public static final char VIRTUAL_TABLE_PREFIX='#';
  public static final char NONAMED_COLUMN_PREFIX='?';
  public static final char NONAMED_WINDOW_PREFIX='^';

  /** it indicates the root block */
  public static final String ROOT_BLOCK = VIRTUAL_TABLE_PREFIX + "ROOT";
  public static final String NONAME_BLOCK_PREFIX = VIRTUAL_TABLE_PREFIX + "QB_";
  private static final int NO_SEQUENCE_PID = -1;
  private int nextPid = 0;
  private Integer noNameBlockId = 0;
  private Integer noNameColumnId = 0;
  private Integer noNameWindowId = 0;

  /** a map from between a block name to a block plan */
  private Map<String, QueryBlock> queryBlocks = new LinkedHashMap<String, QueryBlock>();
  private Map<Integer, LogicalNode> nodeMap = new HashMap<Integer, LogicalNode>();
  private Map<Integer, QueryBlock> queryBlockByPID = new HashMap<Integer, QueryBlock>();
  private Map<String, String> exprToBlockNameMap = TUtil.newHashMap();
  private SimpleDirectedGraph<String, BlockEdge> queryBlockGraph = new SimpleDirectedGraph<String, BlockEdge>();

  /** planning and optimization log */
  private List<String> planingHistory = Lists.newArrayList();
  LogicalPlanner planner;

  private boolean isExplain;
  private final String currentDatabase;

  public LogicalPlan(String currentDatabase, LogicalPlanner planner) {
    this.currentDatabase = currentDatabase;
    this.planner = planner;
  }

  /**
   * Create a LogicalNode instance for a type. Each a LogicalNode instance is given an unique plan node id (PID).
   *
   * @param theClass The class to be created
   * @return a LogicalNode instance identified by an unique plan node id (PID).
   */
  public <T extends LogicalNode> T createNode(Class<T> theClass) {
    try {
      Constructor<T> ctor = theClass.getConstructor(int.class);
      return ctor.newInstance(newPID());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a LogicalNode instance for a type. Each a LogicalNode instance is not given an unique plan node id (PID).
   * This method must be only used after all query planning and optimization phases.
   *
   * @param theClass The class to be created
   * @return a LogicalNode instance
   */
  public static <T extends LogicalNode> T createNodeWithoutPID(Class<T> theClass) {
    try {
      Constructor<T> ctor = theClass.getConstructor(int.class);
      return ctor.newInstance(NO_SEQUENCE_PID);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setExplain() {
    isExplain = true;
  }

  public boolean isExplain() {
    return isExplain;
  }

  /**
   * Create a new {@link QueryBlock} and Get
   *
   * @param blockName the query block name
   * @return a created query block
   */
  public QueryBlock newAndGetBlock(String blockName) {
    QueryBlock block = new QueryBlock(blockName);
    queryBlocks.put(blockName, block);
    return block;
  }

  public int newPID() {
    return nextPid++;
  }

  public QueryBlock newQueryBlock() {
    return newAndGetBlock(NONAME_BLOCK_PREFIX + (noNameBlockId++));
  }

  /**
   * It generates an unique column name from EvalNode. It is usually used for an expression or predicate without
   * a specified name (i.e., alias).
   */
  public String generateUniqueColumnName(EvalNode evalNode) {
    String prefix = evalNode.getName();
    return attachSeqIdToGeneratedColumnName(prefix).toLowerCase();
  }

  /**
   * It generates an unique column name from Expr. It is usually used for an expression or predicate without
   * a specified name (i.e., alias).
   */
  public String generateUniqueColumnName(Expr expr) {
    String generatedName;
    if (expr.getType() == OpType.Column) {
      generatedName = ((ColumnReferenceExpr) expr).getCanonicalName();
    } else { // if a generated column name
      generatedName = attachSeqIdToGeneratedColumnName(getGeneratedPrefixFromExpr(expr));
    }
    return generatedName;
  }

  /**
   * It attaches a generated column name with a sequence id. It always keeps generated names unique.
   */
  private String attachSeqIdToGeneratedColumnName(String prefix) {
    int sequence = noNameColumnId++;
    return NONAMED_COLUMN_PREFIX + prefix.toLowerCase() + (sequence > 0 ? "_" + sequence : "");
  }

  /**
   * It generates a column reference prefix name. It is usually used for an expression or predicate without
   * a specified name (i.e., alias). For example, a predicate in WHERE does not have any alias name.
   * It just returns a prefix name. In other words, it always returns the same prefix for the same type of expressions.
   * So, you must add some suffix to the returned name in order to distinguish reference names.
   */
  private static String getGeneratedPrefixFromExpr(Expr expr) {
    String prefix;

    switch (expr.getType()) {
    case Column:
      prefix = ((ColumnReferenceExpr) expr).getCanonicalName();
      break;
    case CountRowsFunction:
      prefix = "count";
      break;
    case GeneralSetFunction:
      GeneralSetFunctionExpr setFunction = (GeneralSetFunctionExpr) expr;
      prefix = setFunction.getSignature();
      break;
    case Function:
      FunctionExpr function = (FunctionExpr) expr;
      prefix = function.getSignature();
      break;
    default:
      prefix = expr.getType().name();
    }
    return prefix;
  }

  public QueryBlock getRootBlock() {
    return queryBlocks.get(ROOT_BLOCK);
  }

  public QueryBlock getBlock(String blockName) {
    return queryBlocks.get(blockName);
  }

  public QueryBlock getBlock(LogicalNode node) {
    return queryBlockByPID.get(node.getPID());
  }

  public void removeBlock(QueryBlock block) {
    queryBlocks.remove(block.getName());
    List<Integer> tobeRemoved = new ArrayList<Integer>();
    for (Map.Entry<Integer, QueryBlock> entry : queryBlockByPID.entrySet()) {
      tobeRemoved.add(entry.getKey());
    }
    for (Integer rn : tobeRemoved) {
      queryBlockByPID.remove(rn);
    }
  }

  public void disconnectBlocks(QueryBlock srcBlock, QueryBlock targetBlock) {
    queryBlockGraph.removeEdge(srcBlock.getName(), targetBlock.getName());
  }

  public void connectBlocks(QueryBlock srcBlock, QueryBlock targetBlock, BlockType type) {
    queryBlockGraph.addEdge(srcBlock.getName(), targetBlock.getName(), new BlockEdge(srcBlock, targetBlock, type));
  }

  public QueryBlock getParentBlock(QueryBlock block) {
    return queryBlocks.get(queryBlockGraph.getParent(block.getName(), 0));
  }

  public List<QueryBlock> getChildBlocks(QueryBlock block) {
    List<QueryBlock> childBlocks = TUtil.newList();
    for (String blockName : queryBlockGraph.getChilds(block.getName())) {
      childBlocks.add(queryBlocks.get(blockName));
    }
    return childBlocks;
  }

  public void mapExprToBlock(Expr expr, String blockName) {
    exprToBlockNameMap.put(ObjectUtils.identityToString(expr), blockName);
  }

  public QueryBlock getBlockByExpr(Expr expr) {
    return getBlock(exprToBlockNameMap.get(ObjectUtils.identityToString(expr)));
  }

  public String getBlockNameByExpr(Expr expr) {
    return exprToBlockNameMap.get(ObjectUtils.identityToString(expr));
  }

  public Collection<QueryBlock> getQueryBlocks() {
    return queryBlocks.values();
  }

  public SimpleDirectedGraph<String, BlockEdge> getQueryBlockGraph() {
    return queryBlockGraph;
  }

  public String getNormalizedColumnName(QueryBlock block, ColumnReferenceExpr columnRef)
      throws PlanningException {
    Column found = resolveColumn(block, columnRef);
    if (found == null) {
      throw new NoSuchColumnException(columnRef.getCanonicalName());
    }
    return found.getQualifiedName();
  }

  public String resolveDatabase(QueryBlock block, String tableName) throws PlanningException {
    List<String> found = new ArrayList<String>();
    for (RelationNode relation : block.getRelations()) {
      // check alias name or table name
      if (CatalogUtil.extractSimpleName(relation.getCanonicalName()).equals(tableName) ||
          CatalogUtil.extractSimpleName(relation.getTableName()).equals(tableName)) {
        // obtain the database name
        found.add(CatalogUtil.extractQualifier(relation.getTableName()));
      }
    }

    if (found.size() == 0) {
      return null;
    } else if (found.size() > 1) {
      throw new PlanningException("Ambiguous table name \"" + tableName + "\"");
    }

    return found.get(0);
  }

  /**
   * It resolves a column.
   */
  public Column resolveColumn(QueryBlock block, ColumnReferenceExpr columnRef) throws PlanningException {
    if (columnRef.hasQualifier()) {
      return resolveColumnWithQualifier(block, columnRef);
    } else {
      return resolveColumnWithoutQualifier(block, columnRef);
    }
  }

  private Column resolveColumnWithQualifier(QueryBlock block, ColumnReferenceExpr columnRef) throws PlanningException {
    String qualifier;
    String canonicalName;
    String qualifiedName;

    if (CatalogUtil.isFQTableName(columnRef.getQualifier())) {
      qualifier = columnRef.getQualifier();
      canonicalName = columnRef.getCanonicalName();
    } else {
      String resolvedDatabaseName = resolveDatabase(block, columnRef.getQualifier());
      if (resolvedDatabaseName == null) {
        throw new NoSuchColumnException(columnRef.getQualifier());
      }
      qualifier = CatalogUtil.buildFQName(resolvedDatabaseName, columnRef.getQualifier());
      canonicalName = CatalogUtil.buildFQName(qualifier, columnRef.getName());
    }
    qualifiedName = CatalogUtil.buildFQName(qualifier, columnRef.getName());

    RelationNode relationOp = block.getRelation(qualifier);

    // if a column name is outside of this query block
    if (relationOp == null) {
      // TODO - nested query can only refer outer query block? or not?
      for (QueryBlock eachBlock : queryBlocks.values()) {
        if (eachBlock.existsRelation(qualifier)) {
          relationOp = eachBlock.getRelation(qualifier);
        }
      }
    }

    // If we cannot find any relation against a qualified column name
    if (relationOp == null) {
      throw new NoSuchColumnException(canonicalName);
    }

    if (block.isAlreadyRenamedTableName(CatalogUtil.extractQualifier(canonicalName))) {
      String changedName = CatalogUtil.buildFQName(
          relationOp.getCanonicalName(),
          CatalogUtil.extractSimpleName(canonicalName));
      canonicalName = changedName;
    }

    Schema schema = relationOp.getTableSchema();
    Column column = schema.getColumn(canonicalName);
    if (column == null) {
      throw new NoSuchColumnException(canonicalName);
    }

    // If code reach here, a column is found.
    // But, it may be aliased from bottom logical node.
    // If the column is aliased, the found name may not be used in upper node.

    // Here, we try to check if column reference is already aliased.
    // If so, it replaces the name with aliased name.
    LogicalNode currentNode = block.getCurrentNode();

    // The condition (currentNode.getInSchema().contains(column)) means
    // the column can be used at the current node. So, we don't need to find aliase name.
    Schema currentNodeSchema = null;
    if (currentNode != null) {
      if (currentNode instanceof RelationNode) {
        currentNodeSchema = ((RelationNode) currentNode).getTableSchema();
      } else {
        currentNodeSchema = currentNode.getInSchema();
      }
    }

    if (currentNode != null && !currentNodeSchema.contains(column)
        && currentNode.getType() != NodeType.TABLE_SUBQUERY) {
      List<Column> candidates = TUtil.newList();
      if (block.namedExprsMgr.isAliased(qualifiedName)) {
        String alias = block.namedExprsMgr.getAlias(canonicalName);
        Column found = resolveColumn(block, new ColumnReferenceExpr(alias));
        if (found != null) {
          candidates.add(found);
        }
      }
      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }
    }

    return column;
  }

  private Column resolveColumnWithoutQualifier(QueryBlock block,
                                               ColumnReferenceExpr columnRef)throws PlanningException {
    // Trying to find the column within the current block
    if (block.currentNode != null && block.currentNode.getInSchema() != null) {
      Column found = block.currentNode.getInSchema().getColumn(columnRef.getCanonicalName());
      if (found != null) {
        return found;
      }
    }

    if (block.getLatestNode() != null) {
      Column found = block.getLatestNode().getOutSchema().getColumn(columnRef.getName());
      if (found != null) {
        return found;
      }
    }

    List<Column> candidates = TUtil.newList();
    // Trying to find columns from aliased references.
    if (block.namedExprsMgr.isAliased(columnRef.getCanonicalName())) {
      String originalName = block.namedExprsMgr.getAlias(columnRef.getCanonicalName());
      Column found = resolveColumn(block, new ColumnReferenceExpr(originalName));
      if (found != null) {
        candidates.add(found);
      }
    }
    if (!candidates.isEmpty()) {
      return ensureUniqueColumn(candidates);
    }

    // Trying to find columns from other relations in the current block
    for (RelationNode rel : block.getRelations()) {
      Column found = rel.getTableSchema().getColumn(columnRef.getName());
      if (found != null) {
        candidates.add(found);
      }
    }

    if (!candidates.isEmpty()) {
      return ensureUniqueColumn(candidates);
    }

    // This is an exception case. It means that there are some bugs in other parts.
    LogicalNode blockRootNode = block.getRoot();
    if (blockRootNode != null && blockRootNode.getOutSchema().getColumn(columnRef.getCanonicalName()) != null) {
      throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
    }

    // Trying to find columns from other relations in other blocks
    for (QueryBlock eachBlock : queryBlocks.values()) {
      for (RelationNode rel : eachBlock.getRelations()) {
        Column found = rel.getTableSchema().getColumn(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }
    }

    if (!candidates.isEmpty()) {
      return ensureUniqueColumn(candidates);
    }

    // Trying to find columns from schema in current block.
    if (block.getSchema() != null) {
      Column found = block.getSchema().getColumn(columnRef.getName());
      if (found != null) {
        candidates.add(found);
      }
    }

    if (!candidates.isEmpty()) {
      return ensureUniqueColumn(candidates);
    }

    throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
  }

  private static Column ensureUniqueColumn(List<Column> candidates)
      throws VerifyException {
    if (candidates.size() == 1) {
      return candidates.get(0);
    } else if (candidates.size() > 2) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Column column : candidates) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(column);
      }
      throw new VerifyException("Ambiguous Column Name: " + sb.toString());
    } else {
      return null;
    }
  }

  public String getQueryGraphAsString() {
    StringBuilder sb = new StringBuilder();

    sb.append("\n-----------------------------\n");
    sb.append("Query Block Graph\n");
    sb.append("-----------------------------\n");
    sb.append(queryBlockGraph.toStringGraph(getRootBlock().getName()));
    sb.append("-----------------------------\n");
    sb.append("Optimization Log:\n");
    DirectedGraphCursor<String, BlockEdge> cursor =
        new DirectedGraphCursor<String, BlockEdge>(queryBlockGraph, getRootBlock().getName());
    while(cursor.hasNext()) {
      QueryBlock block = getBlock(cursor.nextBlock());
      if (block.getPlanHistory().size() > 0) {
        sb.append("\n[").append(block.getName()).append("]\n");
        for (String log : block.getPlanHistory()) {
          sb.append("> ").append(log).append("\n");
        }
      }
    }
    sb.append("-----------------------------\n");
    sb.append("\n");

    sb.append(getLogicalPlanAsString());

    return sb.toString();
  }

  public String getLogicalPlanAsString() {
    ExplainLogicalPlanVisitor explain = new ExplainLogicalPlanVisitor();

    StringBuilder explains = new StringBuilder();
    try {
      ExplainLogicalPlanVisitor.Context explainContext = explain.getBlockPlanStrings(this, getRootBlock().getRoot());
      while(!explainContext.explains.empty()) {
        explains.append(
            ExplainLogicalPlanVisitor.printDepthString(explainContext.getMaxDepth(), explainContext.explains.pop()));
      }
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }

    return explains.toString();
  }

  public void addHistory(String string) {
    planingHistory.add(string);
  }

  public List<String> getHistory() {
    return planingHistory;
  }

  @Override
  public String toString() {
    return getQueryGraphAsString();
  }

  ///////////////////////////////////////////////////////////////////////////
  //                             Query Block
  ///////////////////////////////////////////////////////////////////////////

  public static enum BlockType {
    TableSubQuery,
    ScalarSubQuery
  }

  public static class BlockEdge {
    private String childName;
    private String parentName;
    private BlockType blockType;


    public BlockEdge(String childName, String parentName, BlockType blockType) {
      this.childName = childName;
      this.parentName = parentName;
      this.blockType = blockType;
    }

    public BlockEdge(QueryBlock child, QueryBlock parent, BlockType blockType) {
      this(child.getName(), parent.getName(), blockType);
    }

    public String getParentName() {
      return parentName;
    }

    public String getChildName() {
      return childName;
    }

    public BlockType getBlockType() {
      return blockType;
    }
  }

  public class QueryBlock {
    private final String blockName;
    private LogicalNode rootNode;
    private NodeType rootType;

    // transient states
    private final Map<String, RelationNode> canonicalNameToRelationMap = TUtil.newHashMap();
    private final Map<String, List<String>> aliasMap = TUtil.newHashMap();
    private final Map<OpType, List<Expr>> operatorToExprMap = TUtil.newHashMap();
    private boolean hasWindowFunction = false;
    /**
     * It's a map between nodetype and node. node types can be duplicated. So, latest node type is only kept.
     */
    private final Map<NodeType, LogicalNode> nodeTypeToNodeMap = TUtil.newHashMap();
    private final Map<String, LogicalNode> exprToNodeMap = TUtil.newHashMap();
    final NamedExprsManager namedExprsMgr;

    private LogicalNode currentNode;
    private LogicalNode latestNode;
    private final Set<JoinType> includedJoinTypes = TUtil.newHashSet();
    /**
     * Set true value if this query block has either implicit or explicit aggregation.
     */
    private boolean aggregationRequired = false;
    private Schema schema;

    /** It contains a planning log for this block */
    private final List<String> planingHistory = Lists.newArrayList();
    /** It is for debugging or unit tests */
    private Target [] rawTargets;

    public QueryBlock(String blockName) {
      this.blockName = blockName;
      this.namedExprsMgr = new NamedExprsManager(LogicalPlan.this);
    }

    public String getName() {
      return blockName;
    }

    public void refresh() {
      setRoot(rootNode);
    }

    public void setRoot(LogicalNode blockRoot) {
      this.rootNode = blockRoot;
      if (blockRoot instanceof LogicalRootNode) {
        LogicalRootNode rootNode = (LogicalRootNode) blockRoot;
        rootType = rootNode.getChild().getType();
      }
    }

    public <NODE extends LogicalNode> NODE getRoot() {
      return (NODE) rootNode;
    }

    public NodeType getRootType() {
      return rootType;
    }

    public Target [] getRawTargets() {
      return rawTargets;
    }

    public void setRawTargets(Target[] rawTargets) {
      this.rawTargets = rawTargets;
    }

    public boolean existsRelation(String name) {
      return canonicalNameToRelationMap.containsKey(name);
    }

    public boolean isAlreadyRenamedTableName(String name) {
      return aliasMap.containsKey(name);
    }

    public RelationNode getRelation(String name) {
      if (canonicalNameToRelationMap.containsKey(name)) {
        return canonicalNameToRelationMap.get(name);
      }

      if (aliasMap.containsKey(name)) {
        return canonicalNameToRelationMap.get(aliasMap.get(name).get(0));
      }

      return null;
    }

    public void addRelation(RelationNode relation) {
      if (relation.hasAlias()) {
        TUtil.putToNestedList(aliasMap, relation.getTableName(), relation.getCanonicalName());
      }
      canonicalNameToRelationMap.put(relation.getCanonicalName(), relation);
    }

    public Collection<RelationNode> getRelations() {
      return this.canonicalNameToRelationMap.values();
    }

    public boolean hasTableExpression() {
      return this.canonicalNameToRelationMap.size() > 0;
    }

    public void setSchema(Schema schema) {
      this.schema = schema;
    }

    public Schema getSchema() {
      return schema;
    }

    public NamedExprsManager getNamedExprsManager() {
      return namedExprsMgr;
    }

    public void updateCurrentNode(Expr expr) throws PlanningException {

      if (expr.getType() != OpType.RelationList) { // skip relation list because it is a virtual expr.
        this.currentNode = exprToNodeMap.get(ObjectUtils.identityToString(expr));
        if (currentNode == null) {
          throw new PlanningException("Unregistered Algebra Expression: " + expr.getType());
        }
      }
    }

    @SuppressWarnings("unchecked")
    public <T extends LogicalNode> T getCurrentNode() {
      return (T) this.currentNode;
    }

    public void updateLatestNode(LogicalNode node) {
      this.latestNode = node;
    }

    public <T extends LogicalNode> T getLatestNode() {
      return (T) this.latestNode;
    }

    public void setAlgebraicExpr(Expr expr) {
      TUtil.putToNestedList(operatorToExprMap, expr.getType(), expr);
    }

    public boolean hasAlgebraicExpr(OpType opType) {
      return operatorToExprMap.containsKey(opType);
    }

    public <T extends Expr> List<T> getAlgebraicExpr(OpType opType) {
      return (List<T>) operatorToExprMap.get(opType);
    }

    public <T extends Expr> T getSingletonExpr(OpType opType) {
      if (hasAlgebraicExpr(opType)) {
        return (T) operatorToExprMap.get(opType).get(0);
      } else {
        return null;
      }
    }

    public boolean hasNode(NodeType nodeType) {
      return nodeTypeToNodeMap.containsKey(nodeType);
    }

    public void registerNode(LogicalNode node) {
      // id -> node
      nodeMap.put(node.getPID(), node);

      // So, this is only for filter, groupby, sort, limit, projection, which exists once at a query block.
      nodeTypeToNodeMap.put(node.getType(), node);

      queryBlockByPID.put(node.getPID(), this);
    }

    public void unregisterNode(LogicalNode node) {
      nodeMap.remove(node.getPID());
      nodeTypeToNodeMap.remove(node.getType());
      queryBlockByPID.remove(node.getPID());
    }

    @SuppressWarnings("unchecked")
    public <T extends LogicalNode> T getNode(NodeType nodeType) {
      return (T) nodeTypeToNodeMap.get(nodeType);
    }

    // expr -> node
    public void registerExprWithNode(Expr expr, LogicalNode node) {
      exprToNodeMap.put(ObjectUtils.identityToString(expr), node);
    }

    public <T extends LogicalNode> T getNodeFromExpr(Expr expr) {
      return (T) exprToNodeMap.get(ObjectUtils.identityToString(expr));
    }

    public void setHasWindowFunction() {
      hasWindowFunction = true;
    }

    public boolean hasWindowSpecs() {
      return hasWindowFunction;
    }
//
//    public String addWindowSpecs(WindowSpecExpr windowSpecExpr) {
//      if (windowSpecExpr.hasWindowName()) {
//        if (!windowSpecs.containsKey(windowSpecExpr.getWindowName())) {
//          windowSpecs.put(windowSpecExpr.getWindowName(), windowSpecExpr);
//        }
//        return windowSpecExpr.getWindowName();
//      } else {
//        WindowSpecExpr newWindowSpec = null;
//
//        try {
//          newWindowSpec = (WindowSpecExpr) windowSpecExpr.clone();
//        } catch (CloneNotSupportedException e) {
//          throw new RuntimeException(e);
//        }
//        newWindowSpec.setWindowName(null); // remove name
//
//        String windowName = null;
//        for (WindowSpecExpr existing : windowSpecs.values()) {
//          WindowSpecExpr noNameExistingWindow = null;
//          try {
//            noNameExistingWindow = (WindowSpecExpr) windowSpecExpr.clone();
//          } catch (CloneNotSupportedException e) {
//            throw new RuntimeException(e);
//          }
//          if (newWindowSpec.equals(noNameExistingWindow)) {
//            windowName = existing.getWindowName();
//            break;
//          }
//        }
//
//        if (windowName == null) {
//          windowName = NONAMED_WINDOW_PREFIX + "window" + noNameWindowId++;
//        }
//
//        newWindowSpec.setWindowName(windowName);
//        windowSpecs.put(newWindowSpec.getWindowName(), newWindowSpec);
//        return windowName;
//      }
//    }
//
//    public Map<String, WindowSpecExpr> getWindowSpecs() {
//      return windowSpecs;
//    }

    /**
     * This flag can be changed as a plan is generated.
     *
     * True value means that this query should have aggregation phase. If aggregation plan is added to this block,
     * it becomes false because it doesn't need aggregation phase anymore. It is usually used to add aggregation
     * phase from SELECT statement without group-by clause.
     *
     * @return True if aggregation is needed but this query hasn't had aggregation phase.
     */
    public boolean isAggregationRequired() {
      return this.aggregationRequired;
    }

    /**
     * Unset aggregation required flag. It has to be called after an aggregation phase is added to this block.
     */
    public void unsetAggregationRequire() {
      this.aggregationRequired = false;
    }

    public void setAggregationRequire() {
      aggregationRequired = true;
    }

    public boolean containsJoinType(JoinType joinType) {
      return includedJoinTypes.contains(joinType);
    }

    public void addJoinType(JoinType joinType) {
      includedJoinTypes.add(joinType);
    }

    public List<String> getPlanHistory() {
      return planingHistory;
    }

    public void addPlanHistory(String history) {
      this.planingHistory.add(history);
    }

    public String toString() {
      return blockName;
    }
  }
}