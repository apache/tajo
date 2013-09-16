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

import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Projection;
import org.apache.tajo.annotation.NotThreadSafe;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.planner.graph.DirectedGraphVisitor;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.engine.planner.logical.*;

import java.util.*;

/**
 * This represents and keeps every information about a query plan for a query.
 */
@NotThreadSafe
public class LogicalPlan {
  private final LogicalPlanner planner;

  /** the prefix character for virtual tables */
  public static final char VIRTUAL_TABLE_PREFIX='@';
  /** it indicates the root block */
  public static final String ROOT_BLOCK = VIRTUAL_TABLE_PREFIX + "ROOT";
  /** it indicates a table itself */
  public static final String TABLE_SELF = VIRTUAL_TABLE_PREFIX + "SELF";

  public static final String ANONYMOUS_TABLE_PREFIX = VIRTUAL_TABLE_PREFIX + "NONAME_";
  public static Integer anonymousBlockId = 0;
  public static Integer anonymousColumnId = 0;

  /** a map from between a block name to a block plan */
  private Map<String, QueryBlock> queryBlocks = new LinkedHashMap<String, QueryBlock>();
  private Map<LogicalNode, QueryBlock> queryBlockByNode = new HashMap<LogicalNode, QueryBlock>();
  private SimpleDirectedGraph<QueryBlock, BlockEdge> blockGraph = new SimpleDirectedGraph<QueryBlock, BlockEdge>();
  private Set<LogicalNode> visited = new HashSet<LogicalNode>();

  public LogicalPlan(LogicalPlanner planner) {
    this.planner = planner;
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

  public QueryBlock newAnonymousBlock() {
    return newAndGetBlock(ANONYMOUS_TABLE_PREFIX + (anonymousBlockId++));
  }

  public String newAnonymousColumnName() {
    return "column_" + (anonymousColumnId ++);
  }

  /**
   * Check if a query block exists
   * @param blockName the query block name to be checked
   * @return true if exists. Otherwise, false
   */
  public boolean existBlock(String blockName) {
    return queryBlocks.containsKey(blockName);
  }

  public QueryBlock getRootBlock() {
    return queryBlocks.get(ROOT_BLOCK);
  }

  public QueryBlock getBlock(String blockName) {
    return queryBlocks.get(blockName);
  }

  public QueryBlock getBlock(LogicalNode node) {
    return queryBlockByNode.get(node);
  }

  public void removeBlock(QueryBlock block) {
    queryBlocks.remove(block.getName());
    List<LogicalNode> tobeRemoved = new ArrayList<LogicalNode>();
    for (Map.Entry<LogicalNode, QueryBlock> entry : queryBlockByNode.entrySet()) {
      tobeRemoved.add(entry.getKey());
    }
    for (LogicalNode rn : tobeRemoved) {
      queryBlockByNode.remove(rn);
    }
  }

  public void connectBlocks(QueryBlock srcBlock, QueryBlock targetBlock, BlockType type) {
    blockGraph.connect(srcBlock, targetBlock, new BlockEdge(srcBlock, targetBlock, type));
  }

  public List<QueryBlock> getChildBlocks(QueryBlock block) {
    return blockGraph.getChilds(block);
  }

  public Collection<QueryBlock> getQueryBlocks() {
    return queryBlocks.values();
  }

  public SimpleDirectedGraph<QueryBlock, BlockEdge> getBlockGraph() {
    return blockGraph;
  }

  public boolean postVisit(String blockName, LogicalNode node, Stack<OpType> path) {
    if (visited.contains(node)) {
      return false;
    }

    QueryBlock block = queryBlocks.get(blockName);

    // if an added operator is a relation, add it to relation set.
    switch (node.getType()) {
      case STORE:
        block.setStoreTableNode((StoreTableNode) node);
        break;

      case SCAN:
        ScanNode relationOp = (ScanNode) node;
        block.addRelation(relationOp);
        break;

      case GROUP_BY:
        block.needToResolveGrouping();
        break;

      case SELECTION:
        block.setSelectionNode((SelectionNode) node);
        break;

      case INSERT:
        block.setInsertNode((InsertNode) node);
        break;

      case TABLE_SUBQUERY:
        TableSubQueryNode tableSubQueryNode = (TableSubQueryNode) node;
        block.addRelation(tableSubQueryNode);
        break;
    }


    // if this node is the topmost
    if (path.size() == 0) {
      block.setRoot(node);
    }

    return true;
  }

  /**
   * It tries to find a column with a qualified column name.
   *
   * @throws VerifyException this exception occurs if there is no column matched to the given name.
   */
  public Column findColumnFromRelation(String blockName, String relName, String name)
      throws VerifyException {

    QueryBlock block = queryBlocks.get(blockName);
    RelationNode relationOp = block.getRelation(relName);

    // if a column name is outside of this query block
    if (relationOp == null) {
      // TODO - nested query can only refer outer query block? or not?
      for (QueryBlock eachBlock : queryBlocks.values()) {
        if (eachBlock.containRelation(relName)) {
          relationOp = eachBlock.getRelation(relName);
        }
      }
    }

    if (relationOp == null) {
      throw new NoSuchColumnException(relName + "." + name);
    }

    Schema schema = relationOp.getTableSchema();

    Column column = schema.getColumnByName(name);
    if (column == null) {
      throw new VerifyException("ERROR: no such a column "+ name);
    }

    try {
      column = (Column) column.clone();
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
    String tableName = relationOp.getTableName();
    column.setName(tableName + "." + column.getColumnName());

    return column;
  }

  /**
   * Try to find column from the output of child plans.
   *
   * @throws VerifyException
   */
  public Column findColumnFromChildNode(ColumnReferenceExpr columnRef, String blockName,
                                        LogicalNode node)
      throws VerifyException {
    List<Column> candidates = new ArrayList<Column>();

    Column candidate;
    if (columnRef.hasTableName()) {
      candidate = node.getOutSchema().getColumnByFQN(columnRef.getCanonicalName());

      if (candidate == null) { // If not found, try to find the column with alias name
        String tableName = getBlock(blockName).getRelation(columnRef.getTableName()).getTableName();
        candidate = node.getOutSchema().getColumnByFQN(tableName + "." + columnRef.getName());
      }
      candidates.add(candidate);

    } else {
      candidate = node.getOutSchema().getColumnByName(columnRef.getName());
      candidates.add(candidate);
    }

    if (candidates.isEmpty()) {
      throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
    } else  if (candidates.size() > 1) {
      throw new VerifyException("ERROR: column name "+ columnRef.getCanonicalName()
          + " is ambiguous");
    }

    return candidates.get(0);
  }


  public Column findColumn(String blockName, ColumnReferenceExpr columnRef) throws VerifyException {
    if (columnRef.hasTableName()) {
      return findColumnFromRelation(blockName, columnRef.getTableName(), columnRef.getName());
    } else {
      return suspectColumn(blockName, columnRef.getName());
    }
  }

  /**
   * This method tries to find one column with only column name.
   * If it do not find any column corresponding to the given name, it tries to find the column from other blocks.
   * If it finds two or more columns corresponding to the given name, it incurs @{link VerifyException}.
   *
   * @param blockName The block name is the first priority block used for searching a column corresponding to the name.
   * @param name The column name to be found
   * @return the found column
   *
   * @throws VerifyException If there are two or more found columns, the exception will be caused.
   */
  public Column suspectColumn(String blockName, String name) throws VerifyException {
    List<Column> candidates = new ArrayList<Column>();
    Column candidate;

    // Try to find a column from the current query block
    for (RelationNode rel : queryBlocks.get(blockName).getRelations()) {
      candidate = findColumnFromRelationOp(rel, name);

      if (candidate != null) {
        if (!blockName.equals(LogicalPlan.ROOT_BLOCK)) {
          try {
            candidate = (Column) candidate.clone();
          } catch (CloneNotSupportedException e) {
            e.printStackTrace();
          }
          candidate.setName(rel.getTableName() + "." + candidate.getColumnName());
        }
        candidates.add(candidate);
        if (candidates.size() > 0) {
          break;
        }
      }
    }

    // if a column is not found, try to find the column from outer blocks.
    if (candidates.isEmpty()) {
      // for each block
      Outer:
      for (QueryBlock block : queryBlocks.values()) {
        for (RelationNode rel : block.getRelations()) {
          candidate = findColumnFromRelationOp(rel, name);

          if (candidate != null) {
            if (!blockName.equals(LogicalPlan.ROOT_BLOCK)) {
              try {
                candidate = (Column) candidate.clone();
              } catch (CloneNotSupportedException e) {
                e.printStackTrace();
              }
              candidate.setName(rel.getTableName() + "." + candidate.getColumnName());
            }
            candidates.add(candidate);
            if (candidates.size() > 0)
              break Outer;
          }
        }
      }
    }

    if (candidates.isEmpty()) {
      throw new VerifyException("ERROR: no such a column '"+ name + "'");
    } else  if (candidates.size() > 1) {
      throw new VerifyException("ERROR: column name "+ name + " is ambiguous");
    }

    return candidates.get(0);
  }

  private Column findColumnFromRelationOp(RelationNode relation, String name) throws VerifyException {
    Column candidate = relation.getTableSchema().getColumnByName(name);
    if (candidate != null) {
      try {
        candidate = (Column) candidate.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
      if (!isVirtualRelation(relation.getCanonicalName())) {
        candidate.setName(relation.getTableName() + "." + name);
      }

      return candidate;
    } else {
      return null;
    }
  }

  public static boolean isVirtualRelation(String relationName) {
    return relationName.charAt(0) == VIRTUAL_TABLE_PREFIX;
  }

  public class QueryBlock {
    private String blockName;
    private LogicalNode rootNode;
    private NodeType rootType;
    private Map<String, RelationNode> relations = new HashMap<String, RelationNode>();
    private Projection projection;

    private boolean resolvedGrouping = true;
    private boolean hasGrouping;
    private Projectable projectionNode;
    private SelectionNode selectionNode;
    private GroupbyNode groupingNode;
    private StoreTableNode storeTableNode;
    private InsertNode insertNode;
    private Schema schema;

    TargetListManager targetListManager;

    public QueryBlock(String blockName) {
      this.blockName = blockName;
    }

    public String getName() {
      return blockName;
    }

    public boolean hasRoot() {
      return rootNode != null;
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
      queryBlockByNode.put(blockRoot, this);
    }

    public <T extends LogicalNode> T getRoot() {
      return (T) rootNode;
    }

    public NodeType getRootType() {
      return rootType;
    }

    public boolean containRelation(String name) {
      return relations.containsKey(name);
    }

    public void addRelation(RelationNode relation) {
      relations.put(relation.getCanonicalName(), relation);
    }

    public RelationNode getRelation(String name) {
      return relations.get(name);
    }

    public Collection<RelationNode> getRelations() {
      return this.relations.values();
    }

    public void setSchema(Schema schema) {
      this.schema = schema;
    }

    public Schema getSchema() {
      return schema;
    }

    public boolean hasTableExpression() {
      return this.relations.size() > 0;
    }

    public void setProjection(Projection projection) {
      this.projection = projection;
    }

    public Projection getProjection() {
      return this.projection;
    }

    public void setProjectionNode(Projectable node) {
      this.projectionNode = node;
    }

    public Projectable getProjectionNode() {
      return this.projectionNode;
    }

    public boolean isGroupingResolved() {
      return this.resolvedGrouping;
    }

    public void needToResolveGrouping() {
      this.resolvedGrouping = true;
    }

    public void setHasGrouping() {
      hasGrouping = true;
      resolvedGrouping = false;
    }

    public boolean hasGrouping() {
      return hasGrouping || hasGroupingNode();
    }

    public boolean hasGroupingNode() {
      return this.groupingNode != null;
    }

    public void setGroupingNode(GroupbyNode groupingNode) {
      this.groupingNode = groupingNode;
    }

    public GroupbyNode getGroupingNode() {
      return this.groupingNode;
    }

    public boolean hasSelectionNode() {
      return this.selectionNode != null;
    }

    public void setSelectionNode(SelectionNode selectionNode) {
      this.selectionNode = selectionNode;
    }

    public SelectionNode getSelectionNode() {
      return selectionNode;
    }

    public boolean hasStoreTableNode() {
      return this.storeTableNode != null;
    }

    public void setStoreTableNode(StoreTableNode storeTableNode) {
      this.storeTableNode = storeTableNode;
    }

    public StoreTableNode getStoreTableNode() {
      return this.storeTableNode;
    }

    public boolean hasInsertNode() {
      return this.insertNode != null;
    }

    public InsertNode getInsertNode() {
      return this.insertNode;
    }

    public void setInsertNode(InsertNode insertNode) {
      this.insertNode = insertNode;
    }

    public String toString() {
      return blockName;
    }

    public boolean isTargetEvaluated(int targetId) {
      return targetListManager.isEvaluated(targetId);
    }

    public boolean canTargetEvaluated(int targetId, LogicalNode node) {
      return (targetListManager.getTarget(targetId) != null) &&
          PlannerUtil.canBeEvaluated(targetListManager.getTarget(targetId).getEvalTree(), node);
    }

    /**
     * It requires node's default output schemas.
     * @param node
     */
    public void checkAndSetEvaluatedTargets(LogicalNode node) throws PlanningException {
      if (!(node instanceof Projectable)) {
        return;
      }

      // If all columns are projected and do not include any expression
      if (projection.isAllProjected() && node instanceof RelationNode) {
        targetListManager = new TargetListManager(LogicalPlan.this, PlannerUtil.schemaToTargets(node.getInSchema()));
        targetListManager.setEvaluatedAll();

      } else {

        // fill a target if an annotated target can be created.
        // Some targets which are based on multiple relations can be created only in a certain
        // join node.
        for (int i = 0; i < targetListManager.size(); i++) {
          if (targetListManager.getTarget(i) == null) {
            try {
              targetListManager.updateTarget(i, planner.createTarget(LogicalPlan.this, blockName,
                  projection.getTargets()[i]));
            } catch (VerifyException e) {
            }
          }
        }

        // add target to list if a target can be evaluated at this node
        List<Integer> newEvaluatedTargetIds = new ArrayList<Integer>();
        for (int i = 0; i < targetListManager.size(); i++) {

          if (targetListManager.getTarget(i) != null && !isTargetEvaluated(i)) {
            EvalNode expr = targetListManager.getTarget(i).getEvalTree();

            if (canTargetEvaluated(i, node)) {

              if (node instanceof RelationNode) { // for scan node
                if (expr.getType() == EvalType.FIELD) {
                  targetListManager.setEvaluated(i);
                  if (targetListManager.getTarget(i).hasAlias()) {
                    newEvaluatedTargetIds.add(i);
                  }
                } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
                  // if this expression does no contain any aggregation function
                  targetListManager.setEvaluated(i);
                  newEvaluatedTargetIds.add(i);
                }

              } else if (node instanceof GroupbyNode) { // for grouping
                if (EvalTreeUtil.findDistinctAggFunction(expr).size() > 0) {
                  targetListManager.setEvaluated(i);
                  newEvaluatedTargetIds.add(i);
                }

              } else if (node instanceof JoinNode) { // for join
                if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
                  // if this expression does no contain any aggregation function,
                  targetListManager.setEvaluated(i);
                  newEvaluatedTargetIds.add(i);
                }
              }
            }
          }
        }

        if (node instanceof ScanNode || node instanceof JoinNode) {
          Schema baseSchema = null;
          if (node instanceof ScanNode) {
            baseSchema = ((ScanNode)node).getTableSchema();
          } else if (node instanceof JoinNode) {
            baseSchema = node.getInSchema();
          }

          if (newEvaluatedTargetIds.size() > 0) {
            // fill addedTargets with output columns and new expression columns (e.g., aliased column or expressions)
            Target[] addedTargets = new Target[baseSchema.getColumnNum() + newEvaluatedTargetIds.size()];
            PlannerUtil.schemaToTargets(baseSchema, addedTargets);
            int baseIdx = baseSchema.getColumnNum();
            for (int i = 0; i < newEvaluatedTargetIds.size(); i++) {
              addedTargets[baseIdx + i] = targetListManager.getTarget(newEvaluatedTargetIds.get(i));
            }

            // set targets to ScanNode because it needs to evaluate expressions
            ((Projectable)node).setTargets(addedTargets);
            // the output schema of ScanNode has to have the combination of the original output and newly-added targets.
            node.setOutSchema(LogicalPlanner.getProjectedSchema(LogicalPlan.this, addedTargets));
          } else {
            node.setOutSchema(node.getInSchema());
          }

          // if newEvaluatedTargetIds == 0, the original output schema will be used.
        } else if (node instanceof GroupbyNode) {
          // Set the current targets to the GroupByNode because the GroupByNode is the last projection operator.
          ((Projectable)node).setTargets(targetListManager.getTargets());
          node.setOutSchema(targetListManager.getUpdatedSchema());
        }
      }

      // replace the evaluated targets for upper operators
      targetListManager.getUpdatedTarget();


      if (targetListManager.isAllEvaluated()) {
        schema = targetListManager.getUpdatedSchema();
      }
    }

    public TargetListManager getTargetListManager() {
      return targetListManager;
    }

    public Target[] getCurrentTargets() {
      return targetListManager.getTargets();
    }
  }

  public String getQueryGraphAsString() {
    StringBuilder sb = new StringBuilder();

    sb.append("-----------------------------\n");
    sb.append("Query Block Graph\n");
    sb.append("-----------------------------\n");
    sb.append(blockGraph.toStringGraph(getRootBlock()));
    sb.append("-----------------------------\n");

    sb.append("\n");

    sb.append(getLogicalPlanAsString());

    return sb.toString();
  }

  public String getLogicalPlanAsString() {
    ExplainLogicalPlanVisitor explain = new ExplainLogicalPlanVisitor();

    StringBuilder explains = new StringBuilder();
    try {
    ExplainLogicalPlanVisitor.Context explainContext = explain.getBlockPlanStrings(this, ROOT_BLOCK);
    while(!explainContext.explains.empty()) {
      explains.append(
          ExplainLogicalPlanVisitor.printDepthString(explainContext.getMaxDepth(), explainContext.explains.pop()));
    }
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    return explains.toString();
  }

  @Override
  public String toString() {
    return getQueryGraphAsString();
  }

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
}