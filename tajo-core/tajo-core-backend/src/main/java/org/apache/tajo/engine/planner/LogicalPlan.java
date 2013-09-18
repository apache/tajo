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
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.util.TUtil;

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
  private SimpleDirectedGraph<String, BlockEdge> queryBlockGraph = new SimpleDirectedGraph<String, BlockEdge>();
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
    return "?_" + (anonymousColumnId ++);
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
    queryBlockGraph.connect(srcBlock.getName(), targetBlock.getName(), new BlockEdge(srcBlock, targetBlock, type));
  }

  public QueryBlock getParentBlock(QueryBlock block) {
    return queryBlocks.get(queryBlockGraph.getParent(block.getName()));
  }

  public List<QueryBlock> getChildBlocks(QueryBlock block) {
    List<QueryBlock> childBlocks = TUtil.newList();
    for (String blockName : queryBlockGraph.getChilds(block.getName())) {
      childBlocks.add(queryBlocks.get(blockName));
    }
    return childBlocks;
  }

  public Collection<QueryBlock> getQueryBlocks() {
    return queryBlocks.values();
  }

  public SimpleDirectedGraph<String, BlockEdge> getQueryBlockGraph() {
    return queryBlockGraph;
  }

  /**
   * It resolves a column.
   */
  public Column resolveColumn(QueryBlock block, LogicalNode currentNode, ColumnReferenceExpr columnRef)
      throws VerifyException {

    if (columnRef.hasQualifier()) { // if a column referenec is qualified

      RelationNode relationOp = block.getRelation(columnRef.getQualifier());

      // if a column name is outside of this query block
      if (relationOp == null) {
        // TODO - nested query can only refer outer query block? or not?
        for (QueryBlock eachBlock : queryBlocks.values()) {
          if (eachBlock.containRelation(columnRef.getQualifier())) {
            relationOp = eachBlock.getRelation(columnRef.getQualifier());
          }
        }
      }

      if (relationOp == null) {
        throw new NoSuchColumnException(columnRef.getCanonicalName());
      }

      Schema schema = relationOp.getTableSchema();

      Column column = schema.getColumnByFQN(columnRef.getCanonicalName());
      if (column == null) {
        throw new VerifyException("ERROR: no such a column '"+ column.getQualifiedName() + "'");
      }

      return column;

    } else { // if a column reference is not qualified

      // if current logical node is available
      if (currentNode != null && currentNode.getOutSchema() != null) {
        Column found = currentNode.getOutSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          return found;
        }
      }

      if (block.getLatestNode() != null) {
        Column found = block.getLatestNode().getOutSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          return found;
        }
      }

      // Trying to find columns from other relations in the current block
      List<Column> candidates = TUtil.newList();
      for (RelationNode rel : block.getRelations()) {
        Column found = rel.getOutSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

      // Trying to find columns from other relations in other blocks
      for (QueryBlock eachBlock : queryBlocks.values()) {
        for (RelationNode rel : eachBlock.getRelations()) {
          Column found = rel.getOutSchema().getColumnByName(columnRef.getName());
          if (found != null) {
            candidates.add(found);
          }
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

      throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
    }
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

    sb.append("-----------------------------\n");
    sb.append("Query Block Graph\n");
    sb.append("-----------------------------\n");
    sb.append(queryBlockGraph.toStringGraph(getRootBlock().getName()));
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
    private String blockName;
    private LogicalNode rootNode;
    private NodeType rootType;
    private Map<String, RelationNode> relations = new HashMap<String, RelationNode>();
    private Projection projection;

    // changing states
    private LogicalNode latestNode;
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

    public void setLatestNode(LogicalNode node) {
      this.latestNode = node;
    }

    public <T extends LogicalNode> T getLatestNode() {
      return (T) this.latestNode;
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

    public boolean postVisit(LogicalNode node, Stack<OpType> path) {
      if (visited.contains(node)) {
        return false;
      }

      // if an added operator is a relation, add it to relation set.
      switch (node.getType()) {
        case STORE:
          setStoreTableNode((StoreTableNode) node);
          break;

        case SCAN:
          ScanNode relationOp = (ScanNode) node;
          addRelation(relationOp);
          break;

        case GROUP_BY:
          needToResolveGrouping();
          break;

        case SELECTION:
          setSelectionNode((SelectionNode) node);
          break;

        case INSERT:
          setInsertNode((InsertNode) node);
          break;

        case TABLE_SUBQUERY:
          TableSubQueryNode tableSubQueryNode = (TableSubQueryNode) node;
          addRelation(tableSubQueryNode);
          break;
      }

      setLatestNode(node);

      // if this node is the topmost
      if (path.size() == 0) {
        setRoot(node);
      }

      return true;
    }

    public String toString() {
      return blockName;
    }

    ///////////////////////////////////////////////////////////////////////////
    //                 Target List Management Methods
    ///////////////////////////////////////////////////////////////////////////
    //
    // A target list means a list of expressions after SELECT keyword in SQL.
    //
    // SELECT rel1.col1, sum(rel1.col2), res1.col3 + res2.col2, ... FROM ...
    //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                        TARGET LIST
    //
    ///////////////////////////////////////////////////////////////////////////

    public void initTargetList(Target[] original) {
      targetListManager = new TargetListManager(LogicalPlan.this, original);
    }

    public boolean isTargetResolved(int targetId) {
      return targetListManager.isEvaluated(targetId);
    }

    public void resolveAllTargetList() {
      targetListManager.resolveAll();
    }

    public void resolveTarget(int idx) {
      targetListManager.resolve(idx);
    }

    public boolean isAlreadyTargetCreated(int idx) {
      return getTarget(idx) != null;
    }

    public Target getTarget(int idx) {
      return targetListManager.getTarget(idx);
    }

    public int getTargetListNum() {
      return targetListManager.size();
    }

    public void fillTarget(int idx) throws VerifyException {
      targetListManager.update(idx, planner.createTarget(LogicalPlan.this, this, projection.getTargets()[idx]));
    }

    public boolean checkIfTargetCanBeEvaluated(int targetId, LogicalNode node) {
      return isAlreadyTargetCreated(targetId)
          && PlannerUtil.canBeEvaluated(targetListManager.getTarget(targetId).getEvalTree(), node);
    }

    public TargetListManager getTargetListManager() {
      return targetListManager;
    }

    public Target [] getCurrentTargets() {
      return targetListManager.getTargets();
    }

    public Schema updateSchema() {
      return targetListManager.getUpdatedSchema();
    }

    public void fillTargets() {
      for (int i = 0; i < getTargetListNum(); i++) {
        if (!isAlreadyTargetCreated(i)) {
          try {
            fillTarget(i);
          } catch (VerifyException e) {
          }
        }
      }
    }

    public void checkAndResolveTargets(LogicalNode node) throws PlanningException {
      // If all columns are projected and do not include any expression
      if (projection.isAllProjected() && node instanceof RelationNode) {
        initTargetList(PlannerUtil.schemaToTargets(node.getOutSchema()));
        resolveAllTargetList();

      } else {
        // fill a target if an annotated target can be created.
        // Some targets which are based on multiple relations can be created only in a join node.
        fillTargets();

        // add target to list if a target can be evaluated at this node
        List<Integer> newEvaluatedTargetIds = new ArrayList<Integer>();
        for (int i = 0; i < getTargetListNum(); i++) {

          if (getTarget(i) != null && !isTargetResolved(i)) {
            EvalNode expr = getTarget(i).getEvalTree();

            if (checkIfTargetCanBeEvaluated(i, node)) {

              if (node instanceof RelationNode) { // for scan node
                if (expr.getType() == EvalType.FIELD) {
                  resolveTarget(i);
                  if (getTarget(i).hasAlias()) {
                    newEvaluatedTargetIds.add(i);
                  }
                } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
                  // if this expression does no contain any aggregation function
                  resolveTarget(i);
                  newEvaluatedTargetIds.add(i);
                }

              } else if (node instanceof GroupbyNode) { // for grouping
                if (EvalTreeUtil.findDistinctAggFunction(expr).size() > 0) {
                  resolveTarget(i);
                  newEvaluatedTargetIds.add(i);
                }

              } else if (node instanceof JoinNode) { // for join
                if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
                  // if this expression does no contain any aggregation function,
                  resolveTarget(i);
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
            baseSchema = node.getInSchema(); // composite schema
          }

          if (newEvaluatedTargetIds.size() > 0) {
            // fill addedTargets with output columns and new expression columns (e.g., aliased column or expressions)
            Target[] addedTargets = new Target[baseSchema.getColumnNum() + newEvaluatedTargetIds.size()];
            PlannerUtil.schemaToTargets(baseSchema, addedTargets);
            int baseIdx = baseSchema.getColumnNum();
            for (int i = 0; i < newEvaluatedTargetIds.size(); i++) {
              addedTargets[baseIdx + i] = getTarget(newEvaluatedTargetIds.get(i));
            }

            // set targets to ScanNode because it needs to evaluate expressions
            ((Projectable)node).setTargets(addedTargets);
            // the output schema of ScanNode has to have the combination of the original output and newly-added targets.
            node.setOutSchema(LogicalPlanner.getProjectedSchema(LogicalPlan.this, addedTargets));
          } else {
            // if newEvaluatedTargetIds == 0, the original input schema will be used as the output schema.
            node.setOutSchema(node.getInSchema());
          }
        } else if (node instanceof GroupbyNode) {
          // Set the current targets to the GroupByNode because the GroupByNode is the last projection operator.
          ((Projectable)node).setTargets(getCurrentTargets());
          node.setOutSchema(updateSchema());
        }
      }
    }
  }
}