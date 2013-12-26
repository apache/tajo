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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.NotThreadSafe;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.graph.DirectedGraphCursor;
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
  public static final String NONAME_BLOCK_PREFIX = VIRTUAL_TABLE_PREFIX + "NONAME_";
  private Integer noNameBlockId = 0;
  private Integer noNameColumnId = 0;

  /** a map from between a block name to a block plan */
  private Map<String, QueryBlock> queryBlocks = new LinkedHashMap<String, QueryBlock>();
  private Map<Integer, LogicalNode> nodeMap = new HashMap<Integer, LogicalNode>();
  private Map<Integer, QueryBlock> queryBlockByPID = new HashMap<Integer, QueryBlock>();
  private SimpleDirectedGraph<String, BlockEdge> queryBlockGraph = new SimpleDirectedGraph<String, BlockEdge>();

  /** planning and optimization log */
  private List<String> planingHistory = Lists.newArrayList();

  private PIDFactory pidFactory = new PIDFactory();

  public static class PIDFactory {
    private int nextPid = 0;

    public int newPID() {
      return nextPid++;
    }
  }

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

  public PIDFactory getPidFactory() {
    return pidFactory;
  }

  public int newPID() {
    return pidFactory.newPID();
  }

  public QueryBlock newNoNameBlock() {
    return newAndGetBlock(NONAME_BLOCK_PREFIX + (noNameBlockId++));
  }

  public String newNonameColumnName(String prefix) {
    String suffix = noNameColumnId == 0 ? "" : String.valueOf(noNameColumnId);
    noNameColumnId++;
    return "?" + prefix + suffix;
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

  public void connectBlocks(QueryBlock srcBlock, QueryBlock targetBlock, BlockType type) {
    Preconditions.checkState(queryBlockGraph.getParentCount(srcBlock.getName()) <= 0,
        "There should be only one parent block.");
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
      throws PlanningException {

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
        throw new VerifyException("ERROR: no such a column '"+ columnRef.getCanonicalName() + "'");
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

      // Trying to find columns from schema in current block.
      if (block.getSchema() != null) {
        Column found = block.getSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

      throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
    }
  }

  /**
   * replace the found column if the column is renamed to an alias name
   */
  public Column getColumnOrAliasedColumn(QueryBlock block, Column column) throws PlanningException {
    if (block.targetListManager.isResolve(column)) {
      column = block.targetListManager.getResolvedColumn(column);
    }
    return column;
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
      if (block.getPlaningHistory().size() > 0) {
        sb.append("\n[").append(block.getName()).append("]\n");
        for (String log : block.getPlaningHistory()) {
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
    private String blockName;
    private LogicalNode rootNode;
    private NodeType rootType;
    private Map<String, RelationNode> relations = new HashMap<String, RelationNode>();
    private Map<OpType, List<Expr>> algebraicExprs = TUtil.newHashMap();

    // changing states
    private LogicalNode latestNode;
    private boolean resolvedGrouping = true;
    private boolean hasGrouping;
    private Projectable projectionNode;
    private GroupbyNode groupingNode;
    private JoinNode joinNode;
    private SelectionNode selectionNode;
    private StoreTableNode storeTableNode;
    private InsertNode insertNode;
    private Schema schema;

    TargetListManager targetListManager;

    /** It contains a planning log for this block */
    private List<String> planingHistory = Lists.newArrayList();

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
      queryBlockByPID.put(blockRoot.getPID(), this);
    }

    public <NODE extends LogicalNode> NODE getRoot() {
      return (NODE) rootNode;
    }

    public NodeType getRootType() {
      return rootType;
    }

    public boolean containRelation(String name) {
      return relations.containsKey(PlannerUtil.normalizeTableName(name));
    }

    public void addRelation(RelationNode relation) {
      relations.put(PlannerUtil.normalizeTableName(relation.getCanonicalName()), relation);
    }

    public RelationNode getRelation(String name) {
      return relations.get(PlannerUtil.normalizeTableName(name));
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

    public void updateLatestNode(LogicalNode node) {
      this.latestNode = node;
    }

    public <T extends LogicalNode> T getLatestNode() {
      return (T) this.latestNode;
    }

    public void setAlgebraicExpr(Expr expr) {
      TUtil.putToNestedList(algebraicExprs, expr.getType(), expr);
    }

    public boolean hasAlgebraicExpr(OpType opType) {
      return algebraicExprs.containsKey(opType);
    }

    public <T extends Expr> List<T> getAlgebraicExpr(OpType opType) {
      return (List<T>) algebraicExprs.get(opType);
    }

    public <T extends Expr> T getSingletonExpr(OpType opType) {
      if (hasAlgebraicExpr(opType)) {
        return (T) algebraicExprs.get(opType).get(0);
      } else {
        return null;
      }
    }

    public boolean hasProjection() {
      return hasAlgebraicExpr(OpType.Projection);
    }

    public Projection getProjection() {
      return getSingletonExpr(OpType.Projection);
    }

    public boolean hasHaving() {
      return hasAlgebraicExpr(OpType.Having);
    }

    public Having getHaving() {
      return getSingletonExpr(OpType.Having);
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

    public void resolveGroupingRequired() {
      this.resolvedGrouping = true;
    }

    public void setHasGrouping() {
      hasGrouping = true;
      resolvedGrouping = false;
    }

    public boolean hasGrouping() {
      return hasGrouping || hasGroupbyNode();
    }

    public boolean hasGroupbyNode() {
      return this.groupingNode != null;
    }

    public void setGroupbyNode(GroupbyNode groupingNode) {
      this.groupingNode = groupingNode;
    }

    public GroupbyNode getGroupbyNode() {
      return this.groupingNode;
    }

    public boolean hasJoinNode() {
      return joinNode != null;
    }

    /**
     * @return the topmost JoinNode instance
     */
    public JoinNode getJoinNode() {
      return joinNode;
    }

    public void setJoinNode(JoinNode node) {
      if (joinNode == null || latestNode == node) {
        this.joinNode = node;
      } else {
        PlannerUtil.replaceNode(LogicalPlan.this, latestNode, this.joinNode, node);
      }
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

    public List<String> getPlaningHistory() {
      return planingHistory;
    }

    public void addHistory(String history) {
      this.planingHistory.add(history);
    }

    public boolean postVisit(LogicalNode node, Stack<OpType> path) {
      if (nodeMap.containsKey(node.getPID())) {
        return false;
      }

      nodeMap.put(node.getPID(), node);
      updateLatestNode(node);

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
          resolveGroupingRequired();
          setGroupbyNode((GroupbyNode) node);
          break;

        case JOIN:
          setJoinNode((JoinNode) node);
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
      return targetListManager.isResolved(targetId);
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

    public void fillTarget(int idx) throws PlanningException {
      Target target = planner.createTarget(LogicalPlan.this, this, getProjection().getTargets()[idx]);
      // below code reaches only when target is created.
      targetListManager.fill(idx, target);
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

    public void fillTargets() throws PlanningException {
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
      if (getProjection().isAllProjected() && node instanceof RelationNode) {
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
          GroupbyNode groupbyNode = (GroupbyNode) node;
          groupbyNode.setTargets(targetListManager.getUpdatedTarget(Sets.newHashSet(newEvaluatedTargetIds)));
          //groupbyNode.setTargets(getCurrentTargets());
          boolean distinct = false;
          for (Target target : groupbyNode.getTargets()) {
            for (AggregationFunctionCallEval aggrFunc : EvalTreeUtil.findDistinctAggFunction(target.getEvalTree())) {
              if (aggrFunc.isDistinct()) {
                distinct = true;
                break;
              }
            }
          }
          groupbyNode.setDistinct(distinct);
          node.setOutSchema(updateSchema());

          // if a having condition is given,
          if (hasHaving()) {
            EvalNode havingCondition = planner.createEvalTree(LogicalPlan.this, this, getHaving().getQual());
            List<AggregationFunctionCallEval> aggrFunctions = EvalTreeUtil.findDistinctAggFunction(havingCondition);

            if (aggrFunctions.size() == 0) {
              groupbyNode.setHavingCondition(havingCondition);
            } else {
              Target [] addedTargets = new Target[aggrFunctions.size()];
              for (int i = 0; i < aggrFunctions.size(); i++) {
                Target aggrFunctionTarget = new Target(aggrFunctions.get(i),
                    newNonameColumnName(aggrFunctions.get(i).getName()));
                addedTargets[i] = aggrFunctionTarget;
                EvalTreeUtil.replace(havingCondition, aggrFunctions.get(i),
                    new FieldEval(aggrFunctionTarget.getColumnSchema()));
              }
              Target [] updatedTargets = TUtil.concat(groupbyNode.getTargets(), addedTargets);
              groupbyNode.setTargets(updatedTargets);
              groupbyNode.setHavingCondition(havingCondition);
              groupbyNode.setHavingSchema(PlannerUtil.targetToSchema(groupbyNode.getTargets()));
            }
          }
        }
      }
    }
  }
}