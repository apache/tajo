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

package org.apache.tajo.engine.planner.global;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.rewrite.ProjectionPushDownRule;
import org.apache.tajo.storage.AbstractStorageManager;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.*;

/**
 * Build DAG
 */
public class GlobalPlanner {
  private static Log LOG = LogFactory.getLog(GlobalPlanner.class);

  private TajoConf conf;
  private CatalogProtos.StoreType storeType;

  public GlobalPlanner(final TajoConf conf, final AbstractStorageManager sm) throws IOException {
    this.conf = conf;
    this.storeType = CatalogProtos.StoreType.valueOf(conf.getVar(ConfVars.SHUFFLE_FILE_FORMAT).toUpperCase());
    Preconditions.checkArgument(storeType != null);
  }

  public class GlobalPlanContext {
    MasterPlan plan;
    Map<Integer, ExecutionBlock> execBlockMap = Maps.newHashMap();
  }

  /**
   * Builds a master plan from the given logical plan.
   */
  public void build(MasterPlan masterPlan) throws IOException, PlanningException {

    DistributedPlannerVisitor planner = new DistributedPlannerVisitor();
    GlobalPlanContext globalPlanContext = new GlobalPlanContext();
    globalPlanContext.plan = masterPlan;
    LOG.info(masterPlan.getLogicalPlan());

    // copy a logical plan in order to keep the original logical plan. The distributed planner can modify
    // an input logical plan.
    LogicalNode inputPlan = PlannerUtil.clone(masterPlan.getLogicalPlan(),
        masterPlan.getLogicalPlan().getRootBlock().getRoot());

    // create a distributed execution plan by visiting each logical node.
    // Its output is a graph, where each vertex is an execution block, and each edge is a data channel.
    // MasterPlan contains them.
    LogicalNode lastNode = planner.visit(globalPlanContext,
        masterPlan.getLogicalPlan(), masterPlan.getLogicalPlan().getRootBlock(), inputPlan, new Stack<LogicalNode>());
    ExecutionBlock childExecBlock = globalPlanContext.execBlockMap.get(lastNode.getPID());

    ExecutionBlock terminalBlock;
    // TODO - consider two terminal types: specified output or not
    if (childExecBlock.getPlan() != null) {
      terminalBlock = masterPlan.createTerminalBlock();
      DataChannel finalChannel = new DataChannel(childExecBlock.getId(), terminalBlock.getId());
      setFinalOutputChannel(finalChannel, lastNode.getOutSchema());
      masterPlan.addConnect(finalChannel);
    } else { // if one or more unions is terminal
      terminalBlock = childExecBlock;
      for (DataChannel outputChannel : masterPlan.getIncomingChannels(terminalBlock.getId())) {
        setFinalOutputChannel(outputChannel, lastNode.getOutSchema());
      }
    }

    masterPlan.setTerminal(terminalBlock);
    LOG.info(masterPlan);
  }

  private static void setFinalOutputChannel(DataChannel outputChannel, Schema outputSchema) {
    outputChannel.setShuffleType(NONE_SHUFFLE);
    outputChannel.setShuffleOutputNum(1);
    outputChannel.setStoreType(CatalogProtos.StoreType.CSV);
    outputChannel.setSchema(outputSchema);
  }

  public static ScanNode buildInputExecutor(LogicalPlan plan, DataChannel channel) {
    Preconditions.checkArgument(channel.getSchema() != null,
        "Channel schema (" + channel.getSrcId().getId() +" -> "+ channel.getTargetId().getId()+") is not initialized");
    TableMeta meta = new TableMeta(channel.getStoreType(), new Options());
    TableDesc desc = new TableDesc(channel.getSrcId().toString(), channel.getSchema(), meta, new Path("/"));
    return new ScanNode(plan.newPID(), desc);
  }

  private DataChannel createDataChannelFromJoin(ExecutionBlock leftBlock, ExecutionBlock rightBlock,
                                                ExecutionBlock parent, JoinNode join, boolean leftTable) {
    ExecutionBlock childBlock = leftTable ? leftBlock : rightBlock;

    DataChannel channel = new DataChannel(childBlock, parent, HASH_SHUFFLE, 32);
    channel.setStoreType(storeType);
    if (join.getJoinType() != JoinType.CROSS) {
      Column [][] joinColumns = PlannerUtil.joinJoinKeyForEachTable(join.getJoinQual(),
          leftBlock.getPlan().getOutSchema(), rightBlock.getPlan().getOutSchema());
      if (leftTable) {
        channel.setShuffleKeys(joinColumns[0]);
      } else {
        channel.setShuffleKeys(joinColumns[1]);
      }
    }
    return channel;
  }

  /**
   * It calculates the total volume of all descendent relation nodes.
   */
  public static long computeDescendentVolume(LogicalNode node) throws PlanningException {

    if (node instanceof RelationNode) {
      switch (node.getType()) {
      case SCAN:
      case PARTITIONS_SCAN:
        ScanNode scanNode = (ScanNode) node;
        if (scanNode.getTableDesc().getStats() == null) {
          // TODO - this case means that data is not located in HDFS. So, we need additional
          // broadcast method.
          return Long.MAX_VALUE;
        } else {
          return scanNode.getTableDesc().getStats().getNumBytes();
        }
      case TABLE_SUBQUERY:
        return computeDescendentVolume(((TableSubQueryNode) node).getSubQuery());
      default:
        throw new IllegalArgumentException("Not RelationNode");
      }
    } else if (node instanceof UnaryNode) {
      return computeDescendentVolume(((UnaryNode) node).getChild());
    } else if (node instanceof BinaryNode) {
      BinaryNode binaryNode = (BinaryNode) node;
      return computeDescendentVolume(binaryNode.getLeftChild()) + computeDescendentVolume(binaryNode.getRightChild());
    }

    throw new PlanningException("Invalid State");
  }

  private static boolean checkIfCanBeOneOfBroadcastJoin(LogicalNode node) {
    return node.getType() == NodeType.SCAN || node.getType() == NodeType.PARTITIONS_SCAN;
  }

  private ExecutionBlock buildJoinPlan(GlobalPlanContext context, JoinNode joinNode,
                                       ExecutionBlock leftBlock, ExecutionBlock rightBlock)
      throws PlanningException {
    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    LogicalNode leftNode = joinNode.getLeftChild();
    LogicalNode rightNode = joinNode.getRightChild();

    boolean leftBroadcasted = false;
    boolean rightBroadcasted = false;

    if (checkIfCanBeOneOfBroadcastJoin(leftNode) && checkIfCanBeOneOfBroadcastJoin(rightNode)) {
      ScanNode leftScan = (ScanNode) leftNode;
      ScanNode rightScan = (ScanNode) rightNode;

      TableDesc leftDesc = leftScan.getTableDesc();
      TableDesc rightDesc = rightScan.getTableDesc();
      long broadcastThreshold = conf.getLongVar(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD);

      if (leftDesc.getStats().getNumBytes() < broadcastThreshold) {
        leftBroadcasted = true;
      }
      if (rightDesc.getStats().getNumBytes() < broadcastThreshold) {
        rightBroadcasted = true;
      }

      if (leftBroadcasted || rightBroadcasted) {
        currentBlock = masterPlan.newExecutionBlock();
        currentBlock.setPlan(joinNode);
        if (leftBroadcasted) {
          currentBlock.addBroadcastTable(leftScan.getCanonicalName());
          LOG.info("The left table " + rightScan.getCanonicalName() + " ("
              + rightScan.getTableDesc().getStats().getNumBytes() + ") is marked a broadcasted table");
        }
        if (rightBroadcasted) {
          currentBlock.addBroadcastTable(rightScan.getCanonicalName());
          LOG.info("The right table " + rightScan.getCanonicalName() + " ("
              + rightScan.getTableDesc().getStats().getNumBytes() + ") is marked a broadcasted table");
        }

        context.execBlockMap.remove(leftScan.getPID());
        context.execBlockMap.remove(rightScan.getPID());
        return currentBlock;
      }
    }

    // symmetric repartition join
    currentBlock = masterPlan.newExecutionBlock();

    DataChannel leftChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, true);
    DataChannel rightChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, false);

    ScanNode leftScan = buildInputExecutor(masterPlan.getLogicalPlan(), leftChannel);
    ScanNode rightScan = buildInputExecutor(masterPlan.getLogicalPlan(), rightChannel);

    joinNode.setLeftChild(leftScan);
    joinNode.setRightChild(rightScan);
    currentBlock.setPlan(joinNode);

    masterPlan.addConnect(leftChannel);
    masterPlan.addConnect(rightChannel);

    return currentBlock;
  }

  /**
   * If a query contains a distinct aggregation function, the query does not
   * perform pre-aggregation in the first phase. Instead, in the fist phase,
   * the query performs only hash shuffle. Then, the query performs the
   * sort aggregation in the second phase. At that time, the aggregation
   * function should be executed as the first phase.
   */
  private ExecutionBlock buildDistinctGroupBy(GlobalPlanContext context, ExecutionBlock childBlock,
                                              GroupbyNode groupbyNode) {
    // setup child block
    LogicalNode topMostOfFirstPhase = groupbyNode.getChild();
    childBlock.setPlan(topMostOfFirstPhase);

    // setup current block
    ExecutionBlock currentBlock = context.plan.newExecutionBlock();
    LinkedHashSet<Column> columnsForDistinct = new LinkedHashSet<Column>();
    for (AggregationFunctionCallEval function : groupbyNode.getAggFunctions()) {
      if (function.isDistinct()) {
        columnsForDistinct.addAll(EvalTreeUtil.findDistinctRefColumns(function));
      } else {
        // See the comment of this method. the aggregation function should be executed as the first phase.
        function.setFirstPhase();
      }
    }

    // Set sort aggregation enforcer to the second groupby node
    Set<Column> existingColumns = Sets.newHashSet(groupbyNode.getGroupingColumns());
    columnsForDistinct.removeAll(existingColumns); // remove existing grouping columns
    SortSpec [] sortSpecs = PlannerUtil.columnsToSortSpec(columnsForDistinct);
    currentBlock.getEnforcer().enforceSortAggregation(groupbyNode.getPID(), sortSpecs);


    // setup channel
    DataChannel channel;
    channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 32);
    channel.setShuffleKeys(groupbyNode.getGroupingColumns());
    channel.setSchema(topMostOfFirstPhase.getOutSchema());
    channel.setStoreType(storeType);

    // setup current block with channel
    ScanNode scanNode = buildInputExecutor(context.plan.getLogicalPlan(), channel);
    groupbyNode.setChild(scanNode);
    currentBlock.setPlan(groupbyNode);
    context.plan.addConnect(channel);

    return currentBlock;
  }

  private ExecutionBlock buildGroupBy(GlobalPlanContext context, ExecutionBlock lastBlock,
                                      GroupbyNode groupbyNode) throws PlanningException {

    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    if (groupbyNode.isDistinct()) { // if there is at one distinct aggregation function
      return buildDistinctGroupBy(context, lastBlock, groupbyNode);
    } else {
      GroupbyNode firstPhaseGroupby = createFirstPhaseGroupBy(masterPlan.getLogicalPlan(), groupbyNode);

      if (hasUnionChild(firstPhaseGroupby)) {
        currentBlock = buildGroupbyAndUnionPlan(masterPlan, lastBlock, firstPhaseGroupby, groupbyNode);
      } else {
        // general hash-shuffled aggregation
        currentBlock = buildTwoPhaseGroupby(masterPlan, lastBlock, firstPhaseGroupby, groupbyNode);
      }
    }

    return currentBlock;
  }

  public boolean hasUnionChild(UnaryNode node) {

    // there are two cases:
    //
    // The first case is:
    //
    //  create table [tbname] as select * from ( select ... UNION select ...) T
    //
    // We can generalize this case as 'a store operator on the top of union'.
    // In this case, a store operator determines a shuffle method.
    //
    // The second case is:
    //
    // select avg(..) from (select ... UNION select ) T
    //
    // We can generalize this case as 'a shuffle required operator on the top of union'.

    if (node.getChild() instanceof UnaryNode) { // first case
      UnaryNode child = node.getChild();

      if (child.getChild().getType() == NodeType.PROJECTION) {
        child = child.getChild();
      }

      if (child.getChild().getType() == NodeType.TABLE_SUBQUERY) {
        TableSubQueryNode tableSubQuery = child.getChild();
        return tableSubQuery.getSubQuery().getType() == NodeType.UNION;
      }

    } else if (node.getChild().getType() == NodeType.TABLE_SUBQUERY) { // second case
      TableSubQueryNode tableSubQuery = node.getChild();
      return tableSubQuery.getSubQuery().getType() == NodeType.UNION;
    }

    return false;
  }

  private ExecutionBlock buildGroupbyAndUnionPlan(MasterPlan masterPlan, ExecutionBlock lastBlock,
                                                  GroupbyNode firstPhaseGroupBy, GroupbyNode secondPhaseGroupBy) {
    DataChannel lastDataChannel = null;

    // It pushes down the first phase group-by operator into all child blocks.
    //
    // (second phase)    G (currentBlock)
    //                  /|\
    //                / / | \
    // (first phase) G G  G  G (child block)

    // They are already connected one another.
    // So, we don't need to connect them again.
    for (DataChannel dataChannel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      if (firstPhaseGroupBy.isEmptyGrouping()) {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 1);
      } else {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 32);
      }
      dataChannel.setSchema(firstPhaseGroupBy.getOutSchema());
      ExecutionBlock childBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

      // Why must firstPhaseGroupby be copied?
      //
      // A groupby in each execution block can have different child.
      // It affects groupby's input schema.
      GroupbyNode firstPhaseGroupbyCopy = PlannerUtil.clone(masterPlan.getLogicalPlan(), firstPhaseGroupBy);
      firstPhaseGroupbyCopy.setChild(childBlock.getPlan());
      childBlock.setPlan(firstPhaseGroupbyCopy);

      // just keep the last data channel.
      lastDataChannel = dataChannel;
    }

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), lastDataChannel);
    secondPhaseGroupBy.setChild(scanNode);
    lastBlock.setPlan(secondPhaseGroupBy);
    return lastBlock;
  }

  private ExecutionBlock buildTwoPhaseGroupby(MasterPlan masterPlan, ExecutionBlock latestBlock,
                                                     GroupbyNode firstPhaseGroupby, GroupbyNode secondPhaseGroupby) {
    ExecutionBlock childBlock = latestBlock;
    childBlock.setPlan(firstPhaseGroupby);
    ExecutionBlock currentBlock = masterPlan.newExecutionBlock();

    DataChannel channel;
    if (firstPhaseGroupby.isEmptyGrouping()) {
      channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 1);
      channel.setShuffleKeys(firstPhaseGroupby.getGroupingColumns());
    } else {
      channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 32);
      channel.setShuffleKeys(firstPhaseGroupby.getGroupingColumns());
    }
    channel.setSchema(firstPhaseGroupby.getOutSchema());
    channel.setStoreType(storeType);

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
    secondPhaseGroupby.setChild(scanNode);
    secondPhaseGroupby.setInSchema(scanNode.getOutSchema());
    currentBlock.setPlan(secondPhaseGroupby);

    masterPlan.addConnect(channel);

    return currentBlock;
  }

  public static GroupbyNode createFirstPhaseGroupBy(LogicalPlan plan, GroupbyNode groupBy) {
    Preconditions.checkNotNull(groupBy);

    GroupbyNode firstPhaseGroupBy = PlannerUtil.clone(plan, groupBy);
    GroupbyNode secondPhaseGroupBy = groupBy;

    // Set first phase expressions
    if (secondPhaseGroupBy.hasAggFunctions()) {
      int evalNum = secondPhaseGroupBy.getAggFunctions().length;
      AggregationFunctionCallEval [] secondPhaseEvals = secondPhaseGroupBy.getAggFunctions();
      AggregationFunctionCallEval [] firstPhaseEvals = new AggregationFunctionCallEval[evalNum];

      String [] firstPhaseEvalNames = new String[evalNum];
      for (int i = 0; i < evalNum; i++) {
        try {
          firstPhaseEvals[i] = (AggregationFunctionCallEval) secondPhaseEvals[i].clone();
        } catch (CloneNotSupportedException e) {
          throw new RuntimeException(e);
        }

        firstPhaseEvals[i].setFirstPhase();
        firstPhaseEvalNames[i] = plan.newGeneratedFieldName(firstPhaseEvals[i]);
        FieldEval param = new FieldEval(firstPhaseEvalNames[i], firstPhaseEvals[i].getValueType());
        secondPhaseEvals[i].setArgs(new EvalNode[] {param});
      }

      secondPhaseGroupBy.setAggFunctions(secondPhaseEvals);
      firstPhaseGroupBy.setAggFunctions(firstPhaseEvals);
      Target [] firstPhaseTargets = ProjectionPushDownRule.buildGroupByTarget(firstPhaseGroupBy, firstPhaseEvalNames);
      firstPhaseGroupBy.setTargets(firstPhaseTargets);
      secondPhaseGroupBy.setInSchema(PlannerUtil.targetToSchema(firstPhaseTargets));
    }
    return firstPhaseGroupBy;
  }

  private ExecutionBlock buildSortPlan(GlobalPlanContext context, ExecutionBlock childBlock, SortNode currentNode) {
    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    SortNode firstSortNode = PlannerUtil.clone(context.plan.getLogicalPlan(), currentNode);

    if (firstSortNode.getChild().getType() == NodeType.TABLE_SUBQUERY &&
        ((TableSubQueryNode)firstSortNode.getChild()).getSubQuery().getType() == NodeType.UNION) {

      currentBlock = childBlock;
      for (DataChannel channel : masterPlan.getIncomingChannels(childBlock.getId())) {
        channel.setShuffle(RANGE_SHUFFLE, PlannerUtil.sortSpecsToSchema(currentNode.getSortKeys()).toArray(), 32);
        channel.setSchema(firstSortNode.getOutSchema());

        ExecutionBlock subBlock = masterPlan.getExecBlock(channel.getSrcId());
        SortNode s1 = PlannerUtil.clone(context.plan.getLogicalPlan(), firstSortNode);
        s1.setChild(subBlock.getPlan());
        subBlock.setPlan(s1);

        ScanNode secondScan = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
        currentNode.setChild(secondScan);
        currentNode.setInSchema(secondScan.getOutSchema());
        currentBlock.setPlan(currentNode);
        currentBlock.getEnforcer().addSortedInput(secondScan.getTableName(), currentNode.getSortKeys());
      }
    } else {
      LogicalNode childBlockPlan = childBlock.getPlan();
      firstSortNode.setChild(childBlockPlan);
      // sort is a non-projectable operator. So, in/out schemas are the same to its child operator.
      firstSortNode.setInSchema(childBlockPlan.getOutSchema());
      firstSortNode.setOutSchema(childBlockPlan.getOutSchema());
      childBlock.setPlan(firstSortNode);

      currentBlock = masterPlan.newExecutionBlock();
      DataChannel channel = new DataChannel(childBlock, currentBlock, RANGE_SHUFFLE, 32);
      channel.setShuffleKeys(PlannerUtil.sortSpecsToSchema(currentNode.getSortKeys()).toArray());
      channel.setSchema(firstSortNode.getOutSchema());

      ScanNode secondScan = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
      currentNode.setChild(secondScan);
      currentNode.setInSchema(secondScan.getOutSchema());
      currentBlock.setPlan(currentNode);
      currentBlock.getEnforcer().addSortedInput(secondScan.getTableName(), currentNode.getSortKeys());
      masterPlan.addConnect(channel);
    }

    return currentBlock;
  }

  /**
   * It builds a distributed execution block for CTAS, InsertNode, and StoreTableNode.
   */
  private ExecutionBlock buildStorePlan(GlobalPlanContext context,
                                        ExecutionBlock lastBlock,
                                        StoreTableNode currentNode) throws PlanningException {


    if(currentNode.hasPartition()) { // if a target table is a partitioned table

      // Verify supported partition types
      PartitionMethodDesc partitionMethod = currentNode.getPartitionMethod();
      if (partitionMethod.getPartitionType() != CatalogProtos.PartitionType.COLUMN) {
        throw new PlanningException(String.format("Not supported partitionsType :%s",
            partitionMethod.getPartitionType()));
      }

      if (hasUnionChild(currentNode)) { // if it has union children
        return buildShuffleAndStorePlanToPartitionedTableWithUnion(context, currentNode, lastBlock);
      } else { // otherwise
        return buildShuffleAndStorePlanToPartitionedTable(context, currentNode, lastBlock);
      }
    } else { // if result table is not a partitioned table, directly store it
      return buildNoPartitionedStorePlan(context, currentNode, lastBlock);
    }
  }

  /**
   * It makes a plan to store directly union plans into a non-partitioned table.
   */
  private ExecutionBlock buildShuffleAndStorePlanNoPartitionedTableWithUnion(GlobalPlanContext context,
                                                                             StoreTableNode currentNode,
                                                                             ExecutionBlock childBlock) {
    for (ExecutionBlock grandChildBlock : context.plan.getChilds(childBlock)) {
      StoreTableNode copy = PlannerUtil.clone(context.plan.getLogicalPlan(), currentNode);
      copy.setChild(grandChildBlock.getPlan());
      grandChildBlock.setPlan(copy);
    }
    return childBlock;
  }

  /**
   * It inserts shuffle and adds store plan on a partitioned table,
   * and it push downs those plans into child unions.
   */
  private ExecutionBlock buildShuffleAndStorePlanToPartitionedTableWithUnion(GlobalPlanContext context,
                                                                             StoreTableNode currentNode,
                                                                             ExecutionBlock lastBlock)
      throws PlanningException {

    MasterPlan masterPlan = context.plan;
    DataChannel lastChannel = null;
    for (DataChannel channel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      ExecutionBlock childBlock = masterPlan.getExecBlock(channel.getSrcId());
      setShuffleKeysFromPartitionedTableStore(currentNode, channel);
      channel.setSchema(childBlock.getPlan().getOutSchema());
      channel.setStoreType(storeType);
      lastChannel = channel;
    }

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), lastChannel);
    currentNode.setChild(scanNode);
    currentNode.setInSchema(scanNode.getOutSchema());
    lastBlock.setPlan(currentNode);
    return lastBlock;
  }

  /**
   * It inserts shuffle and adds store plan on a partitioned table.
   */
  private ExecutionBlock buildShuffleAndStorePlanToPartitionedTable(GlobalPlanContext context,
                                                                    StoreTableNode currentNode,
                                                                    ExecutionBlock lastBlock)
      throws PlanningException {
    MasterPlan masterPlan = context.plan;

    ExecutionBlock nextBlock = masterPlan.newExecutionBlock();
    DataChannel channel = new DataChannel(lastBlock, nextBlock, HASH_SHUFFLE, 32);
    setShuffleKeysFromPartitionedTableStore(currentNode, channel);
    channel.setSchema(lastBlock.getPlan().getOutSchema());
    channel.setStoreType(storeType);

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
    currentNode.setChild(scanNode);
    currentNode.setInSchema(scanNode.getOutSchema());
    nextBlock.setPlan(currentNode);

    masterPlan.addConnect(channel);

    return nextBlock;
  }

  private ExecutionBlock buildNoPartitionedStorePlan(GlobalPlanContext context,
                                                     StoreTableNode currentNode,
                                                     ExecutionBlock childBlock) {
    if (hasUnionChild(currentNode)) { // when the below is union
      return buildShuffleAndStorePlanNoPartitionedTableWithUnion(context, currentNode, childBlock);
    } else {
      currentNode.setChild(childBlock.getPlan());
      currentNode.setInSchema(childBlock.getPlan().getOutSchema());
      childBlock.setPlan(currentNode);
      return childBlock;
    }
  }

  private void setShuffleKeysFromPartitionedTableStore(StoreTableNode node, DataChannel channel) {
    Preconditions.checkState(node.hasTargetTable(), "A target table must be a partitioned table.");
    PartitionMethodDesc partitionMethod = node.getPartitionMethod();

    if (node.getType() == NodeType.INSERT) {
      InsertNode insertNode = (InsertNode) node;
      channel.setSchema(((InsertNode)node).getProjectedSchema());
      Column [] shuffleKeys = new Column[partitionMethod.getExpressionSchema().getColumnNum()];
      int i = 0;
      for (Column column : partitionMethod.getExpressionSchema().getColumns()) {
        int id = insertNode.getTableSchema().getColumnId(column.getQualifiedName());
        shuffleKeys[i++] = insertNode.getProjectedSchema().getColumn(id);
      }
      channel.setShuffleKeys(shuffleKeys);
    } else {
      channel.setShuffleKeys(partitionMethod.getExpressionSchema().toArray());
    }
    channel.setShuffleType(HASH_SHUFFLE);
    channel.setShuffleOutputNum(32);
  }

  public class DistributedPlannerVisitor extends BasicLogicalPlanVisitor<GlobalPlanContext, LogicalNode> {

    @Override
    public LogicalNode visitRoot(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 LogicalRootNode node, Stack<LogicalNode> stack) throws PlanningException {
      return super.visitRoot(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitProjection(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                       ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitProjection(context, plan, block, node, stack);

      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());

      if (child.getType() == NodeType.TABLE_SUBQUERY &&
          ((TableSubQueryNode)child).getSubQuery().getType() == NodeType.UNION) {
        MasterPlan masterPlan = context.plan;
        for (DataChannel dataChannel : masterPlan.getIncomingChannels(execBlock.getId())) {
          ExecutionBlock subBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

          ProjectionNode copy = PlannerUtil.clone(plan, node);
          copy.setChild(subBlock.getPlan());
          subBlock.setPlan(copy);
        }
        execBlock.setPlan(null);
      } else {
        node.setChild(execBlock.getPlan());
        node.setInSchema(execBlock.getPlan().getOutSchema());
        execBlock.setPlan(node);
      }

      context.execBlockMap.put(node.getPID(), execBlock);
      return node;
    }

    @Override
    public LogicalNode visitLimit(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitLimit(context, plan, block, node, stack);

      ExecutionBlock execBlock;
      execBlock = context.execBlockMap.remove(child.getPID());
      if (child.getType() == NodeType.SORT) {
        node.setChild(execBlock.getPlan());
        execBlock.setPlan(node);

        ExecutionBlock childBlock = context.plan.getChild(execBlock, 0);
        LimitNode childLimit = PlannerUtil.clone(context.plan.getLogicalPlan(), node);
        childLimit.setChild(childBlock.getPlan());
        childBlock.setPlan(childLimit);

        DataChannel channel = context.plan.getChannel(childBlock, execBlock);
        channel.setShuffleOutputNum(1);
        context.execBlockMap.put(node.getPID(), execBlock);
      } else {
        node.setChild(execBlock.getPlan());
        execBlock.setPlan(node);

        ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
        DataChannel newChannel = new DataChannel(execBlock, newExecBlock, HASH_SHUFFLE, 1);
        newChannel.setShuffleKeys(new Column[]{});
        newChannel.setSchema(node.getOutSchema());
        newChannel.setStoreType(storeType);

        ScanNode scanNode = buildInputExecutor(plan, newChannel);
        LimitNode parentLimit = PlannerUtil.clone(context.plan.getLogicalPlan(), node);
        parentLimit.setChild(scanNode);
        newExecBlock.setPlan(parentLimit);
        context.plan.addConnect(newChannel);
        context.execBlockMap.put(parentLimit.getPID(), newExecBlock);
        node = parentLimit;
      }

      return node;
    }

    @Override
    public LogicalNode visitSort(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SortNode node, Stack<LogicalNode> stack) throws PlanningException {

      LogicalNode child = super.visitSort(context, plan, block, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildSortPlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitHaving(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    HavingNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitHaving(context, plan, block, node, stack);

      // Don't separate execution block. Having is pushed to the second grouping execution block.
      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      node.setChild(childBlock.getPlan());
      childBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), childBlock);

      return node;
    }

    @Override
    public LogicalNode visitGroupBy(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitGroupBy(context, plan, block, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildGroupBy(context, childBlock, node);
      context.execBlockMap.put(newExecBlock.getPlan().getPID(), newExecBlock);

      return newExecBlock.getPlan();
    }

    @Override
    public LogicalNode visitFilter(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitFilter(context, plan, block, node, stack);

      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());
      node.setChild(execBlock.getPlan());
      node.setInSchema(execBlock.getPlan().getOutSchema());
      execBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    @Override
    public LogicalNode visitJoin(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode leftChild = visit(context, plan, block, node.getLeftChild(), stack);
      LogicalNode rightChild = visit(context, plan, block, node.getRightChild(), stack);

      ExecutionBlock leftChildBlock = context.execBlockMap.get(leftChild.getPID());
      ExecutionBlock rightChildBlock = context.execBlockMap.get(rightChild.getPID());

      ExecutionBlock newExecBlock = buildJoinPlan(context, node, leftChildBlock, rightChildBlock);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitUnion(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                  UnionNode node, Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);
      LogicalPlan.QueryBlock leftQueryBlock = plan.getBlock(node.getLeftChild());
      LogicalNode leftChild = visit(context, plan, leftQueryBlock, leftQueryBlock.getRoot(), stack);

      LogicalPlan.QueryBlock rightQueryBlock = plan.getBlock(node.getRightChild());
      LogicalNode rightChild = visit(context, plan, rightQueryBlock, rightQueryBlock.getRoot(), stack);
      stack.pop();

      List<ExecutionBlock> unionBlocks = Lists.newArrayList();
      List<ExecutionBlock> queryBlockBlocks = Lists.newArrayList();

      ExecutionBlock leftBlock = context.execBlockMap.remove(leftChild.getPID());
      ExecutionBlock rightBlock = context.execBlockMap.remove(rightChild.getPID());
      if (leftChild.getType() == NodeType.UNION) {
        unionBlocks.add(leftBlock);
      } else {
        queryBlockBlocks.add(leftBlock);
      }
      if (rightChild.getType() == NodeType.UNION) {
        unionBlocks.add(rightBlock);
      } else {
        queryBlockBlocks.add(rightBlock);
      }

      ExecutionBlock execBlock;
      if (unionBlocks.size() == 0) {
        execBlock = context.plan.newExecutionBlock();
      } else {
        execBlock = unionBlocks.get(0);
      }

      for (ExecutionBlock childBlocks : unionBlocks) {
        for (ExecutionBlock grandChildBlock : context.plan.getChilds(childBlocks)) {
          queryBlockBlocks.add(grandChildBlock);
        }
      }

      for (ExecutionBlock childBlocks : queryBlockBlocks) {
        DataChannel channel = new DataChannel(childBlocks, execBlock, NONE_SHUFFLE, 1);
        channel.setStoreType(storeType);
        context.plan.addConnect(channel);
      }

      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    private LogicalNode handleUnaryNode(GlobalPlanContext context, LogicalNode child, LogicalNode node) {
      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());
      execBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    @Override
    public LogicalNode visitExcept(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                   ExceptNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitExcept(context, plan, queryBlock, node, stack);
      return handleUnaryNode(context, child, node);
    }

    @Override
    public LogicalNode visitIntersect(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                      IntersectNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitIntersect(context, plan, queryBlock, node, stack);
      return handleUnaryNode(context, child, node);
    }

    @Override
    public LogicalNode visitTableSubQuery(GlobalPlanContext context, LogicalPlan plan,
                                          LogicalPlan.QueryBlock queryBlock,
                                          TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitTableSubQuery(context, plan, queryBlock, node, stack);
      node.setSubQuery(child);

      ExecutionBlock currentBlock = context.execBlockMap.remove(child.getPID());

      if (child.getType() == NodeType.UNION) {
        for (ExecutionBlock childBlock : context.plan.getChilds(currentBlock.getId())) {
          TableSubQueryNode copy = PlannerUtil.clone(plan, node);
          copy.setSubQuery(childBlock.getPlan());
          childBlock.setPlan(copy);
        }
      } else {
        currentBlock.setPlan(node);
      }
      context.execBlockMap.put(node.getPID(), currentBlock);
      return node;
    }

    @Override
    public LogicalNode visitScan(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                 ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
      ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
      newExecBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), newExecBlock);
      return node;
    }

    @Override
    public LogicalNode visitPartitionedTableScan(GlobalPlanContext context, LogicalPlan plan,
                                                 LogicalPlan.QueryBlock block, PartitionedTableScanNode node,
                                                 Stack<LogicalNode> stack)throws PlanningException {
      ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
      newExecBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), newExecBlock);
      return node;
    }

    @Override
    public LogicalNode visitStoreTable(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                       StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitStoreTable(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitCreateTable(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                       CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitStoreTable(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitInsert(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                   InsertNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      LogicalNode child = super.visitInsert(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }
  }

  @SuppressWarnings("unused")
  private class ConsecutiveUnionFinder extends BasicLogicalPlanVisitor<List<UnionNode>, LogicalNode> {
    @Override
    public LogicalNode visitUnion(List<UnionNode> unionNodeList, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                  UnionNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      if (node.getType() == NodeType.UNION) {
        unionNodeList.add(node);
      }

      stack.push(node);
      TableSubQueryNode leftSubQuery = node.getLeftChild();
      TableSubQueryNode rightSubQuery = node.getRightChild();
      if (leftSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visit(unionNodeList, plan, queryBlock, leftSubQuery, stack);
      }
      if (rightSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visit(unionNodeList, plan, queryBlock, rightSubQuery, stack);
      }
      stack.pop();

      return node;
    }
  }
}
