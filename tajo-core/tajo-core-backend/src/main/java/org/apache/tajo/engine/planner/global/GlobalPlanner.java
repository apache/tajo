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
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.storage.AbstractStorageManager;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.PartitionType.*;

public class GlobalPlanner {
  private static Log LOG = LogFactory.getLog(GlobalPlanner.class);

  private TajoConf conf;
  private AbstractStorageManager sm;

  public GlobalPlanner(final TajoConf conf, final AbstractStorageManager sm)
      throws IOException {
    this.conf = conf;
    this.sm = sm;
  }

  public class GlobalPlanContext {
    MasterPlan plan;
    Set<String> broadcastTables = new HashSet<String>();
    LogicalNode topmost;
    LogicalNode lastRepartionableNode;
    ExecutionBlock topMostLeftExecBlock;
  }

  /**
   * Builds a master plan from the given logical plan.
   */
  public void build(MasterPlan masterPlan)
      throws IOException, PlanningException {

    DistributedPlannerVisitor planner = new DistributedPlannerVisitor();
    GlobalPlanContext globalPlanContext = new GlobalPlanContext();
    globalPlanContext.plan = masterPlan;
    LOG.info(masterPlan.getLogicalPlan());

    LogicalNode rootNode = PlannerUtil.clone(masterPlan.getLogicalPlan().getRootBlock().getRoot());
    planner.visitChild(globalPlanContext, masterPlan.getLogicalPlan(), rootNode, new Stack<LogicalNode>());

    ExecutionBlock terminalBlock = masterPlan.createTerminalBlock();

    if (globalPlanContext.lastRepartionableNode != null
        && globalPlanContext.lastRepartionableNode.getType() == NodeType.UNION) {
      UnionNode unionNode = (UnionNode) globalPlanContext.lastRepartionableNode;
      ConsecutiveUnionFinder finder = new ConsecutiveUnionFinder();
      UnionsFinderContext finderContext = new UnionsFinderContext();
      finder.visitChild(finderContext, masterPlan.getLogicalPlan(), unionNode, new Stack<LogicalNode>());

      for (UnionNode union : finderContext.unionList) {
        TableSubQueryNode leftSubQuery = union.getLeftChild();
        TableSubQueryNode rightSubQuery = union.getRightChild();
        if (leftSubQuery.getSubQuery().getType() != NodeType.UNION) {
          ExecutionBlock execBlock = masterPlan.newExecutionBlock();
          execBlock.setPlan(leftSubQuery);
          DataChannel dataChannel = new DataChannel(execBlock, terminalBlock, NONE_PARTITION, 1);
          masterPlan.addConnect(dataChannel);
        }
        if (rightSubQuery.getSubQuery().getType() != NodeType.UNION) {
          ExecutionBlock execBlock = masterPlan.newExecutionBlock();
          execBlock.setPlan(rightSubQuery);
          DataChannel dataChannel = new DataChannel(execBlock, terminalBlock, NONE_PARTITION, 1);
          masterPlan.addConnect(dataChannel);
        }
      }
    } else {
      DataChannel dataChannel = new DataChannel(globalPlanContext.topMostLeftExecBlock, terminalBlock, NONE_PARTITION, 1);
      dataChannel.setSchema(globalPlanContext.topmost.getOutSchema());
      masterPlan.addConnect(dataChannel);
    }
    masterPlan.setTerminal(terminalBlock);
    LOG.info(masterPlan);
  }

  private ExecutionBlock buildRepartitionBlocks(MasterPlan masterPlan, LogicalNode lastDistNode, LogicalNode curNode,
                                                LogicalNode childNode, ExecutionBlock lastChildBlock)
      throws PlanningException {

    ExecutionBlock currentBlock = null;
    ExecutionBlock childBlock;
    childBlock = lastChildBlock;

    NodeType shuffleRequiredNodeType = lastDistNode.getType();
    if (shuffleRequiredNodeType == NodeType.GROUP_BY) {
      ExecutionBlock [] blocks = buildGroupBy(masterPlan, lastDistNode, curNode, childNode, childBlock);
      currentBlock = blocks[0];
    } else if (shuffleRequiredNodeType == NodeType.SORT) {
      ExecutionBlock [] blocks = buildSortPlan(masterPlan, lastDistNode, curNode, childNode, childBlock);
      currentBlock = blocks[0];
    } else if (shuffleRequiredNodeType == NodeType.JOIN) {
      ExecutionBlock [] blocks = buildJoinPlan(masterPlan, lastDistNode, childBlock, lastChildBlock);
      currentBlock = blocks[0];
    }

    return currentBlock;
  }

  public static ScanNode buildInputExecutor(LogicalPlan plan, DataChannel channel) {
    Preconditions.checkArgument(channel.getSchema() != null,
        "Channel schema (" + channel.getSrcId().getId() +" -> "+ channel.getTargetId().getId()+") is not initialized");
    TableMeta meta = new TableMetaImpl(channel.getSchema(), channel.getStoreType(), new Options());
    TableDesc desc = new TableDescImpl(channel.getSrcId().toString(), meta, new Path("/"));
    return new ScanNode(plan.newPID(), desc);
  }

  private DataChannel createDataChannelFromJoin(ExecutionBlock leftBlock, ExecutionBlock rightBlock,
                                                ExecutionBlock parent, JoinNode join, boolean leftTable) {
    ExecutionBlock childBlock = leftTable ? leftBlock : rightBlock;

    DataChannel channel = new DataChannel(childBlock, parent, HASH_PARTITION, 32);
    if (join.getJoinType() != JoinType.CROSS) {
      Column [][] joinColumns = PlannerUtil.joinJoinKeyForEachTable(join.getJoinQual(),
          leftBlock.getPlan().getOutSchema(), rightBlock.getPlan().getOutSchema());
      if (leftTable) {
        channel.setPartitionKey(joinColumns[0]);
      } else {
        channel.setPartitionKey(joinColumns[1]);
      }
    }
    return channel;
  }

  private ExecutionBlock [] buildJoinPlan(MasterPlan masterPlan, LogicalNode lastDistNode,
                                          ExecutionBlock childBlock, ExecutionBlock lastChildBlock)
      throws PlanningException {
    ExecutionBlock currentBlock;

    JoinNode joinNode = (JoinNode) lastDistNode;
    LogicalNode leftNode = joinNode.getLeftChild();
    LogicalNode rightNode = joinNode.getRightChild();

    boolean leftBroadcasted = false;
    boolean rightBroadcasted = false;

    if (leftNode.getType() == NodeType.SCAN && rightNode.getType() == NodeType.SCAN ) {
      ScanNode leftScan = (ScanNode) leftNode;
      ScanNode rightScan = (ScanNode) rightNode;

      TableMeta leftMeta = leftScan.getTableDesc().getMeta();
      TableMeta rightMeta = rightScan.getTableDesc().getMeta();
      long broadcastThreshold = conf.getLongVar(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD);

      if (leftMeta.getStat().getNumBytes() < broadcastThreshold) {
        leftBroadcasted = true;
      }
      if (rightMeta.getStat().getNumBytes() < broadcastThreshold) {
        rightBroadcasted = true;
      }

      if (leftBroadcasted || rightBroadcasted) {
        currentBlock = masterPlan.newExecutionBlock();
        currentBlock.setPlan(joinNode);
        if (leftBroadcasted) {
          currentBlock.addBroadcastTable(leftScan.getCanonicalName());
        }
        if (rightBroadcasted) {
          currentBlock.addBroadcastTable(rightScan.getCanonicalName());
        }
        return new ExecutionBlock[] { currentBlock, childBlock };
      }
    }

    // symmetric repartition join

    ExecutionBlock leftBlock;
    if (lastChildBlock == null) {
      leftBlock = masterPlan.newExecutionBlock();
      leftBlock.setPlan(leftNode);
    } else {
      leftBlock = lastChildBlock;
    }
    ExecutionBlock rightBlock = masterPlan.newExecutionBlock();
    rightBlock.setPlan(rightNode);

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

    return new ExecutionBlock[] { currentBlock, childBlock };

  }

  private ExecutionBlock [] buildGroupBy(MasterPlan masterPlan, LogicalNode lastDistNode, LogicalNode currentNode,
                                         LogicalNode childNode, ExecutionBlock childBlock) throws PlanningException {
    ExecutionBlock currentBlock = null;
    GroupbyNode groupByNode = (GroupbyNode) lastDistNode;

    if (groupByNode.isDistinct()) {
      if (childBlock == null) { // first repartition node
        childBlock = masterPlan.newExecutionBlock();
      }
      childBlock.setPlan(groupByNode.getChild());
      currentBlock = masterPlan.newExecutionBlock();

      LinkedHashSet<Column> columnsForDistinct = new LinkedHashSet<Column>();

      for (Target target : groupByNode.getTargets()) {
        List<AggregationFunctionCallEval> functions = EvalTreeUtil.findDistinctAggFunction(target.getEvalTree());
        for (AggregationFunctionCallEval function : functions) {
          if (function.isDistinct()) {
            columnsForDistinct.addAll(EvalTreeUtil.findDistinctRefColumns(function));
          }
        }
      }

      Set<Column> existingColumns = Sets.newHashSet(groupByNode.getGroupingColumns());
      columnsForDistinct.removeAll(existingColumns); // remove existing grouping columns
      SortSpec [] sortSpecs = PlannerUtil.columnsToSortSpec(columnsForDistinct);
      currentBlock.getEnforcer().enforceSortAggregation(groupByNode.getPID(), sortSpecs);

      DataChannel channel;
      channel = new DataChannel(childBlock, currentBlock, HASH_PARTITION, 32);
      channel.setPartitionKey(groupByNode.getGroupingColumns());
      channel.setSchema(groupByNode.getInSchema());

      GroupbyNode secondGroupBy = groupByNode;
      ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
      secondGroupBy.setChild(scanNode);

      LogicalNode parent = PlannerUtil.findTopParentNode(currentNode, lastDistNode.getType());
      if (parent instanceof UnaryNode && parent != secondGroupBy) {
        ((UnaryNode)parent).setChild(secondGroupBy);
      }

      masterPlan.addConnect(channel);
      currentBlock.setPlan(currentNode);
      
    } else {

      GroupbyNode firstGroupBy = PlannerUtil.transformGroupbyTo2P(groupByNode);
      firstGroupBy.setHavingCondition(null);

      if (firstGroupBy.getChild().getType() == NodeType.TABLE_SUBQUERY &&
          ((TableSubQueryNode)firstGroupBy.getChild()).getSubQuery().getType() == NodeType.UNION) {

        UnionNode unionNode = PlannerUtil.findTopNode(groupByNode, NodeType.UNION);
        ConsecutiveUnionFinder finder = new ConsecutiveUnionFinder();
        UnionsFinderContext finderContext = new UnionsFinderContext();
        finder.visitChild(finderContext, masterPlan.getLogicalPlan(), unionNode, new Stack<LogicalNode>());

        currentBlock = masterPlan.newExecutionBlock();
        GroupbyNode secondGroupBy = groupByNode;
        for (UnionNode union : finderContext.unionList) {
          TableSubQueryNode leftSubQuery = union.getLeftChild();
          TableSubQueryNode rightSubQuery = union.getRightChild();
          DataChannel dataChannel;
          if (leftSubQuery.getSubQuery().getType() != NodeType.UNION) {
            ExecutionBlock execBlock = masterPlan.newExecutionBlock();
            GroupbyNode g1 = PlannerUtil.clone(firstGroupBy);
            g1.setChild(leftSubQuery);
            execBlock.setPlan(g1);
            dataChannel = new DataChannel(execBlock, currentBlock, HASH_PARTITION, 32);

            ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), dataChannel);
            secondGroupBy.setChild(scanNode);
            masterPlan.addConnect(dataChannel);
          }
          if (rightSubQuery.getSubQuery().getType() != NodeType.UNION) {
            ExecutionBlock execBlock = masterPlan.newExecutionBlock();
            GroupbyNode g1 = PlannerUtil.clone(firstGroupBy);
            g1.setChild(rightSubQuery);
            execBlock.setPlan(g1);
            dataChannel = new DataChannel(execBlock, currentBlock, HASH_PARTITION, 32);

            ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), dataChannel);
            secondGroupBy.setChild(scanNode);
            masterPlan.addConnect(dataChannel);
          }
        }
        LogicalNode parent = PlannerUtil.findTopParentNode(currentNode, lastDistNode.getType());
        if (parent instanceof UnaryNode && parent != secondGroupBy) {
          ((UnaryNode)parent).setChild(secondGroupBy);
        }
        currentBlock.setPlan(currentNode);
      } else {

        if (childBlock == null) { // first repartition node
          childBlock = masterPlan.newExecutionBlock();
        }
        childBlock.setPlan(firstGroupBy);

        currentBlock = masterPlan.newExecutionBlock();

        DataChannel channel;
        if (firstGroupBy.isEmptyGrouping()) {
          channel = new DataChannel(childBlock, currentBlock, HASH_PARTITION, 1);
          channel.setPartitionKey(firstGroupBy.getGroupingColumns());
        } else {
          channel = new DataChannel(childBlock, currentBlock, HASH_PARTITION, 32);
          channel.setPartitionKey(firstGroupBy.getGroupingColumns());
        }
        channel.setSchema(firstGroupBy.getOutSchema());

        GroupbyNode secondGroupBy = groupByNode;
        ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
        secondGroupBy.setChild(scanNode);

        LogicalNode parent = PlannerUtil.findTopParentNode(currentNode, lastDistNode.getType());
        if (parent instanceof UnaryNode && parent != secondGroupBy) {
          ((UnaryNode)parent).setChild(secondGroupBy);
        }

        masterPlan.addConnect(channel);
        currentBlock.setPlan(currentNode);
      }
    }

    return new ExecutionBlock [] {currentBlock, childBlock};
  }

  private ExecutionBlock [] buildSortPlan(MasterPlan masterPlan, LogicalNode lastDistNode, LogicalNode currentNode,
                                          LogicalNode childNode, ExecutionBlock childBlock) {
    ExecutionBlock currentBlock = null;

    SortNode firstSort = (SortNode) lastDistNode;
    if (childBlock == null) {
      childBlock = masterPlan.newExecutionBlock();
    }
    childBlock.setPlan(firstSort);

    currentBlock = masterPlan.newExecutionBlock();
    DataChannel channel = new DataChannel(childBlock, currentBlock, RANGE_PARTITION, 32);
    channel.setPartitionKey(PlannerUtil.sortSpecsToSchema(firstSort.getSortKeys()).toArray());
    channel.setSchema(childNode.getOutSchema());

    SortNode secondSort = PlannerUtil.clone(lastDistNode);
    ScanNode secondScan = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
    secondSort.setChild(secondScan);

    LimitNode limitAndSort;
    LimitNode limitOrNull = PlannerUtil.findTopNode(currentNode, NodeType.LIMIT);
    if (limitOrNull != null) {
      limitAndSort = PlannerUtil.clone(limitOrNull);
      limitAndSort.setChild(firstSort);

      if (childBlock.getPlan().getType() == NodeType.SORT) {
        childBlock.setPlan(limitAndSort);
      } else {
        LogicalNode sortParent = PlannerUtil.findTopParentNode(childBlock.getPlan(), NodeType.SORT);
        if (sortParent != null) {
          if (sortParent instanceof UnaryNode) {
            ((UnaryNode)sortParent).setChild(limitAndSort);
          }
        }
      }
    }

    LogicalNode parent = PlannerUtil.findTopParentNode(currentNode, lastDistNode.getType());
    if (parent instanceof UnaryNode && parent != secondSort) {
      ((UnaryNode)parent).setChild(secondSort);
    }

    masterPlan.addConnect(channel);
    currentBlock.setPlan(currentNode);

    return new ExecutionBlock[] { currentBlock, childBlock };
  }

  public class DistributedPlannerVisitor extends BasicLogicalPlanVisitor<GlobalPlanContext, LogicalNode> {

    @Override
    public LogicalNode visitRoot(GlobalPlanContext context, LogicalPlan plan, LogicalRootNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      super.visitRoot(context, plan, node, stack);

      if (context.lastRepartionableNode != null && context.lastRepartionableNode.getType() != NodeType.UNION) {
        context.topMostLeftExecBlock = buildRepartitionBlocks(context.plan, context.lastRepartionableNode, node, context.topmost, context.topMostLeftExecBlock);
      } else if (context.lastRepartionableNode != null && context.lastRepartionableNode.getType() == NodeType.UNION) {

      } else {
        ExecutionBlock execBlock = context.plan.newExecutionBlock();
        execBlock.setPlan(node);
        context.topMostLeftExecBlock = execBlock;
      }

      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitProjection(GlobalPlanContext context, LogicalPlan plan, ProjectionNode node,
                                       Stack<LogicalNode> stack) throws PlanningException {
      super.visitProjection(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitLimit(GlobalPlanContext context, LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitLimit(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitSort(GlobalPlanContext context, LogicalPlan plan, SortNode node, Stack<LogicalNode> stack)
        throws PlanningException {

      super.visitSort(context, plan, node, stack);

      if (context.lastRepartionableNode != null) {
        context.topMostLeftExecBlock = buildRepartitionBlocks(context.plan, context.lastRepartionableNode, node, context.topmost,
            context.topMostLeftExecBlock);
      }

      context.topmost = node;
      context.lastRepartionableNode = node;

      return node;
    }

    @Override
    public LogicalNode visitGroupBy(GlobalPlanContext context, LogicalPlan plan, GroupbyNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
      super.visitGroupBy(context, plan, node, stack);

      if (context.lastRepartionableNode != null) {
        context.topMostLeftExecBlock = buildRepartitionBlocks(context.plan, context.lastRepartionableNode, node,
            context.topmost, context.topMostLeftExecBlock);
      }

      context.topmost = node;
      context.lastRepartionableNode = node;
      return node;
    }

    @Override
    public LogicalNode visitFilter(GlobalPlanContext context, LogicalPlan plan, SelectionNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      super.visitFilter(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitJoin(GlobalPlanContext context, LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(context, plan, node, stack);

      if (context.lastRepartionableNode != null) {
        context.topMostLeftExecBlock = buildRepartitionBlocks(context.plan, context.lastRepartionableNode, node, context.topmost,
            context.topMostLeftExecBlock);
      }

      context.topmost = node;
      context.lastRepartionableNode = node;

      return node;
    }

    @Override
    public LogicalNode visitUnion(GlobalPlanContext context, LogicalPlan plan, UnionNode node,
                                  Stack<LogicalNode> stack) throws PlanningException {
      super.visitUnion(context, plan, node, stack);

      if (context.lastRepartionableNode != null && context.lastRepartionableNode.getType() != NodeType.UNION) {
        context.topMostLeftExecBlock = buildRepartitionBlocks(context.plan, context.lastRepartionableNode, node,
            context.topmost, context.topMostLeftExecBlock);
      }

      context.topmost = node;
      context.lastRepartionableNode = node;
      return node;
    }

    @Override
    public LogicalNode visitExcept(GlobalPlanContext context, LogicalPlan plan, ExceptNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      super.visitExcept(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitIntersect(GlobalPlanContext context, LogicalPlan plan, IntersectNode node,
                                      Stack<LogicalNode> stack) throws PlanningException {
      super.visitIntersect(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitTableSubQuery(GlobalPlanContext context, LogicalPlan plan, TableSubQueryNode node,
                                          Stack<LogicalNode> stack) throws PlanningException {
      super.visitTableSubQuery(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitScan(GlobalPlanContext context, LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitStoreTable(GlobalPlanContext context, LogicalPlan plan, StoreTableNode node,
                                       Stack<LogicalNode> stack) throws PlanningException {
      super.visitStoreTable(context, plan, node, stack);
      context.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitInsert(GlobalPlanContext context, LogicalPlan plan, InsertNode node,
                                   Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitInsert(context, plan, node, stack);
      context.topmost = node;
      return node;
    }
  }

  private class UnionsFinderContext {
    List<UnionNode> unionList = new ArrayList<UnionNode>();
  }

  @SuppressWarnings("unused")
  private class ConsecutiveUnionFinder extends BasicLogicalPlanVisitor<UnionsFinderContext, LogicalNode> {
    @Override
    public LogicalNode visitUnion(UnionsFinderContext context, LogicalPlan plan, UnionNode node,
                                  Stack<LogicalNode> stack)
        throws PlanningException {
      if (node.getType() == NodeType.UNION) {
        context.unionList.add(node);
      }

      stack.push(node);
      TableSubQueryNode leftSubQuery = node.getLeftChild();
      TableSubQueryNode rightSubQuery = node.getRightChild();
      if (leftSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visitChild(context, plan, leftSubQuery, stack);
      }
      if (rightSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visitChild(context, plan, rightSubQuery, stack);
      }
      stack.pop();

      return node;
    }
  }
}
