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

package org.apache.tajo.master;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.DataChannel;
import org.apache.tajo.catalog.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.global.MasterPlan;
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
    ExecutionBlock topMostRightExecBlock;
  }

  /**
   * Builds a master plan from the given logical plan.
   */
  public void build(MasterPlan masterPlan)
      throws IOException, PlanningException {

    NewPlanner planner = new NewPlanner();
    GlobalPlanContext globalPlanContext = new GlobalPlanContext();
    globalPlanContext.plan = masterPlan;
    LOG.info(masterPlan.getLogicalPlan());

    LogicalNode rootNode = PlannerUtil.clone(masterPlan.getLogicalPlan().getRootBlock().getRoot());
    planner.visitChild(masterPlan.getLogicalPlan(), rootNode, new Stack<LogicalNode>(), globalPlanContext);

    ExecutionBlock terminalBlock = masterPlan.createTerminalBlock();

    if (globalPlanContext.lastRepartionableNode != null
        && globalPlanContext.lastRepartionableNode.getType() == NodeType.UNION) {
      UnionNode unionNode = (UnionNode) globalPlanContext.lastRepartionableNode;
      ConsecutiveUnionFinder finder = new ConsecutiveUnionFinder();
      UnionsFinderContext finderContext = new UnionsFinderContext();
      finder.visitChild(masterPlan.getLogicalPlan(), unionNode, new Stack<LogicalNode>(), finderContext);

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

  public static ScanNode buildInputExecutor(DataChannel channel) {
    Preconditions.checkArgument(channel.getSchema() != null,
        "Channel schema (" + channel.getSrcId().getId() +" -> "+ channel.getTargetId().getId()+") is not initialized");
    TableMeta meta = new TableMetaImpl(channel.getSchema(), channel.getStoreType(), new Options());
    TableDesc desc = new TableDescImpl(channel.getSrcId().toString(), meta, new Path("/"));
    return new ScanNode(new FromTable(desc));
  }

  public class NewPlanner extends BasicLogicalPlanVisitor<GlobalPlanContext> {

    @Override
    public LogicalNode visitRoot(LogicalPlan plan, LogicalRootNode node, Stack<LogicalNode> stack,
                                 GlobalPlanContext data) throws PlanningException {
      super.visitRoot(plan, node, stack, data);

      if (data.lastRepartionableNode != null && data.lastRepartionableNode.getType() != NodeType.UNION) {
        data.topMostLeftExecBlock = addChannel(data.plan, data.lastRepartionableNode, node, data.topmost, data.topMostLeftExecBlock);
      } else if (data.lastRepartionableNode != null && data.lastRepartionableNode.getType() == NodeType.UNION) {

      } else {
        ExecutionBlock execBlock = data.plan.newExecutionBlock();
        execBlock.setPlan(node);
        data.topMostLeftExecBlock = execBlock;
      }

      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitProjection(LogicalPlan plan, ProjectionNode node, Stack<LogicalNode> stack,
                                       GlobalPlanContext data) throws PlanningException {
      super.visitProjection(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitLimit(LogicalPlan plan, LimitNode node, Stack<LogicalNode> stack, GlobalPlanContext data) throws PlanningException {
      super.visitLimit(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    private ExecutionBlock addChannel(MasterPlan masterPlan, LogicalNode lastDistNode, LogicalNode curNode,
                                      LogicalNode childNode, ExecutionBlock lastChildBlock) throws PlanningException {
      ExecutionBlock currentBlock = null;
      ExecutionBlock childBlock;

      childBlock = lastChildBlock;

      NodeType shuffleRequiredNodeType = lastDistNode.getType();
      if (shuffleRequiredNodeType == NodeType.GROUP_BY) {
        GroupbyNode groupByNode = (GroupbyNode) lastDistNode;

        GroupbyNode firstGroupBy = PlannerUtil.transformGroupbyTo2P(groupByNode);

        if (firstGroupBy.getChild().getType() == NodeType.TABLE_SUBQUERY &&
            ((TableSubQueryNode)firstGroupBy.getChild()).getSubQuery().getType() == NodeType.UNION) {

          UnionNode unionNode = PlannerUtil.findTopNode(groupByNode, NodeType.UNION);
          ConsecutiveUnionFinder finder = new ConsecutiveUnionFinder();
          UnionsFinderContext finderContext = new UnionsFinderContext();
          finder.visitChild(masterPlan.getLogicalPlan(), unionNode, new Stack<LogicalNode>(), finderContext);

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

              ScanNode scanNode = buildInputExecutor(dataChannel);
              secondGroupBy.setChild(scanNode);
              masterPlan.addConnect(dataChannel);
            }
            if (rightSubQuery.getSubQuery().getType() != NodeType.UNION) {
              ExecutionBlock execBlock = masterPlan.newExecutionBlock();
              GroupbyNode g1 = PlannerUtil.clone(firstGroupBy);
              g1.setChild(rightSubQuery);
              execBlock.setPlan(g1);
              dataChannel = new DataChannel(execBlock, currentBlock, HASH_PARTITION, 32);

              ScanNode scanNode = buildInputExecutor(dataChannel);
              secondGroupBy.setChild(scanNode);
              masterPlan.addConnect(dataChannel);
            }
          }
          LogicalNode parent = PlannerUtil.findTopParentNode(curNode, lastDistNode.getType());
          if (parent instanceof UnaryNode && parent != secondGroupBy) {
            ((UnaryNode)parent).setChild(secondGroupBy);
          }
          currentBlock.setPlan(curNode);
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
          ScanNode scanNode = buildInputExecutor(channel);
          secondGroupBy.setChild(scanNode);

          LogicalNode parent = PlannerUtil.findTopParentNode(curNode, lastDistNode.getType());
          if (parent instanceof UnaryNode && parent != secondGroupBy) {
            ((UnaryNode)parent).setChild(secondGroupBy);
          }

          masterPlan.addConnect(channel);
          currentBlock.setPlan(curNode);
        }
      } else if (shuffleRequiredNodeType == NodeType.SORT) {
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
        ScanNode secondScan = buildInputExecutor(channel);
        secondSort.setChild(secondScan);

        LimitNode limitAndSort;
        LimitNode limitOrNull = PlannerUtil.findTopNode(curNode, NodeType.LIMIT);
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

        LogicalNode parent = PlannerUtil.findTopParentNode(curNode, lastDistNode.getType());
        if (parent instanceof UnaryNode && parent != secondSort) {
          ((UnaryNode)parent).setChild(secondSort);
        }

        masterPlan.addConnect(channel);
        currentBlock.setPlan(curNode);
      } else if (shuffleRequiredNodeType == NodeType.JOIN) {
        JoinNode joinNode = (JoinNode) lastDistNode;
        LogicalNode leftNode = joinNode.getLeftChild();
        LogicalNode rightNode = joinNode.getRightChild();

        ExecutionBlock leftBlock = null;
        if (lastChildBlock == null) {
          leftBlock = masterPlan.newExecutionBlock();
          leftBlock.setPlan(leftNode);
        } else {
          leftBlock = lastChildBlock;
        }
        ExecutionBlock rightBlock = masterPlan.newExecutionBlock();
        rightBlock.setPlan(rightNode);

        currentBlock = masterPlan.newExecutionBlock();

        DataChannel leftChannel = new DataChannel(leftBlock, currentBlock, HASH_PARTITION, 32);
        DataChannel rightChannel = new DataChannel(rightBlock, currentBlock, HASH_PARTITION, 32);

        ScanNode leftScan = buildInputExecutor(leftChannel);
        ScanNode rightScan = buildInputExecutor(rightChannel);

        joinNode.setLeftChild(leftScan);
        joinNode.setRightChild(rightScan);
        currentBlock.setPlan(joinNode);

        masterPlan.addConnect(leftChannel);
        masterPlan.addConnect(rightChannel);
      }

      return currentBlock;
    }

    @Override
    public LogicalNode visitSort(LogicalPlan plan, SortNode node, Stack<LogicalNode> stack, GlobalPlanContext data)
        throws PlanningException {

      super.visitSort(plan, node, stack, data);

      if (data.lastRepartionableNode != null) {
        data.topMostLeftExecBlock = addChannel(data.plan, data.lastRepartionableNode, node, data.topmost, data.topMostLeftExecBlock);
      }

      data.topmost = node;
      data.lastRepartionableNode = node;

      return node;
    }

    @Override
    public LogicalNode visitGroupBy(LogicalPlan plan, GroupbyNode node, Stack<LogicalNode> stack, GlobalPlanContext data) throws PlanningException {
      super.visitGroupBy(plan, node, stack, data);

      if (data.lastRepartionableNode != null) {
        data.topMostLeftExecBlock = addChannel(data.plan, data.lastRepartionableNode, node, data.topmost, data.topMostLeftExecBlock);
      }

      data.topmost = node;
      data.lastRepartionableNode = node;
      return node;
    }

    @Override
    public LogicalNode visitFilter(LogicalPlan plan, SelectionNode node, Stack<LogicalNode> stack, GlobalPlanContext data) throws PlanningException {
      super.visitFilter(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitJoin(LogicalPlan plan, JoinNode node, Stack<LogicalNode> stack, GlobalPlanContext data) throws PlanningException {
      super.visitJoin(plan, node, stack, data);

      if (data.lastRepartionableNode != null) {
        data.topMostLeftExecBlock = addChannel(data.plan, data.lastRepartionableNode, node, data.topmost, data.topMostLeftExecBlock);
      }

      data.topmost = node;
      data.lastRepartionableNode = node;

      return node;
    }

    @Override
    public LogicalNode visitUnion(LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack, GlobalPlanContext data) throws PlanningException {
      super.visitUnion(plan, node, stack, data);

      if (data.lastRepartionableNode != null && data.lastRepartionableNode.getType() != NodeType.UNION) {
        data.topMostLeftExecBlock = addChannel(data.plan, data.lastRepartionableNode, node, data.topmost,
            data.topMostLeftExecBlock);
      }

      data.topmost = node;
      data.lastRepartionableNode = node;
      return node;
    }

    @Override
    public LogicalNode visitExcept(LogicalPlan plan, ExceptNode node, Stack<LogicalNode> stack,
                                   GlobalPlanContext data) throws PlanningException {
      super.visitExcept(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitIntersect(LogicalPlan plan, IntersectNode node, Stack<LogicalNode> stack,
                                      GlobalPlanContext data) throws PlanningException {
      super.visitIntersect(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitTableSubQuery(LogicalPlan plan, TableSubQueryNode node, Stack<LogicalNode> stack,
                                          GlobalPlanContext data) throws PlanningException {
      super.visitTableSubQuery(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitScan(LogicalPlan plan, ScanNode node, Stack<LogicalNode> stack, GlobalPlanContext data)
        throws PlanningException {
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitStoreTable(LogicalPlan plan, StoreTableNode node, Stack<LogicalNode> stack,
                                       GlobalPlanContext data) throws PlanningException {
      super.visitStoreTable(plan, node, stack, data);
      data.topmost = node;
      return node;
    }

    @Override
    public LogicalNode visitInsert(LogicalPlan plan, InsertNode node, Stack<LogicalNode> stack, GlobalPlanContext data)
        throws PlanningException {
      super.visitInsert(plan, node, stack, data);
      data.topmost = node;
      return node;
    }
  }

  private class UnionsFinderContext {
    List<UnionNode> unionList = new ArrayList<UnionNode>();
  }

  private class ConsecutiveUnionFinder extends BasicLogicalPlanVisitor<UnionsFinderContext> {
    public LogicalNode visitUnion(LogicalPlan plan, UnionNode node, Stack<LogicalNode> stack, UnionsFinderContext data)
        throws PlanningException {
      if (node.getType() == NodeType.UNION) {
        data.unionList.add(node);
      }

      stack.push(node);
      TableSubQueryNode leftSubQuery = node.getLeftChild();
      TableSubQueryNode rightSubQuery = node.getRightChild();
      if (leftSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visitChild(plan, leftSubQuery, stack, data);
      }
      if (rightSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visitChild(plan, rightSubQuery, stack, data);
      }
      stack.pop();

      return node;
    }
  }
}
