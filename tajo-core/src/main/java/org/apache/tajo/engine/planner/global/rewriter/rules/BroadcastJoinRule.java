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

package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty.EnforceType;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.logical.*;

import java.util.List;

public class BroadcastJoinRule implements GlobalPlanRewriteRule {
  private long broadcastTableSizeThreshold;
  private ParentFinder parentFinder;

  @Override
  public String getName() {
    return "BroadcastJoinRule";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
//    if (queryContext.getBool(SessionVars.TEST_BROADCAST_JOIN_ENABLED)) {
//      for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
//        if (block.hasNode(NodeType.JOIN)) {
//          broadcastTableSizeThreshold = queryContext.getLong(SessionVars.BROADCAST_TABLE_SIZE_LIMIT);
//          if (broadcastTableSizeThreshold > 0) {
//            if (parentFinder == null) {
//              parentFinder = new ParentFinder();
//            }
//            return true;
//          }
//        }
//      }
//    }
    return false;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) throws PlanningException{
    rewrite(plan, plan.getTerminalBlock());
    return plan;
  }

  private void rewrite(MasterPlan plan, ExecutionBlock current) throws PlanningException {
    if (plan.isLeaf(current)) {
      // in leaf execution blocks, find input tables which size is less than the predefined threshold.
      for (ScanNode scanNode : current.getScanNodes()) {
        if (getTableVolume(scanNode) <= broadcastTableSizeThreshold) {
          current.addBroadcastRelation(scanNode.getCanonicalName());
        }
      }
    } else {
      // in intermediate execution blocks, merge broadcastable children's plan with the current plan.
      for (ExecutionBlock child : plan.getChilds(current)) {
        rewrite(plan, child);
      }
//      if (current.hasJoin()) {
      if (!plan.isTerminal(current)) {
        boolean needMerge = false;
        List<ExecutionBlock> childs = plan.getChilds(current);
        for (ExecutionBlock child : childs) {
          if (child.isBroadcastable(broadcastTableSizeThreshold)) {
            needMerge = true;
            break;
          }
        }
        if (needMerge) {
          for (ExecutionBlock child : childs) {
            merge(plan, child, current);
          }
        }
//      }
      }
    }
  }

  private ExecutionBlock merge(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) throws PlanningException {
    if (parent.hasJoin()) {
      return mergeTwoPhaseJoin(plan, child, parent);
    } else {
      return mergeTwoPhaseNonJoin(plan, child, parent);
    }
  }

  private ExecutionBlock mergeTwoPhaseNonJoin(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent)
      throws PlanningException {

    ScanNode scanForChild = findScanForChildEb(child, parent);
    if (scanForChild == null) {
      throw new PlanningException("Cannot find any scan nodes for " + child.getId() + " in " + parent.getId());
    }

    parentFinder.set(scanForChild);
    parentFinder.find(parent.getPlan());
    LogicalNode parentOfScanForChild = parentFinder.found;
    if (parentOfScanForChild == null) {
      throw new PlanningException("Cannot find the parent of " + scanForChild.getCanonicalName());
    }

    LogicalNode rootOfChild = child.getPlan();
    if (rootOfChild.getType() == NodeType.STORE) {
      rootOfChild = ((StoreTableNode)rootOfChild).getChild();
    }
    LogicalNode mergedPlan;
    if (rootOfChild.getType() == parentOfScanForChild.getType()) {
      // merge two-phase plan into one-phase plan.
      // remove the second-phase plan.
      LogicalNode firstPhaseNode = rootOfChild;
      LogicalNode secondPhaseNode = parentOfScanForChild;

      parentFinder.set(parentOfScanForChild);
      parentFinder.find(parent.getPlan());
      parentOfScanForChild = parentFinder.found;

      if (parentOfScanForChild == null) {
        // assume that the node which will be merged is the root node of the plan of the parent eb.
        mergedPlan = firstPhaseNode;
      } else {
        replaceChild(firstPhaseNode, scanForChild, parentOfScanForChild);
        mergedPlan = parent.getPlan();
      }

      if (firstPhaseNode.getType() == NodeType.GROUP_BY) {
        GroupbyNode firstPhaseGroupby = (GroupbyNode) firstPhaseNode;
        GroupbyNode secondPhaseGroupby = (GroupbyNode) secondPhaseNode;
        for (AggregationFunctionCallEval aggFunc : firstPhaseGroupby.getAggFunctions()) {
          aggFunc.setFirstAndFinalPhase();
        }
        firstPhaseGroupby.setTargets(secondPhaseGroupby.getTargets());
        firstPhaseGroupby.setOutSchema(secondPhaseGroupby.getOutSchema());
      }
    } else {
      mergedPlan = parent.getPlan();
    }

    parent = mergeExecutionBlocks(plan, child, parent);

    if (parent.getEnforcer().hasEnforceProperty(EnforceType.SORTED_INPUT)) {
      parent.getEnforcer().removeSortedInput(scanForChild.getTableName());
    }

    parent.setPlan(mergedPlan);

    return parent;
  }

  /**
   * Merge child execution blocks.
   *
   * @param plan master plan
   * @param child child block
   * @param parent parent block who has join nodes
   * @return
   */
  private ExecutionBlock mergeTwoPhaseJoin(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent)
      throws PlanningException {
    ScanNode scanForChild = findScanForChildEb(child, parent);
    if (scanForChild == null) {
      throw new PlanningException("Cannot find any scan nodes for " + child.getId() + " in " + parent.getId());
    }

    parentFinder.set(scanForChild);
    parentFinder.find(parent.getPlan());
    LogicalNode parentOfScanForChild = parentFinder.found;
    if (parentOfScanForChild == null) {
      throw new PlanningException("Cannot find the parent of " + scanForChild.getCanonicalName());
    }

    LogicalNode rootOfChild = child.getPlan();
    if (rootOfChild.getType() == NodeType.STORE) {
      rootOfChild = ((StoreTableNode)rootOfChild).getChild();
    }

//    if (parentOfScanForChild instanceof UnaryNode) {
//      ((UnaryNode) parentOfScanForChild).setChild(rootOfChild);
//    } else if (parentOfScanForChild instanceof BinaryNode) {
//      BinaryNode binary = (BinaryNode) parentOfScanForChild;
//      if (binary.getLeftChild().equals(scanForChild)) {
//        binary.setLeftChild(rootOfChild);
//      } else if (binary.getRightChild().equals(scanForChild)) {
//        binary.setRightChild(rootOfChild);
//      } else {
//        throw new PlanningException(scanForChild.getPID() + " is not a child of " + parentOfScanForChild.getPID());
//      }
//    } else {
//      throw new PlanningException(parentOfScanForChild + " seems to not have any children");
//    }
    replaceChild(rootOfChild, scanForChild, parentOfScanForChild);

//    for (String broadcastable : child.getBroadcastTables()) {
//      parent.addBroadcastRelation(broadcastable);
//    }
//
//    // connect parent and grand children
//    List<ExecutionBlock> grandChilds = plan.getChilds(child);
//    for (ExecutionBlock eachGrandChild : grandChilds) {
//      plan.addConnect(eachGrandChild, parent, plan.getChannel(eachGrandChild, child).getShuffleType());
//      plan.disconnect(eachGrandChild, child);
//    }
//
//    plan.disconnect(child, parent);
//    List<DataChannel> channels = plan.getIncomingChannels(child.getId());
//    if (channels == null || channels.size() == 0) {
//      channels = plan.getOutgoingChannels(child.getId());
//      if (channels == null || channels.size() == 0) {
//        plan.removeExecBlock(child.getId());
//      }
//    }
    parent = mergeExecutionBlocks(plan, child, parent);

    parent.setPlan(parent.getPlan());

    return parent;
  }

  private static ExecutionBlock mergeExecutionBlocks(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) {
    for (String broadcastable : child.getBroadcastTables()) {
      parent.addBroadcastRelation(broadcastable);
    }

    // connect parent and grand children
    List<ExecutionBlock> grandChilds = plan.getChilds(child);
    for (ExecutionBlock eachGrandChild : grandChilds) {
      plan.addConnect(eachGrandChild, parent, plan.getChannel(eachGrandChild, child).getShuffleType());
      plan.disconnect(eachGrandChild, child);
    }

    plan.disconnect(child, parent);
    List<DataChannel> channels = plan.getIncomingChannels(child.getId());
    if (channels == null || channels.size() == 0) {
      channels = plan.getOutgoingChannels(child.getId());
      if (channels == null || channels.size() == 0) {
        plan.removeExecBlock(child.getId());
      }
    }
    return parent;
  }

  private static void replaceChild(LogicalNode newChild, ScanNode originalChild, LogicalNode parent)
      throws PlanningException {
    if (parent instanceof UnaryNode) {
      ((UnaryNode) parent).setChild(newChild);
    } else if (parent instanceof BinaryNode) {
      BinaryNode binary = (BinaryNode) parent;
      if (binary.getLeftChild().equals(originalChild)) {
        binary.setLeftChild(newChild);
      } else if (binary.getRightChild().equals(originalChild)) {
        binary.setRightChild(newChild);
      } else {
        throw new PlanningException(originalChild.getPID() + " is not a child of " + parent.getPID());
      }
    } else {
      throw new PlanningException(parent.getPID() + " seems to not have any children");
    }
  }

  private static ScanNode findScanForChildEb(ExecutionBlock child, ExecutionBlock parent) {
    ScanNode scanForChild = null;
    for (ScanNode scanNode : parent.getScanNodes()) {
      if (scanNode.getTableName().equals(child.getId().toString())) {
        scanForChild = scanNode;
        break;
      }
    }
    return scanForChild;
  }

  /**
   * Get a volume of a table of a partitioned table
   * @param scanNode ScanNode corresponding to a table
   * @return table volume (bytes)
   */
  private static long getTableVolume(ScanNode scanNode) {
    long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
    if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
      PartitionedTableScanNode pScanNode = (PartitionedTableScanNode)scanNode;
      if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
        scanBytes = 0L;
      }
    }

    return scanBytes;
  }

  private static class ParentFinder implements LogicalNodeVisitor {
    private LogicalNode target;
    private LogicalNode found;

    public void set(LogicalNode child) {
      this.target = child;
      this.found = null;
    }

    public void find(LogicalNode root) {
      this.visit(root);
    }

    @Override
    public void visit(LogicalNode node) {
      for (int i = 0; i < node.childNum(); i++) {
        if (node.getChild(i).equals(target)) {
          found = node;
          break;
        } else {
          if (found == null) {
            visit(node.getChild(i));
          }
        }
      }
    }
  }
}
