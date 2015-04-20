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
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
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
    if (queryContext.getBool(SessionVars.TEST_BROADCAST_JOIN_ENABLED)) {
      for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
        if (block.hasNode(NodeType.JOIN)) {
          broadcastTableSizeThreshold = queryContext.getLong(SessionVars.BROADCAST_TABLE_SIZE_LIMIT);
          if (broadcastTableSizeThreshold > 0) {
            if (parentFinder == null) {
              parentFinder = new ParentFinder();
            }
            return true;
          }
        }
      }
    }
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
          current.addBroadcastRelation(scanNode.getTableName());
        }
      }
    } else {
      // in intermediate execution blocks, merge broadcastable children's plan with the current plan.
      for (ExecutionBlock child : plan.getChilds(current)) {
        rewrite(plan, child);
      }
      if (current.hasJoin()) {
        boolean needMerge = false;
        for (ExecutionBlock child : plan.getChilds(current)) {
          if (child.isBroadcastable(broadcastTableSizeThreshold)) {
            needMerge = true;
            break;
          }
        }
        if (needMerge) {
          for (ExecutionBlock child : plan.getChilds(current)) {
            merge(plan, child, current);
          }
        }
      }
    }
  }

  /**
   * Merge child execution blocks.
   *
   * @param plan master plan
   * @param child child block
   * @param parent parent block
   * @return
   */
  private ExecutionBlock merge(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) throws PlanningException {
    ScanNode scanForChild = null;
    for (ScanNode scanNode : parent.getScanNodes()) {
      if (scanNode.getTableName().equals(child.getId().toString())) {
        scanForChild = scanNode;
      }
    }
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

    if (parentOfScanForChild instanceof UnaryNode) {
      ((UnaryNode) parentOfScanForChild).setChild(rootOfChild);
    } else if (parentOfScanForChild instanceof BinaryNode) {
      BinaryNode binary = (BinaryNode) parentOfScanForChild;
      if (binary.getLeftChild().equals(scanForChild)) {
        binary.setLeftChild(rootOfChild);
      } else if (binary.getRightChild().equals(scanForChild)) {
        binary.setRightChild(rootOfChild);
      } else {
        throw new PlanningException(scanForChild.getPID() + " is not a child of " + parentOfScanForChild.getPID());
      }
    } else {
      throw new PlanningException(parentOfScanForChild + " seems to not have any children");
    }

    for (String broadcastable : child.getBroadcastTables()) {
      parent.addBroadcastRelation(broadcastable);
    }

    plan.disconnect(child, parent);
    List<DataChannel> channels = plan.getIncomingChannels(child.getId());
    if (channels == null || channels.size() == 0) {
      channels = plan.getOutgoingChannels(child.getId());
      if (channels == null || channels.size() == 0) {
        plan.removeExecBlock(child.getId());
      }
    }

    parent.setPlan(parent.getPlan());

    return parent;
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
