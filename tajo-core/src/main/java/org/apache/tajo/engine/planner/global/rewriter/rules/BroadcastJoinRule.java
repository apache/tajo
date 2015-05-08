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
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.List;

public class BroadcastJoinRule implements GlobalPlanRewriteRule {
  private long broadcastTableSizeThreshold;
  private GlobalPlanRewriteUtil.ParentFinder parentFinder;

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
              parentFinder = new GlobalPlanRewriteUtil.ParentFinder();
            }
            return true;
          }
        }
      }
    }
    return false;
  }

  // TODO: use MasterPlan.execBlockGraph.accept()
  @Override
  public MasterPlan rewrite(MasterPlan plan) throws PlanningException{
    rewrite(plan, plan.getTerminalBlock());
    return plan;
  }

  private void rewrite(MasterPlan plan, ExecutionBlock current) throws PlanningException {
    if (plan.isLeaf(current)) {
      // in leaf execution blocks, find input tables which size is less than the predefined threshold.
      for (ScanNode scanNode : current.getScanNodes()) {
        if (GlobalPlanRewriteUtil.getTableVolume(scanNode) <= broadcastTableSizeThreshold) {
          current.addBroadcastRelation(scanNode.getCanonicalName());
        }
      }
    } else {
      // in intermediate execution blocks, merge broadcastable children's plan with the current plan.
      for (ExecutionBlock child : plan.getChilds(current)) {
        rewrite(plan, child);
      }
      if (!plan.isTerminal(current) && current.hasJoin()) {
        // unioned scans should be handled as a single relation scan

        // check outer join
        if (hasOuterJoin(current)) {
          // find and enforce shuffle for row-preserved tables
          
        }

        // check the total input size

        // check all inputs are marked as broadcast



        ExecutionBlock enforceNonBroadcast = null;
        ExecutionBlock broadcastCandidate = null;
        long smallestChildVolume = Long.MAX_VALUE;
        List<ExecutionBlock> childs = plan.getChilds(current);
        for (ExecutionBlock child : childs) {
          if (child.isBroadcastable(broadcastTableSizeThreshold)) {
            long inputVolume = GlobalPlanRewriteUtil.getInputVolume(child);
            if (smallestChildVolume > inputVolume) {
              smallestChildVolume = inputVolume;
              if (broadcastCandidate != null) {
                enforceNonBroadcast = broadcastCandidate;
              }
              broadcastCandidate = child;
            }
          }
        }
        if (broadcastCandidate != null) {
          if (enforceNonBroadcast != null) {
            List<String> tables = TUtil.newList(enforceNonBroadcast.getBroadcastTables());
            for (String broadcastTable : tables) {
//              enforceNonBroadcast.removeBroadcastRelation(broadcastTable);
              // TODO: remove the largest rel from broadcast when all inputs are broadcast
            }
          }
          for (ExecutionBlock child : childs) {
            mergeTwoPhaseJoin(plan, child, current);
          }
        }
      }
    }
  }

  private static boolean hasOuterJoin(ExecutionBlock block) {
    LogicalNode found = PlannerUtil.findMostBottomNode(block.getPlan(), NodeType.JOIN);
    if (found != null) {
      JoinNode joinNode = (JoinNode) found;
      return PlannerUtil.isOuterJoin(joinNode.getJoinType());
    }
    return false;
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
    ScanNode scanForChild = GlobalPlanRewriteUtil.findScanForChildEb(child, parent);
    if (scanForChild == null) {
      throw new PlanningException("Cannot find any scan nodes for " + child.getId() + " in " + parent.getId());
    }

    parentFinder.set(scanForChild);
    parentFinder.find(parent.getPlan());
    LogicalNode parentOfScanForChild = parentFinder.getFound();
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
    GlobalPlanRewriteUtil.replaceChild(rootOfChild, scanForChild, parentOfScanForChild);

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
    parent = GlobalPlanRewriteUtil.mergeExecutionBlocks(plan, child, parent);

    parent.setPlan(parent.getPlan());

    return parent;
  }


}
