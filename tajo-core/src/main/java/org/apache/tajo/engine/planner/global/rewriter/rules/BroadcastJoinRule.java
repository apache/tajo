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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class BroadcastJoinRule implements GlobalPlanRewriteRule {
  private long broadcastTableSizeThreshold;
  private GlobalPlanRewriteUtil.ParentFinder parentFinder;
  private RelationSizeComparator relSizeComparator;

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
              relSizeComparator = new RelationSizeComparator();
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

  private static class RelationSizeComparator implements Comparator<ScanNode> {

    @Override
    public int compare(ScanNode o1, ScanNode o2) {
      return (int) (GlobalPlanRewriteUtil.getTableVolume(o1) - GlobalPlanRewriteUtil.getTableVolume(o2));
    }
  }

  private void rewrite(MasterPlan plan, ExecutionBlock current) throws PlanningException {
    if (plan.isLeaf(current) && !current.isPreservedRow()) {
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
        // TODO: handle unioned scans as a single scan
        List<ScanNode> broadcastCandidates = TUtil.newList();
        List<ExecutionBlock> childs = plan.getChilds(current);
        Map<ScanNode, ExecutionBlock> relationBlockMap = TUtil.newHashMap();
        Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = current.getUnionScanMap();
        if (unionScanMap == null) {
          unionScanMap = TUtil.newHashMap();
        }

        // find all broadcast candidates from every child
        for (ExecutionBlock child : childs) {
          Map<String, ScanNode> scanNodeMap = TUtil.newHashMap();
          for (ScanNode scanNode : child.getScanNodes()) {
            scanNodeMap.put(scanNode.getCanonicalName(), scanNode);
          }
          for (String relName : child.getBroadcastTables()) {
            broadcastCandidates.add(scanNodeMap.get(relName));
            relationBlockMap.put(scanNodeMap.get(relName), child);
          }
        }
        Collections.sort(broadcastCandidates, relSizeComparator);

        // Enforce broadcast for candidates in ascending order of relation size
        long totalBroadcastVolume = 0;
        int i, broadcastEnd = -1;
        for (i = 0; i < broadcastCandidates.size(); i++) {
          long volumeOfCandidate = GlobalPlanRewriteUtil.getTableVolume(broadcastCandidates.get(i));
          if (totalBroadcastVolume + volumeOfCandidate > broadcastTableSizeThreshold) {
            broadcastEnd = i;
            break;
          }
          totalBroadcastVolume += volumeOfCandidate;
        }
        if (i == broadcastCandidates.size()) {
          broadcastEnd = i-1;
        }

        for (; i < broadcastCandidates.size(); i++) {
          ScanNode nonBroadcast = broadcastCandidates.get(i);
          ExecutionBlock enforceShuffleBlock = relationBlockMap.get(nonBroadcast);
          enforceShuffleBlock.removeBroadcastRelation(nonBroadcast.getTableName());
        }

        // TODO: check all inputs are marked as broadcast
        if (broadcastCandidates.size() > 0) {
          for (ExecutionBlock child : childs) {
//          if (child.hasBroadcastRelation()) {
            List<ExecutionBlockId> unionScans = TUtil.newList();
            ExecutionBlockId representativeId = null;
            if (unionScanMap.containsKey(child.getId())) {
              representativeId = unionScanMap.get(child.getId());
            } else if (unionScanMap.containsValue(child.getId())) {
              representativeId = child.getId();
            }

            if (representativeId != null) {
              for (Map.Entry<ExecutionBlockId, ExecutionBlockId> entry : unionScanMap.entrySet()) {
                if (entry.getValue().equals(representativeId)) {
                  unionScans.add(entry.getKey());
                }
              }

              // add unions
              LogicalNode left, topUnion = null;
              left = GlobalPlanner.buildInputExecutor(plan.getLogicalPlan(), plan.getChannel(unionScans.get(0), current.getId()));
              for (i = 1; i < unionScans.size(); i++) {
                // left must not be null
                UnionNode unionNode = plan.getLogicalPlan().createNode(UnionNode.class);
                unionNode.setLeftChild(left);
                unionNode.setRightChild(GlobalPlanner.buildInputExecutor(plan.getLogicalPlan(), plan.getChannel(unionScans.get(i), current.getId())));
                unionNode.setInSchema(left.getOutSchema());
                unionNode.setOutSchema(left.getOutSchema());
                topUnion = unionNode;
                left = unionNode;
              }

              ScanNode scanForChild = GlobalPlanRewriteUtil.findScanForChildEb(plan.getExecBlock(representativeId), current);
              PlannerUtil.replaceNode(plan.getLogicalPlan(), current.getPlan(), scanForChild, topUnion);

              current.getUnionScanMap().clear();
              current.setPlan(current.getPlan());
            }

            mergeTwoPhaseJoin(plan, child, current);
          }
        }

//        for (i = 0; i <= broadcastEnd; i++) {
//          ExecutionBlock willBeMergedChild = relationBlockMap.get(broadcastCandidates.get(i));
//          mergeTwoPhaseJoin(plan, willBeMergedChild, current);
//        }


//        ExecutionBlock enforceNonBroadcast = null;
//        ExecutionBlock broadcastCandidate = null;
//        long smallestChildVolume = Long.MAX_VALUE;
//
//        for (ExecutionBlock child : childs) {
//          if (child.isBroadcastable(broadcastTableSizeThreshold)) {
//            long inputVolume = GlobalPlanRewriteUtil.getInputVolume(child);
//            if (smallestChildVolume > inputVolume) {
//              smallestChildVolume = inputVolume;
//              if (broadcastCandidate != null) {
//                enforceNonBroadcast = broadcastCandidate;
//              }
//              broadcastCandidate = child;
//            }
//          }
//        }
//        if (broadcastCandidate != null) {
//          if (enforceNonBroadcast != null) {
//            List<String> tables = TUtil.newList(enforceNonBroadcast.getBroadcastTables());
//            for (String broadcastTable : tables) {
////              enforceNonBroadcast.removeBroadcastRelation(broadcastTable);
//              // TODO: remove the largest rel from broadcast when all inputs are broadcast
//            }
//          }
//
//        }
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
//    if (scanForChild == null) {
//      if (parent.getUnionScanMap() != null && !parent.getUnionScanMap().isEmpty()) {
//        ExecutionBlockId scanEbId = parent.getUnionScanMap().get(child.getId());
//        if (scanEbId != null) {
//          ExecutionBlock scanEb = plan.getExecBlock(scanEbId);
//          if (scanEb != null) {
//            scanForChild = GlobalPlanRewriteUtil.findScanForChildEb(scanEb, parent);
//          }
//        }
//      }
//    }
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
