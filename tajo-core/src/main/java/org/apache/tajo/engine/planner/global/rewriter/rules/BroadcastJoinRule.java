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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.graph.DirectedGraphVisitor;

import java.util.*;

/**
 * Broadcast rules are as follows.
 * A relation is broadcastable when its size is smaller than a given threshold.
 */
public class BroadcastJoinRule implements GlobalPlanRewriteRule {
  private final static Log LOG = LogFactory.getLog(BroadcastJoinRule.class);

  private BroadcastJoinPlanBuilder planOptimizer;
  private BroadcastJoinOptimizeFinalizer optimizeFinalizer;

  @Override
  public String getName() {
    return "BroadcastJoinRule";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    if (queryContext.getBool(SessionVars.TEST_BROADCAST_JOIN_ENABLED)) {
      for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
        if (block.hasNode(NodeType.JOIN)) {
          long broadcastSizeThreshold = queryContext.getLong(SessionVars.BROADCAST_TABLE_SIZE_LIMIT);
          if (broadcastSizeThreshold > 0) {
            GlobalPlanRewriteUtil.ParentFinder parentFinder = new GlobalPlanRewriteUtil.ParentFinder();
            RelationSizeComparator relSizeComparator = new RelationSizeComparator();
            planOptimizer = new BroadcastJoinPlanBuilder(plan, relSizeComparator, parentFinder, broadcastSizeThreshold);
            optimizeFinalizer = new BroadcastJoinOptimizeFinalizer(plan, relSizeComparator);
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) throws PlanningException{
    plan.accept(plan.getRoot().getId(), planOptimizer);
    plan.accept(plan.getRoot().getId(), optimizeFinalizer);
    return plan;
  }

  private static class RelationSizeComparator implements Comparator<ScanNode> {

    @Override
    public int compare(ScanNode o1, ScanNode o2) {
      return (int) (GlobalPlanRewriteUtil.getTableVolume(o1) - GlobalPlanRewriteUtil.getTableVolume(o2));
    }
  }

  /**
   *
   */
  private static class BroadcastJoinOptimizeFinalizer implements DirectedGraphVisitor<ExecutionBlockId> {
    private final MasterPlan plan;
    private final RelationSizeComparator relSizeComparator;

    public BroadcastJoinOptimizeFinalizer(MasterPlan plan, RelationSizeComparator relationSizeComparator) {
      this.plan = plan;
      this.relSizeComparator = relationSizeComparator;
    }

    @Override
    public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId currentId) {
      ExecutionBlock current = plan.getExecBlock(currentId);
      if (!plan.isTerminal(current)) {
        // When every child is a broadcast candidate, enforce non-broadcast for the largest relation for the join to be
        // computed at the node who stores such largest relation.
        if (isFullyBroadcastable(current)) {
          List<ScanNode> broadcastCandidates = TUtil.newList(current.getBroadcastRelations());
          Collections.sort(broadcastCandidates, relSizeComparator);

          current.removeBroadcastRelation(broadcastCandidates.remove(broadcastCandidates.size()-1));
        }
      }
    }
  }

  private static class BroadcastJoinPlanBuilder implements DirectedGraphVisitor<ExecutionBlockId> {
    private final MasterPlan plan;
    private final RelationSizeComparator relSizeComparator;
    private final long broadcastSizeThreshold;
    private final GlobalPlanRewriteUtil.ParentFinder parentFinder;

    public BroadcastJoinPlanBuilder(MasterPlan plan, RelationSizeComparator relationSizeComparator,
                                    GlobalPlanRewriteUtil.ParentFinder parentFinder, long broadcastSizeThreshold) {
      this.plan = plan;
      this.relSizeComparator = relationSizeComparator;
      this.broadcastSizeThreshold = broadcastSizeThreshold;
      this.parentFinder = parentFinder;
    }

    @Override
    public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId executionBlockId) {
      ExecutionBlock current = plan.getExecBlock(executionBlockId);

      if (plan.isLeaf(current)) {
        visitLeafNode(current);
      } else {
        visitNonLeafNode(current);
      }
    }

    private void visitLeafNode(ExecutionBlock current) {
      // At leaf execution blocks, find input relations who's size is smaller than the predefined threshold.
      if (!current.isPreservedRow()) {
        // Preserved-row relations must not be broadcasted to avoid data duplication.
        boolean fullyBroadcastable = true;
        for (ScanNode scanNode : current.getScanNodes()) {
          if (GlobalPlanRewriteUtil.getTableVolume(scanNode) <= broadcastSizeThreshold) {
            current.addBroadcastRelation(scanNode);
          } else {
            fullyBroadcastable = false;
          }
        }
        if (fullyBroadcastable && current.getScanNodes().length == 1) {
          try {
            updateScanOfParentAsBroadcastable(plan, current);
          } catch (PlanningException e) {
//            throw new RuntimeException(e);
            // This case is when the current has two or more inputs via union.
            // It is able to be simply ignored.
          }
        }
      }
    }

    private void visitNonLeafNode(ExecutionBlock current) {
      // At non-leaf execution blocks, merge broadcastable children's plan with the current plan.

      if (!plan.isTerminal(current)) {
        if (current.hasJoin()) {
          List<ExecutionBlock> childs = plan.getChilds(current);
          Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = current.getUnionScanMap();

          // When the total size of broadcastable relations exceeds the threshold, enforce two-phase join for large ones
          // in order to broadcast as many relations as possible.
          List<ScanNode> broadcastCandidates = TUtil.newList();
          for (ScanNode scanNode : current.getScanNodes()) {
            long estimatedRelationSize = GlobalPlanRewriteUtil.getTableVolume(scanNode);
            if (estimatedRelationSize > 0 && estimatedRelationSize <= broadcastSizeThreshold) {
              broadcastCandidates.add(scanNode);
            }
          }
          Collections.sort(broadcastCandidates, relSizeComparator);

          // Enforce broadcast for candidates in ascending order of relation size
          long totalBroadcastVolume = 0;
          int i;
          for (i = 0; i < broadcastCandidates.size(); i++) {
            long volumeOfCandidate = GlobalPlanRewriteUtil.getTableVolume(broadcastCandidates.get(i));
            if (totalBroadcastVolume + volumeOfCandidate > broadcastSizeThreshold) {
              break;
            }
            totalBroadcastVolume += volumeOfCandidate;
          }

          for (; i < broadcastCandidates.size(); ) {
            ScanNode nonBroadcast = broadcastCandidates.remove(i);
            broadcastCandidates.remove(nonBroadcast);
          }

          if (current.hasBroadcastRelation()) {
            // The current execution block and its every child are able to be merged.
            for (ExecutionBlock child : childs) {
              try {
                addUnionNodeIfNecessary(unionScanMap, plan, child, current);
                mergeTwoPhaseJoin(plan, child, current);
              } catch (PlanningException e) {
                throw new RuntimeException(e);
              }
            }

            // We assume that if every input of an execution block is broadcastable,
            // the output of the execution block is also broadcastable.
            if (!current.isPreservedRow() && isFullyBroadcastable(current)) {
              try {
                updateScanOfParentAsBroadcastable(plan, current);
              } catch (PlanningException e) {
                throw new RuntimeException(e);
              }
            }
          }
        } else {
          List<ScanNode> relations = TUtil.newList(current.getBroadcastRelations());
          for (ScanNode eachRelation : relations) {
            current.removeBroadcastRelation(eachRelation);
          }
        }
      }
    }

    private void updateScanOfParentAsBroadcastable(MasterPlan plan, ExecutionBlock current) throws PlanningException {
      ExecutionBlock parent = plan.getParent(current);
      if (parent != null && !plan.isTerminal(parent)) {
        ScanNode scanForCurrent = GlobalPlanRewriteUtil.findScanForChildEb(current, parent);
        parent.addBroadcastRelation(scanForCurrent);
      }
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

      parentFinder.set(scanForChild);
      parentFinder.find(parent.getPlan());
      LogicalNode parentOfScanForChild = parentFinder.getFound();

      LogicalNode rootOfChild = child.getPlan();
      if (rootOfChild.getType() == NodeType.STORE) {
        rootOfChild = ((StoreTableNode)rootOfChild).getChild();
      }

      GlobalPlanRewriteUtil.replaceChild(rootOfChild, scanForChild, parentOfScanForChild);

      parent = GlobalPlanRewriteUtil.mergeExecutionBlocks(plan, child, parent);
      parent.removeBroadcastRelation(scanForChild);

      parent.setPlan(parent.getPlan());

      return parent;
    }

    private void addUnionNodeIfNecessary(Map<ExecutionBlockId, ExecutionBlockId> unionScanMap, MasterPlan plan,
                                         ExecutionBlock child, ExecutionBlock current)
        throws PlanningException {
      if (unionScanMap != null) {
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
          for (int i = 1; i < unionScans.size(); i++) {
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
      }
    }
  }

  private static boolean isFullyBroadcastable(ExecutionBlock block) {
    return block.getBroadcastRelations().size() == block.getScanNodes().length;
  }
}
