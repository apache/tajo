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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.graph.DirectedGraphVisitor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link BroadcastJoinRule} converts repartition join plan into broadcast join plan.
 * Broadcast join rules can be defined as follows.
 *
 * <h3>Broadcastable relation</h3>
 * A relation is broadcastable when its size is smaller than a given threshold.
 *
 * <h3>Rules to convert repartition join into broadcast join</h3>
 * <ul>
 *   <li>Given an EB containing a join and its child EBs, those EBs can be merged into a single EB if at least one child EB's output is broadcastable.</li>
 *   <li>Given a user-defined threshold, the total size of broadcast relations of an EB cannot exceed such threshold.</li>
 *   <ul>
 *     <li>After merging EBs according to the first rule, the result EB may not satisfy the second rule. In this case, enforce repartition join for large relations to satisfy the second rule.</li>
 *   </ul>
 *   <li>Preserved-row relations cannot be broadcasted to avoid duplicated results. That is, full outer join cannot be executed with broadcast join.</li>
 *   <ul>
 *     <li>Here is brief backgrounds for this rule. Data of preserved-row relations will be appeared in the join result regardless of join conditions. If multiple tasks execute outer join with broadcasted preserved-row relations, they emit duplicates results.</li>
 *     <li>Even though a single task can execute outer join when every input is broadcastable, broadcast join is not allowed if one of input relation consists of multiple files.</li>
 *   </ul>
 * </ul>
 *
 */
public class BroadcastJoinRule implements GlobalPlanRewriteRule {

  private BroadcastJoinPlanBuilder planBuilder;
  private BroadcastJoinPlanFinalizer planFinalizer;

  protected void init(MasterPlan plan) {
    GlobalPlanRewriteUtil.ParentFinder parentFinder = new GlobalPlanRewriteUtil.ParentFinder();
    RelationSizeComparator relSizeComparator = new RelationSizeComparator();
    planBuilder = new BroadcastJoinPlanBuilder(plan, relSizeComparator, parentFinder);
    planFinalizer = new BroadcastJoinPlanFinalizer(plan, relSizeComparator);
  }

  @Override
  public String getName() {
    return "Broadcast join rule";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    long thresholdForNonCrossJoin = queryContext.getLong(SessionVars.BROADCAST_NON_CROSS_JOIN_THRESHOLD) *
        StorageUnit.KB;
    long thresholdForCrossJoin = queryContext.getLong(SessionVars.BROADCAST_CROSS_JOIN_THRESHOLD) *
        StorageUnit.KB;
    boolean broadcastJoinEnabled = queryContext.getBool(SessionVars.TEST_BROADCAST_JOIN_ENABLED);
    if (broadcastJoinEnabled &&
        (thresholdForNonCrossJoin > 0 || thresholdForCrossJoin > 0)) {
      for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
        if (block.hasNode(NodeType.JOIN)) {
          init(plan);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public MasterPlan rewrite(OverridableConf queryContext, MasterPlan plan) throws TajoException {
    long thresholdForNonCrossJoin = queryContext.getLong(SessionVars.BROADCAST_NON_CROSS_JOIN_THRESHOLD) *
        StorageUnit.KB;
    long thresholdForCrossJoin = queryContext.getLong(SessionVars.BROADCAST_CROSS_JOIN_THRESHOLD) *
        StorageUnit.KB;
    plan.accept(new Context(thresholdForNonCrossJoin, thresholdForCrossJoin), plan.getRoot().getId(), planBuilder);
    plan.accept(null, plan.getRoot().getId(), planFinalizer);
    return plan;
  }

  private static class RelationSizeComparator implements Comparator<ScanNode> {

    @Override
    public int compare(ScanNode o1, ScanNode o2) {
      long compare = PlannerUtil.getTableVolume(o1) - PlannerUtil.getTableVolume(o2);
      if (compare == 0) {
        return 0;
      } else if (compare > 0) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  /**
   * If a plan contains only broadcast relations, it will be executed at multiple workers who store any broadcast relations.
   * {@Link BroadcastJoinPlanFinalizer} checks whether every input is the broadcast candidate or not.
   * If so, it removes the broadcast property from the largest relation.
   */
  private class BroadcastJoinPlanFinalizer implements DirectedGraphVisitor<Object, ExecutionBlockId> {
    private final MasterPlan plan;
    private final RelationSizeComparator relSizeComparator;

    public BroadcastJoinPlanFinalizer(MasterPlan plan, RelationSizeComparator relationSizeComparator) {
      this.plan = plan;
      this.relSizeComparator = relationSizeComparator;
    }

    @Override
    public void visit(Object context, Stack<ExecutionBlockId> stack, ExecutionBlockId currentId) {
      ExecutionBlock current = plan.getExecBlock(currentId);
      if (!plan.isTerminal(current)) {
        // When every child is a broadcast candidate, enforce non-broadcast for the largest relation for the join to be
        // computed at the node who stores such largest relation.
        if (isFullyBroadcastable(current)) {
          List<ScanNode> broadcastCandidates = new ArrayList<>(current.getBroadcastRelations());
          Collections.sort(broadcastCandidates, relSizeComparator);

          current.removeBroadcastRelation(broadcastCandidates.remove(broadcastCandidates.size()-1));
        }
      }
    }
  }

  private static class Context {
    private final long thresholdForNonCrossJoin;
    private final long thresholdForCrossJoin;
    private final Map<String, Long> estimatedEbOutputSize = new HashMap<>(); // map of table name and its volume

    public Context(long thresholdForNonCrossJoin, long thresholdForCrossJoin) {
      this.thresholdForNonCrossJoin = thresholdForNonCrossJoin;
      this.thresholdForCrossJoin = thresholdForCrossJoin;
    }
  }

  private class BroadcastJoinPlanBuilder implements DirectedGraphVisitor<Context, ExecutionBlockId> {
    private final MasterPlan plan;
    private final RelationSizeComparator relSizeComparator;
    private final GlobalPlanRewriteUtil.ParentFinder parentFinder;

    public BroadcastJoinPlanBuilder(MasterPlan plan, RelationSizeComparator relationSizeComparator,
                                    GlobalPlanRewriteUtil.ParentFinder parentFinder) {
      this.plan = plan;
      this.relSizeComparator = relationSizeComparator;
      this.parentFinder = parentFinder;
    }

    @Override
    public void visit(Context context, Stack<ExecutionBlockId> stack, ExecutionBlockId executionBlockId)
        throws TajoException {
      ExecutionBlock current = plan.getExecBlock(executionBlockId);

      if (plan.isLeaf(current)) {
        visitLeafNode(context, current);
      } else {
        visitNonLeafNode(context, current);
      }
    }

    /**
     * Estimate the result size of leaf blocks.
     *
     * @param current
     */
    private void visitLeafNode(Context context, ExecutionBlock current) {
      // Preserved-row relations must not be broadcasted to avoid data duplication.
      if (!current.isPreservedRow()) {
        long totalVolume = 0;
        for (ScanNode scanNode : current.getScanNodes()) {
          totalVolume += PlannerUtil.getTableVolume(scanNode);
        }
        context.estimatedEbOutputSize.put(current.getId().toString(), totalVolume);
      }
    }

    /**
     * 1. Based on the join type, find broadcastable relations of the child execution blocks.
     * 2. Update the current block's inputs based on the broadcastability of the child blocks.
     * 3. Merge child blocks and the current block if the scan to the corresponding child block is broadcastable.
     * 4. Estimate the result size of the current block.
     *
     * @param current
     */
    private void visitNonLeafNode(Context context, ExecutionBlock current) throws TajoException {
      // At non-leaf execution blocks, merge broadcastable children's plan with the current plan.

      if (!plan.isTerminal(current)) {
        if (current.hasJoin()) {
          List<ExecutionBlock> childs = plan.getChilds(current);
          Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = current.getUnionScanMap();
          LogicalNode found = PlannerUtil.findTopNode(current.getPlan(), NodeType.JOIN);
          if (found == null) {
            throw new TajoInternalError("ExecutionBlock " + current.getId() + " doesn't have any join operator, " +
                "but the master plan indicates that it has.");
          }
          JoinType joinType = ((JoinNode)found).getJoinType();

          for (ExecutionBlock child : childs) {
            if (!child.isPreservedRow()) {
              updateBroadcastableRelForChildEb(context, child, joinType);
              // Mark the scan node for the child eb as broadcastable to figure out the current and child ebs can be merged.
              updateInputBasedOnChildEb(child, current);
            }
          }

          if (current.hasBroadcastRelation()) {
            long broadcastThreshold = joinType.equals(JoinType.CROSS) ?
                context.thresholdForCrossJoin : context.thresholdForNonCrossJoin;

            // The current execution block and its every child are able to be merged.
            for (ExecutionBlock child : childs) {
              addUnionNodeIfNecessary(unionScanMap, plan, child, current);

              // First check that two stages can be merged.
              // If the total volume of broadcast candidates of the merged stage exceeds the threshold,
              // these stages cannot be merged.
              //
              // Note: this is a greedy approach, and there may be a better solution to find more optimized broadcast
              // join plan. For example, it would be better to split the merged stage by marking the largest broadcast
              // candidate as not being broadcasted because it can reduce the network cost a little bit.
              // However, the benefit looks not large (every broadcast candidates are very small), so the simple greedy
              // solution is used here.
              if (getTotalVolumeOfBroadcastableRelations(context, current) +
                  getTotalVolumeOfBroadcastableRelations(context, child)
                  > broadcastThreshold) {
                // If a scan node for the child eb is marked as a broadcast candidate, mark it as not being broadcasted
                // again.
                List<ScanNode> notBroadcastable = current.getBroadcastRelations().stream()
                    .filter(r -> r.getTableName().equals(child.getId().toString()))
                    .collect(Collectors.toList());
                notBroadcastable.forEach(r -> current.removeBroadcastRelation(r));
              } else {
                mergeTwoPhaseJoinIfPossible(plan, child, current);
              }
            }

            long outputVolume = estimateOutputVolume(current);
            context.estimatedEbOutputSize.put(current.getId().toString(), outputVolume);
          }
        } else {
          List<ScanNode> relations = new ArrayList<>(current.getBroadcastRelations());
          for (ScanNode eachRelation : relations) {
            current.removeBroadcastRelation(eachRelation);
          }
        }
      }
    }

    private void updateInputBasedOnChildEb(ExecutionBlock child, ExecutionBlock parent) {
      if (isFullyBroadcastable(child)) {
        if (plan.isLeaf(child) && child.getScanNodes().length == 1) {
          try {
            updateScanOfParentAsBroadcastable(plan, child, parent);
          } catch (NoScanNodeForChildEbException e) {
            // This case is when the current has two or more inputs via union, and simply ignored.
          }
        } else {
          updateScanOfParentAsBroadcastable(plan, child, parent);
        }
      }
    }

    private void updateBroadcastableRelForChildEb(Context context, ExecutionBlock child, JoinType joinType) {
      long threshold = joinType == JoinType.CROSS ? context.thresholdForCrossJoin : context.thresholdForNonCrossJoin;
      for (ScanNode scanNode : child.getScanNodes()) {
        long volume = PlannerUtil.getTableVolume(scanNode);
        if (volume >= 0 && volume <= threshold) {
          // If the child eb is already visited, the below line may update its broadcast relations.
          // Furthermore, this operation might mark the preserved-row relation as the broadcast relation with outer join.
          // However, the rewriting result is still valid. Please consider the following query:
          //
          // EX) SELECT ... FROM a LEFT OUTER JOIN b on ... LEFT OUTER JOIN c on ...
          //
          // and assume that three relations of a, b, and c are all broadcastable.
          // The initial global plan will be as follow:
          //
          // EB 2)
          //     LEFT OUTER JOIN
          //        /       \
          //       c        EB_1
          // EB 1)
          //     LEFT OUTER JOIN
          //        /       \
          //       a         b
          //
          // When visiting EB_1, the bellow line marks only b as the broadcast relation because a is the preserved-row
          // relation. However, when visiting EB_2, it marks both a and b as the broadcast relations because EB_1 is
          // the null-supplying relation which has a and b as its inputs.
          // Thus, the rewriting result will be like
          //
          // EB 2) broadcast: a, b
          //     LEFT OUTER JOIN
          //        /       \
          //       c     LEFT OUTER JOIN
          //                /       \
          //               a         b
          //
          // This plan returns the same result as a plan that broadcasts the result of the first join.
          // Obviously, the result must be valid.
          child.addBroadcastRelation(scanNode);
        }
      }
    }

    private long estimateOutputVolume(ExecutionBlock block) {
      return estimateOutputVolumeInternal(PlannerUtil.<JoinNode>findTopNode(block.getPlan(), NodeType.JOIN));
    }

    private long estimateOutputVolumeInternal(LogicalNode node) throws TajoInternalError {

      if (node instanceof RelationNode) {
        switch (node.getType()) {
          case INDEX_SCAN:
          case SCAN:
            ScanNode scanNode = (ScanNode) node;
            if (scanNode.getTableDesc().getStats() == null) {
              // TODO - this case means that data is not located in HDFS. So, we need additional
              // broadcast method.
              return Long.MAX_VALUE;
            } else {
              return scanNode.getTableDesc().getStats().getNumBytes();
            }
          case PARTITIONS_SCAN:
            PartitionedTableScanNode pScanNode = (PartitionedTableScanNode) node;
            if (pScanNode.getTableDesc().getStats() == null) {
              // TODO - this case means that data is not located in HDFS. So, we need additional
              // broadcast method.
              return Long.MAX_VALUE;
            } else {
              // if there is no selected partition
              if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
                return 0;
              } else {
                return pScanNode.getTableDesc().getStats().getNumBytes();
              }
            }
          case TABLE_SUBQUERY:
            return estimateOutputVolumeInternal(((TableSubQueryNode) node).getSubQuery());
        }
      } else if (node instanceof UnaryNode) {
        return estimateOutputVolumeInternal(((UnaryNode) node).getChild());
      } else if (node instanceof UnionNode) {
        UnionNode binaryNode = (UnionNode) node;
        return estimateOutputVolumeInternal(binaryNode.getLeftChild()) +
            estimateOutputVolumeInternal(binaryNode.getRightChild());
      } else if (node instanceof JoinNode) {
        JoinNode joinNode = (JoinNode) node;
        JoinSpec joinSpec = joinNode.getJoinSpec();
        long leftChildVolume = estimateOutputVolumeInternal(joinNode.getLeftChild());
        long rightChildVolume = estimateOutputVolumeInternal(joinNode.getRightChild());
        switch (joinNode.getJoinType()) {
          case CROSS:
            return leftChildVolume * rightChildVolume;
          case INNER:
            return (long) (leftChildVolume * rightChildVolume *
                Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
          case LEFT_OUTER:
            return leftChildVolume;
          case RIGHT_OUTER:
            return rightChildVolume;
          case FULL_OUTER:
            return leftChildVolume < rightChildVolume ? leftChildVolume : rightChildVolume;
          case LEFT_ANTI:
          case LEFT_SEMI:
            return (long) (leftChildVolume *
                Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
          case RIGHT_ANTI:
          case RIGHT_SEMI:
            return (long) (rightChildVolume *
                Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, joinSpec.getPredicates().size()));
        }
      }

      throw new TajoInternalError("Invalid State at node " + node.getPID());
    }

    private long getTotalVolumeOfBroadcastableRelations(Context context, ExecutionBlock block) {
      return block.getBroadcastRelations().stream()
          .filter(r -> !context.estimatedEbOutputSize.containsKey(r.getTableName()))
          .mapToLong(r -> {
            long volume = PlannerUtil.getTableVolume(r);
            return volume == TajoConstants.UNKNOWN_LENGTH ?
                Integer.MAX_VALUE : volume; // Use Integer.MAX to prevent overflow
          }).sum();
    }

    private void updateScanOfParentAsBroadcastable(MasterPlan plan, ExecutionBlock current, ExecutionBlock parent) {
      if (parent != null && !plan.isTerminal(parent)) {
        ScanNode scanForCurrent = findScanForChildEb(current, parent);
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
    private ExecutionBlock mergeTwoPhaseJoinIfPossible(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent)
        throws TajoException {
      ScanNode scanForChild = findScanForChildEb(child, parent);

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
                                         ExecutionBlock child, ExecutionBlock current) throws TajoException {
      if (unionScanMap != null) {
        List<ExecutionBlockId> unionScans = new ArrayList<>();
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
            unionNode.setRightChild(GlobalPlanner.buildInputExecutor(plan.getLogicalPlan(),
                plan.getChannel(unionScans.get(i), current.getId())));
            unionNode.setInSchema(left.getOutSchema());
            unionNode.setOutSchema(left.getOutSchema());
            topUnion = unionNode;
            left = unionNode;
          }

          ScanNode scanForChild = findScanForChildEb(plan.getExecBlock(representativeId), current);
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

  /**
   * Find a scan node in the plan of the parent EB corresponding to the output of the child EB.
   *
   * @param child
   * @param parent
   * @return ScanNode
   */
  private static ScanNode findScanForChildEb(ExecutionBlock child, ExecutionBlock parent) {
    ScanNode scanForChild = null;
    for (ScanNode scanNode : parent.getScanNodes()) {
      if (scanNode.getTableName().equals(child.getId().toString())) {
        scanForChild = scanNode;
        break;
      }
    }
    if (scanForChild == null) {
      throw new NoScanNodeForChildEbException(
          "cannot find any scan nodes for " + child.getId() + " in " + parent.getId());
    }
    return scanForChild;
  }

  private static class NoScanNodeForChildEbException extends RuntimeException  {
    NoScanNodeForChildEbException(String message) {
      super(message);
    }
  }
}
