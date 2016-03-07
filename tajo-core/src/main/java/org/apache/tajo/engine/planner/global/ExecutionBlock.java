/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.*;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * An ExecutionBlock is a basic execution unit that could be distributed across a number of nodes.
 * An ExecutionBlock class contains input information (e.g., child execution blocks or input
 * tables), and output information (e.g., partition type, partition key, and partition number).
 * In addition, it includes a logical plan to be executed in each node.
 */
public class ExecutionBlock {
  private ExecutionBlockId executionBlockId;
  private LogicalNode plan = null;
  private Enforcer enforcer = new Enforcer();
  // Actual ScanNode's ExecutionBlockId -> Delegated ScanNode's ExecutionBlockId.
  private Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = new HashMap<>();

  private Map<String, ScanNode> broadcastRelations = new HashMap<>();

  private PlanContext planContext;

  /*
   * An execution block is null-supplying or preserved-row when its output is used as an input for outer join.
   * These properties are decided based on the type of parent execution block's outer join.
   * Here are brief descriptions for these properties.
   *
   * 1) left outer join
   *
   *              parent eb
   *         -------------------
   *         | left outer join |
   *         -------------------
   *           /              \
   *   left child eb     right child eb
   * ----------------- ------------------
   * | preserved-row | | null-supplying |
   * ----------------- ------------------
   *
   * 2) right outer join
   *
   *               parent eb
   *         --------------------
   *         | right outer join |
   *         --------------------
   *           /              \
   *   left child eb     right child eb
   * ------------------ -----------------
   * | null-supplying | | preserved-row |
   * ------------------ -----------------
   *
   * 3) full outer join
   *
   *               parent eb
   *         -------------------
   *         | full outer join |
   *         -------------------
   *           /              \
   *   left child eb      right child eb
   * ------------------ ------------------
   * | null-supplying | | preserved-row  |
   * | preserved-row  | | null-supplying |
   * ------------------ ------------------
   *
   * The null-supplying and preserved-row properties are used to find which relations will be broadcasted.
   */
  protected boolean nullSuppllying = false;
  protected boolean preservedRow = false;

  public ExecutionBlock(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public ExecutionBlockId getId() {
    return executionBlockId;
  }

  public void setPlan(LogicalNode plan) throws TajoException {
    this.plan = plan;

    if (plan == null) {
      return;
    }

    final PlanVisitor visitor = new PlanVisitor();
    planContext = new PlanContext();
    visitor.visit(planContext, null, null, plan, new Stack<>());
  }

  public void addUnionScan(ExecutionBlockId realScanEbId, ExecutionBlockId delegatedScanEbId) {
    unionScanMap.put(realScanEbId, delegatedScanEbId);
  }

  public Map<ExecutionBlockId, ExecutionBlockId> getUnionScanMap() {
    return unionScanMap;
  }

  public LogicalNode getPlan() {
    return plan;
  }

  public Enforcer getEnforcer() {
    return enforcer;
  }

  public StoreTableNode getStoreTableNode() {
    return planContext.store;
  }

  public int getNonBroadcastRelNum() {
    int nonBroadcastRelNum = 0;
    for (ScanNode scanNode : planContext.scanlist) {
      if (!broadcastRelations.containsKey(scanNode.getCanonicalName())) {
        nonBroadcastRelNum++;
      }
    }
    return nonBroadcastRelNum;
  }

  public ScanNode [] getScanNodes() {
    return planContext.scanlist.toArray(new ScanNode[planContext.scanlist.size()]);
  }

  public boolean hasJoin() {
    return planContext.hasJoinPlan;
  }

  public boolean hasUnion() {
    return planContext.hasUnionPlan;
  }

  public boolean hasAgg() {
    return planContext.hasAggPlan;
  }

  public boolean isUnionOnly() {
    return planContext.isUnionOnly();
  }

  public void addBroadcastRelation(ScanNode relationNode) {
    if (!broadcastRelations.containsKey(relationNode.getCanonicalName())) {
      enforcer.addBroadcast(relationNode.getCanonicalName());
    }
    broadcastRelations.put(relationNode.getCanonicalName(), relationNode);
  }

  public void removeBroadcastRelation(ScanNode relationNode) {
    broadcastRelations.remove(relationNode.getCanonicalName());
    enforcer.removeBroadcast(relationNode.getCanonicalName());
  }

  public boolean isBroadcastRelation(ScanNode relationNode) {
    return broadcastRelations.containsKey(relationNode.getCanonicalName());
  }

  public boolean hasBroadcastRelation() {
    return broadcastRelations.size() > 0;
  }

  public Collection<ScanNode> getBroadcastRelations() {
    return broadcastRelations.values();
  }

  public String toString() {
    return executionBlockId.toString();
  }

  public void setNullSuppllying() {
    nullSuppllying = true;
  }

  public void setPreservedRow() {
    preservedRow = true;
  }

  public boolean isNullSuppllying() {
    return nullSuppllying;
  }

  public boolean isPreservedRow() {
    return preservedRow;
  }

  private class PlanContext {
    StoreTableNode store = null;

    List<ScanNode> scanlist = new ArrayList<>();

    boolean hasJoinPlan = false;
    boolean hasUnionPlan = false;
    boolean hasAggPlan = false;
    boolean hasSortPlan = false;

    boolean isUnionOnly() {
      return hasUnionPlan && !hasJoinPlan && !hasAggPlan && !hasSortPlan;
    }
  }

  private class PlanVisitor extends BasicLogicalPlanVisitor<PlanContext, LogicalNode> {

    @Override
    public LogicalNode visitJoin(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack) throws TajoException {
      context.hasJoinPlan = true;
      return super.visitJoin(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitGroupBy(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                                    Stack<LogicalNode> stack) throws TajoException {
      context.hasAggPlan = true;
      return super.visitGroupBy(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitWindowAgg(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, WindowAggNode node,
                                      Stack<LogicalNode> stack) throws TajoException {
      context.hasAggPlan = true;
      return super.visitWindowAgg(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitDistinctGroupby(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                            DistinctGroupbyNode node, Stack<LogicalNode> stack) throws TajoException {
      context.hasAggPlan = true;
      return super.visitDistinctGroupby(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitSort(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, SortNode node,
                                 Stack<LogicalNode> stack) throws TajoException {
      context.hasSortPlan = true;
      return super.visitSort(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitUnion(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                                  Stack<LogicalNode> stack) throws TajoException {
      context.hasUnionPlan = true;
      return super.visitUnion(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitStoreTable(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, StoreTableNode node,
                                       Stack<LogicalNode> stack) throws TajoException {
      context.store = node;
      return super.visitStoreTable(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitScan(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) throws TajoException {
      context.scanlist.add(node);
      return super.visitScan(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitPartitionedTableScan(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                 PartitionedTableScanNode node, Stack<LogicalNode> stack)
        throws TajoException {
      context.scanlist.add(node);
      return super.visitPartitionedTableScan(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitIndexScan(PlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, IndexScanNode node,
                                      Stack<LogicalNode> stack) throws TajoException {
      context.scanlist.add(node);
      return super.visitIndexScan(context, plan, block, node, stack);
    }
  }
}
