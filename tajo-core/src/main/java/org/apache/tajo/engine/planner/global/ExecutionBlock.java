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
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.util.TUtil;

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
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = new ArrayList<ScanNode>();
  private Enforcer enforcer = new Enforcer();

  // Actual ScanNode's ExecutionBlockId -> Delegated ScanNode's ExecutionBlockId.
  private Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = new HashMap<ExecutionBlockId, ExecutionBlockId>();

  private boolean hasJoinPlan;
  private boolean hasUnionPlan;
  private boolean isUnionOnly;

  private Map<String, ScanNode> broadcastRelations = TUtil.newHashMap();

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

  public void setPlan(LogicalNode plan) {
    hasJoinPlan = false;
    hasUnionPlan = false;
    isUnionOnly = true;
    this.scanlist.clear();
    this.plan = plan;

    if (plan == null) {
      return;
    }

    LogicalNode node = plan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      // TODO: the below code should be improved to handle every case
      if (isUnionOnly && node.getType() != NodeType.ROOT && node.getType() != NodeType.TABLE_SUBQUERY &&
          node.getType() != NodeType.SCAN && node.getType() != NodeType.PARTITIONS_SCAN &&
          node.getType() != NodeType.UNION && node.getType() != NodeType.PROJECTION) {
        isUnionOnly = false;
      }
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        s.add(s.size(), unary.getChild());
      } else if (node instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) node;
        if (binary.getType() == NodeType.JOIN) {
          hasJoinPlan = true;
        } else if (binary.getType() == NodeType.UNION) {
          hasUnionPlan = true;
        }
        s.add(s.size(), binary.getLeftChild());
        s.add(s.size(), binary.getRightChild());
      } else if (node instanceof ScanNode) {
        scanlist.add((ScanNode)node);
      } else if (node instanceof TableSubQueryNode) {
        TableSubQueryNode subQuery = (TableSubQueryNode) node;
        s.add(s.size(), subQuery.getSubQuery());
      } else if (node instanceof StoreTableNode) {
        store = (StoreTableNode)node;
      }
    }
    if (!hasUnionPlan) {
      isUnionOnly = false;
    }
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
    return store;
  }

  public int getNonBroadcastRelNum() {
    int nonBroadcastRelNum = 0;
    for (ScanNode scanNode : scanlist) {
      if (!broadcastRelations.containsKey(scanNode.getCanonicalName())) {
        nonBroadcastRelNum++;
      }
    }
    return nonBroadcastRelNum;
  }

  public ScanNode [] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }

  public boolean hasJoin() {
    return hasJoinPlan;
  }

  public boolean hasUnion() {
    return hasUnionPlan;
  }

  public boolean isUnionOnly() {
    return isUnionOnly;
  }

  public void addBroadcastRelation(ScanNode relationNode) {
    broadcastRelations.put(relationNode.getCanonicalName(), relationNode);
    enforcer.addBroadcast(relationNode.getCanonicalName());
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
}
