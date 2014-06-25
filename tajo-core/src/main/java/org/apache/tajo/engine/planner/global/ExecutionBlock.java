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
import org.apache.tajo.engine.planner.logical.*;

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

  //Real ScanNode's ExecutionBlockId -> Delegated ScanNode's ExecutionBlockId.
  private Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = new HashMap<ExecutionBlockId, ExecutionBlockId>();

  private boolean hasJoinPlan;
  private boolean hasUnionPlan;

  private Set<String> broadcasted = new HashSet<String>();

  public ExecutionBlock(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public ExecutionBlockId getId() {
    return executionBlockId;
  }

  public void setPlan(LogicalNode plan) {
    hasJoinPlan = false;
    hasUnionPlan = false;
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
      }
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

  public ScanNode [] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }

  public boolean hasJoin() {
    return hasJoinPlan;
  }

  public boolean hasUnion() {
    return hasUnionPlan;
  }

  public void addBroadcastTable(String tableName) {
    broadcasted.add(tableName);
    enforcer.addBroadcast(tableName);
  }

  public void removeBroadcastTable(String tableName) {
    broadcasted.remove(tableName);
    enforcer.removeBroadcast(tableName);
  }

  public boolean isBroadcastTable(String tableName) {
    return broadcasted.contains(tableName);
  }

  public Collection<String> getBroadcastTables() {
    return broadcasted;
  }

  public String toString() {
    return executionBlockId.toString();
  }
}
