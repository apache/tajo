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

import com.google.common.annotations.VisibleForTesting;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.planner.LogicalPlan.PIDFactory;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * An ExecutionBlock is a basic execution unit that could be distributed across a number of nodes.
 * An ExecutionBlock class contains input information (e.g., child execution blocks or input
 * tables), and output information (e.g., partition type, partition key, and partition number).
 * In addition, it includes a logical plan to be executed in each node.
 */
public class ExecutionBlock {
  private ExecutionBlockId executionBlockId;
  private ExecutionPlan executionPlan;
  private Enforcer enforcer = new Enforcer();

  private Set<String> broadcasted = new HashSet<String>();

  public ExecutionBlock(ExecutionBlockId executionBlockId, PIDFactory pidFactory, LogicalRootNode rootNode) {
    this.executionBlockId = executionBlockId;
    this.executionPlan = new ExecutionPlan(pidFactory, rootNode);
  }

  @VisibleForTesting
  public ExecutionBlock(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public ExecutionBlockId getId() {
    return executionBlockId;
  }

  public void setPlan(LogicalNode plan) {
    executionPlan.setPlan(plan);
  }

  public ExecutionPlan getPlan() {
    return executionPlan;
  }

  public Enforcer getEnforcer() {
    return enforcer;
  }

  public InputContext getInputContext() {
    return executionPlan.getInputContext();
  }

  public boolean hasJoin() {
    return executionPlan.hasJoinPlan();
  }

  public boolean hasUnion() {
    return executionPlan.hasUnionPlan();
  }

  public void addBroadcastTables(Collection<String> tableNames) {
    broadcasted.addAll(tableNames);
  }

  public void addBroadcastTable(String tableName) {
    broadcasted.add(tableName);
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
