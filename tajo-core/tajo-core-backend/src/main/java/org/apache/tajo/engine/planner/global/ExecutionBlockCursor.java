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

import java.util.ArrayList;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * This class is a pointer to an ExecutionBlock that the query engine should execute.
 * For each call of nextBlock(), it retrieves a next ExecutionBlock in a postfix order.
 */
public class ExecutionBlockCursor {
  private MasterPlan masterPlan;
  private ArrayList<ExecutionBlock> orderedBlocks = new ArrayList<ExecutionBlock>();
  private int cursor = 0;

  public ExecutionBlockCursor(MasterPlan plan) {
    this.masterPlan = plan;
    buildOrder(plan.getRoot());
  }

  public int size() {
    return orderedBlocks.size();
  }

  private void buildOrder(ExecutionBlock current) {
    if (!masterPlan.isLeaf(current.getId())) {
      if (masterPlan.getChildCount(current.getId()) == 1) {
        ExecutionBlock block = masterPlan.getChild(current, 0);
        buildOrder(block);
      } else {
        for (ExecutionBlock exec : masterPlan.getChilds(current)) {
          buildOrder(exec);
        }
      }
    }
    orderedBlocks.add(current);
  }

  public boolean hasNext() {
    return cursor < orderedBlocks.size();
  }

  public ExecutionBlock nextBlock() {
    return orderedBlocks.get(cursor++);
  }

  public ExecutionBlock peek() {
    return orderedBlocks.get(cursor);
  }

  public ExecutionBlock peek(int skip) {
    return  orderedBlocks.get(cursor + skip);
  }

  public void reset() {
    cursor = 0;
  }
}
