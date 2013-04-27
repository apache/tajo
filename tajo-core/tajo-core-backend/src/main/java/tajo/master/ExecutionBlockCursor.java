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

package tajo.master;

import tajo.engine.planner.global.MasterPlan;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * This class is a pointer to an ExecutionBlock that the query engine should execute.
 * For each call of nextBlock(), it retrieves a next ExecutionBlock in a postfix order.
 */
public class ExecutionBlockCursor {
  private ArrayList<ExecutionBlock> orderedBlocks = new ArrayList<ExecutionBlock>();
  private int cursor = 0;

  public ExecutionBlockCursor(MasterPlan plan) {
    buildOrder(plan.getRoot());
  }

  private void buildOrder(ExecutionBlock current) {
    if (current.hasChildBlock()) {
      if (current.getChildNum() == 1) {
        ExecutionBlock block = current.getChildBlocks().iterator().next();
        buildOrder(block);
      } else {
        Iterator<ExecutionBlock> it = current.getChildBlocks().iterator();
        ExecutionBlock outer = it.next();
        ExecutionBlock inner = it.next();

        // Switch between outer and inner
        // if an inner has a child and an outer doesn't.
        // It is for left-deep-first search.
        if (!outer.hasChildBlock() && inner.hasChildBlock()) {
          ExecutionBlock tmp = outer;
          outer = inner;
          inner = tmp;
        }

        buildOrder(outer);
        buildOrder(inner);
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
