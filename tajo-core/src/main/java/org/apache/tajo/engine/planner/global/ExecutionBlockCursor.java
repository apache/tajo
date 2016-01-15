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
import org.apache.tajo.SessionVars;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * This class is a pointer to an ExecutionBlock that the query engine should execute.
 */
public class ExecutionBlockCursor implements Iterable<ExecutionBlock> {
  private MasterPlan masterPlan;
  private ArrayList<ExecutionBlock> orderedBlocks = new ArrayList<>();

  private List<BuildOrderItem> executionOrderedBlocks = new ArrayList<>();
  private List<BuildOrderItem> notOrderedSiblingBlocks = new ArrayList<>();
  private Map<ExecutionBlockId, AtomicInteger> orderRequiredChildCountMap = new HashMap<>();

  public ExecutionBlockCursor(MasterPlan plan) {
    this(plan, false);
  }

  public ExecutionBlockCursor(MasterPlan plan, boolean siblingFirstOrder) {
    this.masterPlan = plan;
    if (siblingFirstOrder) {
      buildSiblingFirstOrder(plan.getRoot());
    } else {
      buildDepthFirstOrder(plan.getRoot());
    }
  }

  @Override
  public Iterator<ExecutionBlock> iterator() {
    return orderedBlocks.iterator();
  }

  public int size() {
    return orderedBlocks.size();
  }

  public ExecutionQueue newCursor() {
    int parallel = masterPlan.getContext().getInt(SessionVars.QUERY_EXECUTE_PARALLEL);
    if (parallel > 1) {
      return new ParallelExecutionQueue(masterPlan, parallel);
    }
    return new SimpleExecutionQueue();
  }

  public class SimpleExecutionQueue implements ExecutionQueue {

    private final Iterator<ExecutionBlock> iterator = iterator();
    private ExecutionBlock last;

    @Override
    public int size() {
      return ExecutionBlockCursor.this.size();
    }

    @Override
    public ExecutionBlock[] first() {
      return iterator.hasNext() ? next(null) : null;
    }

    @Override
    public ExecutionBlock[] next(ExecutionBlockId blockId) {
      return iterator.hasNext() ? new ExecutionBlock[]{last = iterator.next()} : null;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (ExecutionBlock block : ExecutionBlockCursor.this) {
        if (sb.length() > 0) {
          sb.append(',');
        }
        if (block == last) {
          sb.append('(');
        }
        sb.append(block.getId().getId());
        if (block == last) {
          sb.append(')');
        }
      }
      return sb.toString();
    }
  }

  // Add all execution blocks in a depth first and postfix order
  private void buildDepthFirstOrder(ExecutionBlock current) {
    if (!masterPlan.isLeaf(current.getId())) {
      masterPlan.getChilds(current).forEach(this::buildDepthFirstOrder);
    }
    orderedBlocks.add(current);
  }

  private void buildSiblingFirstOrder(ExecutionBlock current) {
    /*
     |-eb_1404887024677_0004_000007
       |-eb_1404887024677_0004_000006
          |-eb_1404887024677_0004_000005
             |-eb_1404887024677_0004_000004
                |-eb_1404887024677_0004_000003
             |-eb_1404887024677_0004_000002
                |-eb_1404887024677_0004_000001

     In the case of the upper plan, buildDepthFirstOrder() makes the following order in a depth first and postfix order.
       [eb_1, eb_2, eb_3, eb_4, eb_5, eb_6, eb_7]
     The eb_2 doesn't know eb_3's output bytes and uses a size of eb_4's all scan nodes.

     buildSiblingFirstOrder() makes the following order in a sibling order.
       [eb_1, eb_3, eb_2, eb_4, eb_5, eb_6, eb_7]
     In this order the eb_2 knows eb_3's output bytes and the eb_4 also knows eb_1's output bytes.
     */
    preExecutionOrder(new BuildOrderItem(null, current));

    for (BuildOrderItem eachItem: executionOrderedBlocks) {
      if (masterPlan.isLeaf(eachItem.eb.getId())) {
        orderedBlocks.add(eachItem.eb);
        orderRequiredChildCountMap.get(eachItem.parentEB.getId()).decrementAndGet();
      } else {
        if (eachItem.allSiblingsOrdered()) {
          orderedBlocks.addAll(notOrderedSiblingBlocks.stream().map(eachSiblingItem -> eachSiblingItem.eb).collect(Collectors.toList()));
          orderedBlocks.add(eachItem.eb);
          notOrderedSiblingBlocks.clear();
        } else {
          notOrderedSiblingBlocks.add(eachItem);
        }
      }
    }
  }

  private void preExecutionOrder(BuildOrderItem current) {
    Stack<BuildOrderItem> stack = new Stack<>();
    if (!masterPlan.isLeaf(current.eb.getId())) {
      List<ExecutionBlock> children = masterPlan.getChilds(current.eb);
      orderRequiredChildCountMap.put(current.eb.getId(), new AtomicInteger(children.size()));
      for (ExecutionBlock execBlock : children) {
        BuildOrderItem item = new BuildOrderItem(current.eb, execBlock);
        item.setSiblings(children);
        if (!masterPlan.isLeaf(execBlock)) {
          preExecutionOrder(item);
        } else {
          stack.push(item);
        }
      }
      stack.forEach(this::preExecutionOrder);
    }
    executionOrderedBlocks.add(current);
  }

  class BuildOrderItem {
    ExecutionBlock eb;
    ExecutionBlock parentEB;
    List<ExecutionBlockId> siblings = new ArrayList<>();

    BuildOrderItem(ExecutionBlock parentEB, ExecutionBlock eb) {
      this.parentEB = parentEB;
      this.eb = eb;
    }

    public void setSiblings(List<ExecutionBlock> siblings) {
      for (ExecutionBlock eachEB: siblings) {
        if (eachEB.getId().equals(eb.getId())) {
          continue;
        }

        this.siblings.add(eachEB.getId());
      }
    }

    public boolean allSiblingsOrdered() {
      for (ExecutionBlockId eachSibling: siblings) {
        if (orderRequiredChildCountMap.get(eachSibling) != null &&
            orderRequiredChildCountMap.get(eachSibling).get() > 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return eb.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BuildOrderItem)) {
        return false;
      }
      return eb.equals(((BuildOrderItem) obj).eb);
    }

    @Override
    public int hashCode() {
      int result = eb != null ? eb.hashCode() : 0;
      result = 31 * result + (parentEB != null ? parentEB.hashCode() : 0);
      result = 31 * result + (siblings != null ? siblings.hashCode() : 0);
      return result;
    }
  }
}
