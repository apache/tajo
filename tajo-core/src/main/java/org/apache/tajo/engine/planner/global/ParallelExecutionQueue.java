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

import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class ParallelExecutionQueue implements ExecutionQueue, Iterable<ExecutionBlock> {

  private static final Log LOG = LogFactory.getLog(ParallelExecutionQueue.class);

  private final int maximum;
  private final MasterPlan masterPlan;
  private final List<Deque<ExecutionBlock>> executable;
  private final Set<ExecutionBlockId> executed = new HashSet<ExecutionBlockId>();

  public ParallelExecutionQueue(MasterPlan masterPlan, int maximum) {
    this.masterPlan = masterPlan;
    this.maximum = maximum;
    this.executable = toStacks(masterPlan.getRoot());
  }

  private List<Deque<ExecutionBlock>> toStacks(ExecutionBlock root) {
    List<Deque<ExecutionBlock>> stacks = new ArrayList<Deque<ExecutionBlock>>();
    toStacks(root, stacks, new ArrayList<ExecutionBlock>());
    return stacks;
  }

  // currently, diamond shaped DAG is not supported in tajo
  private void toStacks(ExecutionBlock current, List<Deque<ExecutionBlock>> queues,
                        List<ExecutionBlock> stack) {
    stack.add(current);
    if (masterPlan.isLeaf(current.getId())) {
      queues.add(new ArrayDeque<ExecutionBlock>(stack));
    } else {
      List<ExecutionBlock> children = masterPlan.getChilds(current);
      for (int i = 0; i < children.size(); i++) {
        toStacks(children.get(i), queues, i == 0 ? stack : new Stack<ExecutionBlock>());
      }
    }
  }

  @Override
  public synchronized int size() {
    int size = 0;
    for (Deque<ExecutionBlock> queue : executable) {
      size += queue.size();
    }
    return size;
  }

  @Override
  public synchronized ExecutionBlock[] first() {
    int max = Math.min(maximum, executable.size());
    List<ExecutionBlock> result = new ArrayList<ExecutionBlock>();
    for (Deque<ExecutionBlock> queue : executable) {
      if (result.size() < max && isExecutableNow(queue.peekLast())) {
        result.add(queue.removeLast());
      }
    }
    LOG.info("Initial executable blocks " + result);
    return result.toArray(new ExecutionBlock[result.size()]);
  }

  @Override
  public synchronized ExecutionBlock[] next(ExecutionBlockId doneNow) {
    executed.add(doneNow);

    int remaining = 0;
    for (Deque<ExecutionBlock> queue : executable) {
      if (!queue.isEmpty() && isExecutableNow(queue.peekLast())) {
        LOG.info("Next executable block " + queue.peekLast());
        return new ExecutionBlock[]{queue.removeLast()};
      }
      remaining += queue.size();
    }
    return remaining > 0 ? new ExecutionBlock[0] : null;
  }

  private boolean isExecutableNow(ExecutionBlock current) {
    ExecutionBlock parent = masterPlan.getParent(current);

    List<ExecutionBlock> dependents = masterPlan.getChilds(current);
    if (parent != null && masterPlan.getChannel(current.getId(), parent.getId()).needShuffle()) {
      // add all children of sibling for partitioning
      dependents = new ArrayList<ExecutionBlock>();
      for (ExecutionBlock sibling : masterPlan.getChilds(parent)) {
        dependents.addAll(masterPlan.getChilds(sibling));
      }
    }
    for (ExecutionBlock child : dependents) {
      if (!executed.contains(child.getId())) {
        return false;   // there's something should be done before this
      }
    }
    return true;
  }

  @Override
  public Iterator<ExecutionBlock> iterator() {
    return Iterables.concat(executable).iterator();
  }
}
