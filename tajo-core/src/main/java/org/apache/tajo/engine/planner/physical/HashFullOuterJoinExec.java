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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Pair;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class HashFullOuterJoinExec extends CommonHashJoinExec<Pair<Boolean, List<Tuple>>> {

  private boolean finalLoop; // final loop for right unmatched

  public HashFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                               PhysicalExec inner) {
    super(context, plan, outer, inner);
  }

  public Iterator<Tuple> getUnmatchedRight() {

    return new Iterator<Tuple>() {

      private Iterator<Pair<Boolean, List<Tuple>>> iterator1 = tupleSlots.values().iterator();
      private Iterator<Tuple> iterator2;

      @Override
      public boolean hasNext() {
        if (hasMore()) {
          return true;
        }
        for (iterator2 = null; !hasMore() && iterator1.hasNext();) {
          Pair<Boolean, List<Tuple>> next = iterator1.next();
          if (!next.getFirst()) {
            iterator2 = next.getSecond().iterator();
          }
        }
        return hasMore();
      }

      private boolean hasMore() {
        return iterator2 != null && iterator2.hasNext();
      }

      @Override
      public Tuple next() {
        return iterator2.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    while (!context.isStopped() && !finished) {
      if (iterator != null && iterator.hasNext()) {
        frameTuple.setRight(iterator.next());
        projector.eval(frameTuple, outTuple);
        return outTuple;
      }
      if (finalLoop) {
        finished = true;
        return null;
      }
      Tuple leftTuple = leftChild.next();
      if (leftTuple == null) {
      // if no more tuples in left tuples, a join is completed.
        // in this stage we can begin outputing tuples from the right operand (which were before in tupleSlots) null padded on the left side
        frameTuple.setLeft(NullTuple.create(leftNumCols));
        iterator = getUnmatchedRight();
        finalLoop = true;
        continue;
      }
      frameTuple.setLeft(leftTuple);

      if (leftFiltered(leftTuple)) {
        iterator = nullIterator(rightNumCols);
        continue;
      }
      // getting corresponding right
      Pair<Boolean, List<Tuple>> hashed = tupleSlots.get(toKey(leftTuple));
      if (hashed == null) {
        iterator = nullIterator(rightNumCols);
        continue;
      }
      Iterator<Tuple> rightTuples = rightFiltered(hashed.getSecond());
      if (!rightTuples.hasNext()) {
        iterator = nullIterator(rightNumCols);
        continue;
      }
      iterator = rightTuples;
      hashed.setFirst(true);   // match found
    }

    return null;
  }

  @Override
  protected Map<Tuple, Pair<Boolean, List<Tuple>>> convert(Map<Tuple, List<Tuple>> hashed,
                                                           boolean fromCache) throws IOException {
    Map<Tuple, Pair<Boolean, List<Tuple>>> tuples = new HashMap<Tuple, Pair<Boolean, List<Tuple>>>(hashed.size());
    for (Map.Entry<Tuple, List<Tuple>> entry : hashed.entrySet()) {
      // flag: initially false (whether this join key had at least one match on the counter part)
      tuples.put(entry.getKey(), new Pair<Boolean, List<Tuple>>(false, entry.getValue()));
    }
    return tuples;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    for (Pair<Boolean, List<Tuple>> value : tupleSlots.values()) {
      value.setFirst(false);
    }
    finalLoop = false;
  }
}

