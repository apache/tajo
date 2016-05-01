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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Pair;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HashFullOuterJoinExec extends CommonHashJoinExec<Pair<Boolean, TupleList>> {

  private boolean finalLoop; // final loop for right unmatched
  private final List<Tuple> nullTupleList;

  public HashFullOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
                               PhysicalExec inner) {
    super(context, plan, outer, inner);
    nullTupleList = nullTupleList(rightNumCols);
  }

  public Iterator<Tuple> getUnmatchedRight() {

    return new Iterator<Tuple>() {

      private Iterator<Pair<Boolean, TupleList>> iterator1 = tupleSlots.values().iterator();
      private Iterator<Tuple> iterator2;

      @Override
      public boolean hasNext() {
        if (hasMore()) {
          return true;
        }
        for (iterator2 = null; !hasMore() && iterator1.hasNext();) {
          Pair<Boolean, TupleList> next = iterator1.next();
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
        return projector.eval(frameTuple);
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

      // getting corresponding right
      Pair<Boolean, TupleList> hashed = tupleSlots.get(leftKeyExtractor.project(leftTuple));
      if (hashed == null) {
        if (leftFiltered(leftTuple)) {
          iterator = null;
        } else {
          iterator = nullTupleList.iterator();
        }
        continue;
      }
      Iterator<Tuple> rightTuples = joinQualFiltered(leftTuple, rightFiltered(hashed.getSecond()));
      if (!rightTuples.hasNext()) {
        iterator = nullTupleList.iterator();
        continue;
      }
      iterator = rightTuples;
      hashed.setFirst(true);   // match found
    }

    return null;
  }

  private Iterator<Tuple> joinQualFiltered(Tuple leftTuple, Iterator<Tuple> rightTuples) {
    final FrameTuple frameTuple = new FrameTuple();
    frameTuple.setLeft(leftTuple);

    return Iterators.filter(rightTuples, new Predicate<Tuple>() {
      @Override
      public boolean apply(Tuple input) {
        frameTuple.setRight(input);
        return joinQual.eval(frameTuple).isTrue();
      }
    });
  }

  @Override
  protected TupleMap<Pair<Boolean, TupleList>> convert(TupleMap<TupleList> hashed,
                                                       boolean fromCache) throws IOException {
    TupleMap<Pair<Boolean, TupleList>> tuples = new TupleMap<>(hashed.size());
    for (Map.Entry<KeyTuple, TupleList> entry : hashed.entrySet()) {
      // flag: initially false (whether this join key had at least one match on the counter part)
      tuples.putWihtoutKeyCopy(entry.getKey(), new Pair<>(false, entry.getValue()));
    }
    return tuples;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    for (Pair<Boolean, TupleList> value : tupleSlots.values()) {
      value.setFirst(false);
    }
    finalLoop = false;
  }
}

