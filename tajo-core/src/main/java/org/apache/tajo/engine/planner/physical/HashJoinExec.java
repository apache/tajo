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
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HashJoinExec extends CommonHashJoinExec<List<Tuple>> {

  public HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftExec,
      PhysicalExec rightExec) {
    super(context, plan, leftExec, rightExec);
  }

  @Override
  protected Map<Tuple, List<Tuple>> convert(Map<Tuple, List<Tuple>> hashed, boolean fromCache)
      throws IOException {
    return fromCache ? new HashMap<Tuple, List<Tuple>>(hashed) : hashed;
  }

  @Override
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

      Tuple leftTuple = leftChild.next(); // it comes from a disk
      if (leftTuple == null || leftFiltered(leftTuple)) { // if no more tuples in left tuples on disk, a join is completed.
        finished = leftTuple == null;
        continue;
      }

      frameTuple.setLeft(leftTuple);

      // getting corresponding right
      Iterable<Tuple> hashed = getRights(toKey(leftTuple));
      Iterator<Tuple> rightTuples = rightFiltered(hashed);
      if (rightTuples.hasNext()) {
        iterator = rightTuples;
      }
    }

    return null;
  }

  private Iterable<Tuple> getRights(Tuple key) {
    return tupleSlots.get(key);
  }

}
