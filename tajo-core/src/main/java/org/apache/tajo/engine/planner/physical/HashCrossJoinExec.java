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
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HashCrossJoinExec extends HashJoinExec {

  private Iterator<List<Tuple>> outIterator;

  public HashCrossJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftExec,
                           PhysicalExec rightExec) {
    super(context, plan, leftExec, rightExec);
  }

  @Override
  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }
    if (tupleSlots.isEmpty()) {
      return null;
    }

    while (!context.isStopped() && !finished) {
      if (shouldGetLeftTuple) {
        leftTuple = leftChild.next();
        if (leftTuple == null) {
          finished = true;
          return null;
        }
        outIterator = tupleSlots.values().iterator();
        iterator = outIterator.next().iterator();
        shouldGetLeftTuple = false;
      }

      // getting a next right tuple on in-memory hash table.
      while (!iterator.hasNext() && outIterator.hasNext()) {
        iterator = outIterator.next().iterator();
      }
      if (!iterator.hasNext()) {
        shouldGetLeftTuple = true;
        continue;
      }
      frameTuple.set(leftTuple, iterator.next());
      projector.eval(frameTuple, outTuple);
      return new VTuple(outTuple);
    }

    return null;
  }
}
