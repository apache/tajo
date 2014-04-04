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

import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.List;

/**
 * Prepare a hash table of the NOT IN side of the join. Scan the FROM side table.
 * For each tuple of the FROM side table, it tries to find a matched tuple from the hash table for the NOT INT side.
 * If not found, it returns the tuple of the FROM side table with null padding.
 */
public class HashLeftAntiJoinExec extends HashJoinExec {
  private Tuple rightNullTuple;

  public HashLeftAntiJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec fromSideChild,
                              PhysicalExec notInSideChild) {
    super(context, plan, fromSideChild, notInSideChild);
    // NUll Tuple
    rightNullTuple = new VTuple(leftChild.outColumnNum);
    for (int i = 0; i < leftChild.outColumnNum; i++) {
      rightNullTuple.put(i, NullDatum.get());
    }
  }

  /**
   * The End of Tuple (EOT) condition is true only when no more tuple in the left relation (on disk).
   * next() method finds the first unmatched tuple from both tables.
   *
   * For each left tuple, next() tries to find the right tuple from the hash table. If there is no hash bucket
   * in the hash table. It returns a tuple. If next() find the hash bucket in the hash table, it reads tuples in
   * the found bucket sequentially. If it cannot find tuple in the bucket, it returns a tuple.
   *
   * @return The tuple which is unmatched to a given join condition.
   * @throws IOException
   */
  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    Tuple rightTuple;
    boolean notFound;

    while(!finished) {

      // getting new outer
      leftTuple = leftChild.next(); // it comes from a disk
      if (leftTuple == null) { // if no more tuples in left tuples on disk, a join is completed.
        finished = true;
        return null;
      }

      // Try to find a hash bucket in in-memory hash table
      getKeyLeftTuple(leftTuple, leftKeyTuple);
      List<Tuple> rightTuples = tupleSlots.get(leftKeyTuple);
      if (rightTuples != null) {
        // if found, it gets a hash bucket from the hash table.
        iterator = rightTuples.iterator();
      } else {
        // if not found, it returns a tuple.
        frameTuple.set(leftTuple, rightNullTuple);
        projector.eval(frameTuple, outTuple);
        return outTuple;
      }

      // Reach here only when a hash bucket is found. Then, it checks all tuples in the found bucket.
      // If it finds a matched tuple, it escapes the loop for all tuples in the hash bucket.
      notFound = true;
      while (notFound && iterator.hasNext()) {
        rightTuple = iterator.next();
        frameTuple.set(leftTuple, rightTuple);
        if (joinQual.eval(inSchema, frameTuple).isTrue()) { // if the matched one is found
          notFound = false;
        }
      }

      if (notFound) { // if there is no matched tuple
        frameTuple.set(leftTuple, rightNullTuple);
        projector.eval(frameTuple, outTuple);
        break;
      }
    }

    return outTuple;
  }
}
