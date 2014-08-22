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
 * If found, it returns the tuple of the FROM side table.
 */
public class HashLeftSemiJoinExec extends HashJoinExec {
  private Tuple rightNullTuple;

  public HashLeftSemiJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec fromSideChild,
                              PhysicalExec inSideChild) {
    super(context, plan, fromSideChild, inSideChild);
    // NUll Tuple
    rightNullTuple = new VTuple(leftChild.outColumnNum);
    for (int i = 0; i < leftChild.outColumnNum; i++) {
      rightNullTuple.put(i, NullDatum.get());
    }
  }

  protected void compile() {
    joinQual = context.getPrecompiledEval(inSchema, joinQual);
  }

  /**
   * The End of Tuple (EOT) condition is true only when no more tuple in the left relation (on disk).
   * next() method finds the first unmatched tuple from both tables.
   *
   * For each left tuple on the disk, next() tries to find at least one matched tuple from the hash table.
   *
   * In more detail, until there is a hash bucket matched to the left tuple in the hash table, it continues to traverse
   * the left tuples. If next() finds the matched bucket in the hash table, it finds any matched tuple in the bucket.
   * If found, it returns the composite tuple immediately without finding more matched tuple in the bucket.
   *
   * @return The tuple which is firstly matched to a given join condition.
   * @throws java.io.IOException
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
        continue;
      }

      // Reach here only when a hash bucket is found. Then, it checks all tuples in the found bucket.
      // If it finds any matched tuple, it returns the tuple immediately.
      notFound = true;
      while (notFound && iterator.hasNext()) {
        rightTuple = iterator.next();
        frameTuple.set(leftTuple, rightTuple);
        if (joinQual.eval(inSchema, frameTuple).isTrue()) { // if the matched one is found
          notFound = false;
          projector.eval(frameTuple, outTuple);
        }
      }

      if (!notFound) { // if there is no matched tuple
        break;
      }
    }

    return outTuple;
  }
}
