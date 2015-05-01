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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

public class HashJoinExec extends CommonJoinExec {

  protected List<Column[]> joinKeyPairs;

  // temporal tuples and states for nested loop join
  protected boolean first = true;
  protected FrameTuple frameTuple;
  protected Tuple outTuple = null;
  protected Map<Tuple, List<Tuple>> tupleSlots;
  protected Iterator<Tuple> iterator = null;
  protected Tuple leftTuple;
  protected Tuple leftKeyTuple;

  protected int [] leftKeyList;
  protected int [] rightKeyList;

  protected boolean finished = false;
  protected boolean shouldGetLeftTuple = true;

  private TableStats cachedRightTableStats;

  public HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftExec,
      PhysicalExec rightExec) {
    super(context, plan, leftExec, rightExec);

    // HashJoin only can manage equi join key pairs.
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, leftExec.getSchema(),
        rightExec.getSchema(), false);

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = leftExec.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = rightExec.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());
    leftKeyTuple = new VTuple(leftKeyList.length);
  }

  protected void getKeyLeftTuple(final Tuple outerTuple, Tuple keyTuple) {
    for (int i = 0; i < leftKeyList.length; i++) {
      keyTuple.put(i, outerTuple.get(leftKeyList[i]));
    }
  }

  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    Tuple rightTuple;
    boolean found = false;

    while(!context.isStopped() && !finished) {
      if (shouldGetLeftTuple) { // initially, it is true.
        // getting new outer
        leftTuple = leftChild.next(); // it comes from a disk
        if (leftTuple == null) { // if no more tuples in left tuples on disk, a join is completed.
          finished = true;
          return null;
        }

        // getting corresponding right
        getKeyLeftTuple(leftTuple, leftKeyTuple); // get a left key tuple
        List<Tuple> rightTuples = tupleSlots.get(leftKeyTuple);
        if (rightTuples != null) { // found right tuples on in-memory hash table.
          iterator = rightTuples.iterator();
          shouldGetLeftTuple = false;
        } else {
          shouldGetLeftTuple = true;
          continue;
        }
      }

      // getting a next right tuple on in-memory hash table.
      rightTuple = iterator.next();
      frameTuple.set(leftTuple, rightTuple); // evaluate a join condition on both tuples
      if (joinQual.eval(frameTuple).isTrue()) { // if both tuples are joinable
        projector.eval(frameTuple, outTuple);
        found = true;
      }

      if (!iterator.hasNext()) { // no more right tuples for this hash key
        shouldGetLeftTuple = true;
      }

      if (found) {
        break;
      }
    }

    return new VTuple(outTuple);
  }

  protected void loadRightToHashTable() throws IOException {
    ScanExec scanExec = PhysicalPlanUtil.findExecutor(rightChild, ScanExec.class);
    if (scanExec.canBroadcast()) {
      /* If this table can broadcast, all tasks in a node will share the same cache */
      TableCacheKey key = CacheHolder.BroadcastCacheHolder.getCacheKey(
          context, scanExec.getCanonicalName(), scanExec.getFragments());
      loadRightFromCache(key);
    } else {
      this.tupleSlots = buildRightToHashTable();
    }

    first = false;
  }

  protected void loadRightFromCache(TableCacheKey key) throws IOException {
    ExecutionBlockSharedResource sharedResource = context.getSharedResource();
    synchronized (sharedResource.getLock()) {
      if (sharedResource.hasBroadcastCache(key)) {
        CacheHolder<Map<Tuple, List<Tuple>>> data = sharedResource.getBroadcastCache(key);
        this.tupleSlots = data.getData();
        this.cachedRightTableStats = data.getTableStats();
      } else {
        CacheHolder.BroadcastCacheHolder holder =
            new CacheHolder.BroadcastCacheHolder(buildRightToHashTable(), rightChild.getInputStats(), null);
        sharedResource.addBroadcastCache(key, holder);
        CacheHolder<Map<Tuple, List<Tuple>>> data = sharedResource.getBroadcastCache(key);
        this.tupleSlots = data.getData();
        this.cachedRightTableStats = data.getTableStats();
      }
    }
  }

  private Map<Tuple, List<Tuple>> buildRightToHashTable() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    Map<Tuple, List<Tuple>> map = new HashMap<Tuple, List<Tuple>>(100000);

    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      keyTuple = new VTuple(joinKeyPairs.size());
      for (int i = 0; i < rightKeyList.length; i++) {
        keyTuple.put(i, tuple.get(rightKeyList[i]));
      }

      List<Tuple> newValue = map.get(keyTuple);

      if (newValue != null) {
        newValue.add(tuple);
      } else {
        newValue = new ArrayList<Tuple>();
        newValue.add(tuple);
        map.put(keyTuple, newValue);
      }
    }

    return map;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    tupleSlots.clear();
    first = true;

    finished = false;
    iterator = null;
    shouldGetLeftTuple = true;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (tupleSlots != null) {
      tupleSlots.clear();
      tupleSlots = null;
    }

    iterator = null;
  }

  @Override
  public TableStats getInputStats() {
    if (leftChild == null) {
      return inputStats;
    }
    TableStats leftInputStats = leftChild.getInputStats();
    inputStats.setNumBytes(0);
    inputStats.setReadBytes(0);
    inputStats.setNumRows(0);

    if (leftInputStats != null) {
      inputStats.setNumBytes(leftInputStats.getNumBytes());
      inputStats.setReadBytes(leftInputStats.getReadBytes());
      inputStats.setNumRows(leftInputStats.getNumRows());
    }

    TableStats rightInputStats = cachedRightTableStats == null ? rightChild.getInputStats() : cachedRightTableStats;
    if (rightInputStats != null) {
      inputStats.setNumBytes(inputStats.getNumBytes() + rightInputStats.getNumBytes());
      inputStats.setReadBytes(inputStats.getReadBytes() + rightInputStats.getReadBytes());
      inputStats.setNumRows(inputStats.getNumRows() + rightInputStats.getNumRows());
    }

    return inputStats;
  }
}
