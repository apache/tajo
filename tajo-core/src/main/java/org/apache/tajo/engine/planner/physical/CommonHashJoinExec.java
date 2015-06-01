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
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * common exec for all hash join execs
 *
 * @param <T> Tuple collection type to load small relation onto in-memory
 */
public abstract class CommonHashJoinExec<T> extends CommonJoinExec {

  protected final List<Column[]> joinKeyPairs;

  // temporal tuples and states for nested loop join
  protected boolean first = true;
  protected Map<Tuple, T> tupleSlots;

  protected Iterator<Tuple> iterator;

  protected final Tuple keyTuple;

  protected final int rightNumCols;
  protected final int leftNumCols;

  protected final int[] leftKeyList;
  protected final int[] rightKeyList;

  protected boolean finished;

  public CommonHashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, plan, outer, inner);

    // HashJoin only can manage equi join key pairs.
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(),
        inner.getSchema(), false);

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = outer.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = inner.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    leftNumCols = outer.getSchema().size();
    rightNumCols = inner.getSchema().size();

    keyTuple = new VTuple(leftKeyList.length);
  }

  protected void loadRightToHashTable() throws IOException {
    ScanExec scanExec = PhysicalPlanUtil.findExecutor(rightChild, ScanExec.class);
    if (scanExec.canBroadcast()) {
      /* If this table can broadcast, all tasks in a node will share the same cache */
      TableCacheKey key = CacheHolder.BroadcastCacheHolder.getCacheKey(
          context, scanExec.getCanonicalName(), scanExec.getFragments());
      loadRightFromCache(key);
    } else {
      this.tupleSlots = convert(buildRightToHashTable(), false);
    }

    first = false;
  }

  protected void loadRightFromCache(TableCacheKey key) throws IOException {
    ExecutionBlockSharedResource sharedResource = context.getSharedResource();

    CacheHolder<Map<Tuple, List<Tuple>>> holder;
    synchronized (sharedResource.getLock()) {
      if (sharedResource.hasBroadcastCache(key)) {
        holder = sharedResource.getBroadcastCache(key);
      } else {
        Map<Tuple, List<Tuple>> built = buildRightToHashTable();
        holder = new CacheHolder.BroadcastCacheHolder(built, rightChild.getInputStats(), null);
        sharedResource.addBroadcastCache(key, holder);
      }
    }
    this.tupleSlots = convert(holder.getData(), true);
  }

  protected Map<Tuple, List<Tuple>> buildRightToHashTable() throws IOException {
    Tuple tuple;
    Map<Tuple, List<Tuple>> map = new HashMap<Tuple, List<Tuple>>(100000);

    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      Tuple keyTuple = new VTuple(joinKeyPairs.size());
      for (int i = 0; i < rightKeyList.length; i++) {
        keyTuple.put(i, tuple.asDatum(rightKeyList[i]));
      }

      /*
       * TODO
       * Currently, some physical executors can return new instances of tuple, but others not.
       * This sometimes causes wrong results due to the singleton Tuple instance.
       * The below line is a temporal solution to fix this problem.
       * This will be improved at https://issues.apache.org/jira/browse/TAJO-1343.
       */
      try {
        tuple = tuple.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }

      List<Tuple> newValue = map.get(keyTuple);
      if (newValue == null) {
        map.put(keyTuple, newValue = new ArrayList<Tuple>());
      }
      // if source is scan or groupby, it needs not to be cloned
      newValue.add(new VTuple(tuple));
    }
    return map;
  }

  // todo: convert loaded data to cache condition
  protected abstract Map<Tuple, T> convert(Map<Tuple, List<Tuple>> hashed, boolean fromCache)
      throws IOException;

  protected Tuple toKey(final Tuple outerTuple) {
    for (int i = 0; i < leftKeyList.length; i++) {
      keyTuple.put(i, outerTuple.asDatum(leftKeyList[i]));
    }
    return keyTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    finished = false;
    iterator = null;
  }

  @Override
  public void close() throws IOException {
    super.close();
    iterator = null;
    if (tupleSlots != null) {
      tupleSlots.clear();
      tupleSlots = null;
    }
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

    TableStats rightInputStats = rightChild.getInputStats();
    if (rightInputStats != null) {
      inputStats.setNumBytes(inputStats.getNumBytes() + rightInputStats.getNumBytes());
      inputStats.setReadBytes(inputStats.getReadBytes() + rightInputStats.getReadBytes());
      inputStats.setNumRows(inputStats.getNumRows() + rightInputStats.getNumRows());
    }

    return inputStats;
  }
}
