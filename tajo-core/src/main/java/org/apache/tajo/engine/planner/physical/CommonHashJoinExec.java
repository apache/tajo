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

import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * common exec for all hash join execs
 *
 * @param <T> Tuple collection type to load small relation onto in-memory
 */
public abstract class CommonHashJoinExec<T> extends CommonJoinExec {

  // temporal tuples and states for nested loop join
  protected boolean first = true;
  protected TupleMap<T> tupleSlots;
  protected Iterator<Tuple> iterator;

  protected boolean finished;

  protected TableStats tableStatsOfCachedRightChild = null;

  public CommonHashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, plan, outer, inner);
  }

  protected void loadRightToHashTable() throws IOException {
    ScanExec scanExec = PhysicalPlanUtil.findExecutor(rightChild, ScanExec.class);
    if (scanExec.canBroadcast()) {
      /* If this table can broadcast, all tasks in a node will share the same cache */
      TableCacheKey key = CacheHolder.BroadcastCacheHolder.getCacheKey(context, scanExec);
      loadRightFromCache(key);
    } else {
      this.tupleSlots = convert(buildRightToHashTable(), false);
    }

    first = false;
  }

  protected void loadRightFromCache(TableCacheKey key) throws IOException {
    ExecutionBlockSharedResource sharedResource = context.getSharedResource();

    CacheHolder<TupleMap<TupleList>> holder;
    synchronized (sharedResource.getLock()) {
      if (sharedResource.hasBroadcastCache(key)) {
        holder = sharedResource.getBroadcastCache(key);
      } else {
        TupleMap<TupleList> built = buildRightToHashTable();
        holder = new CacheHolder.BroadcastCacheHolder(built, rightChild.getInputStats(), null);
        sharedResource.addBroadcastCache(key, holder);
      }
    }
    this.tableStatsOfCachedRightChild = holder.getTableStats();
    this.tupleSlots = convert(holder.getData(), true);
  }

  protected TupleMap<TupleList> buildRightToHashTable() throws IOException {
    if (plan.getJoinType().equals(JoinType.CROSS)) {
      return buildRightToHashTableForCrossJoin();
    } else {
      return buildRightToHashTableForNonCrossJoin();
    }
  }

  protected TupleMap<TupleList> buildRightToHashTableForCrossJoin() throws IOException {
    Tuple tuple;
    TupleMap<TupleList> map = new TupleMap<>(1);
    TupleList tuples = new TupleList();

    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      tuples.add(tuple);
    }
    map.put(null, tuples);
    return map;
  }

  protected TupleMap<TupleList> buildRightToHashTableForNonCrossJoin() throws IOException {
    Tuple tuple;
    TupleMap<TupleList> map = new TupleMap<>(context.getQueryContext().getInt(SessionVars.JOIN_HASH_TABLE_SIZE));

    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      KeyTuple keyTuple = rightKeyExtractor.project(tuple);
      if (isLoadable(plan, keyTuple)) { // filter out null values
        TupleList newValue = map.get(keyTuple);
        if (newValue == null) {
          map.put(keyTuple, newValue = new TupleList());
        }
        // if source is scan or groupby, it needs not to be cloned
        newValue.add(tuple);
      }
    }
    return map;
  }

  /**
   * Check the given tuple is able to be loaded into the hash table or not.
   * When the plan is full outer join, every tuple including null values should be loaded
   * because both input tables of the join are preserved-row relations as well as null-supplying relations.
   *
   * Otherwise, except for anti join, only the tuples not containing null values should be loaded.
   *
   * For the case of anti join, the right table is expected to be empty if there are any null values.
   *
   * @param plan
   * @param tuple
   * @return
   */
  private static boolean isLoadable(JoinNode plan, Tuple tuple) {
    return plan.getJoinType().equals(JoinType.FULL_OUTER)
        || Arrays.stream(tuple.getValues()).noneMatch(Datum::isNull);
  }

  // todo: convert loaded data to cache condition
  protected abstract TupleMap<T> convert(TupleMap<TupleList> hashed, boolean fromCache)
      throws IOException;

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

    TableStats rightInputStats = tableStatsOfCachedRightChild == null ?
        rightChild.getInputStats() : tableStatsOfCachedRightChild;
    if (rightInputStats != null) {
      inputStats.setNumBytes(inputStats.getNumBytes() + rightInputStats.getNumBytes());
      inputStats.setReadBytes(inputStats.getReadBytes() + rightInputStats.getReadBytes());
      inputStats.setNumRows(inputStats.getNumRows() + rightInputStats.getNumRows());
    }

    return inputStats;
  }
}
