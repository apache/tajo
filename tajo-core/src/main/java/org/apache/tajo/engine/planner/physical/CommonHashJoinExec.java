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
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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

  protected final boolean isCrossJoin;
  protected final List<Column[]> joinKeyPairs;

  protected final int rightNumCols;
  protected final int leftNumCols;

  protected final Column[] leftKeyList;
  protected final Column[] rightKeyList;

  protected final KeyProjector leftKeyExtractor;

  protected boolean finished;

  protected TableStats tableStatsOfCachedRightChild = null;

  public CommonHashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, plan, outer, inner);

    switch (plan.getJoinType()) {

      case CROSS:
        if (hasJoinQual) {
          throw new TajoInternalError("Cross join cannot evaluate join conditions.");
        } else {
          isCrossJoin = true;
          joinKeyPairs = null;
          rightNumCols = leftNumCols = -1;
          leftKeyList = rightKeyList = null;
          leftKeyExtractor = null;
        }
        break;

      case INNER:
        // Other join types except INNER join can have empty join condition.
        if (!hasJoinQual) {
          throw new TajoInternalError("Inner join must have any join conditions.");
        }
      default:
        isCrossJoin = false;
        // HashJoin only can manage equi join key pairs.
        this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(),
            inner.getSchema(), false);

        leftKeyList = new Column[joinKeyPairs.size()];
        rightKeyList = new Column[joinKeyPairs.size()];

        for (int i = 0; i < joinKeyPairs.size(); i++) {
          leftKeyList[i] = outer.getSchema().getColumn(joinKeyPairs.get(i)[0].getQualifiedName());
          rightKeyList[i] = inner.getSchema().getColumn(joinKeyPairs.get(i)[1].getQualifiedName());
        }

        leftNumCols = outer.getSchema().size();
        rightNumCols = inner.getSchema().size();

        leftKeyExtractor = new KeyProjector(leftSchema, leftKeyList);
        break;
    }
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
    if (isCrossJoin) {
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
    TupleMap<TupleList> map = new TupleMap<>(100000);
    KeyProjector keyProjector = new KeyProjector(rightSchema, rightKeyList);

    while (!context.isStopped() && (tuple = rightChild.next()) != null) {
      KeyTuple keyTuple = keyProjector.project(tuple);
      TupleList newValue = map.get(keyTuple);
      if (newValue == null) {
        map.put(keyTuple, newValue = new TupleList());
      }
      // if source is scan or groupby, it needs not to be cloned
      newValue.add(tuple);
    }
    return map;
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
