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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;


public class HashLeftOuterJoinExec extends BinaryPhysicalExec {
  // from logical plan
  protected JoinNode plan;
  protected EvalNode joinQual;   // ex) a.id = b.id
  protected EvalNode joinFilter; // ex) a > 10

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

  // projection
  protected Projector projector;

  private int rightNumCols;
  private TableStats cachedRightTableStats;
  private static final Log LOG = LogFactory.getLog(HashLeftOuterJoinExec.class);

  public HashLeftOuterJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftChild,
                               PhysicalExec rightChild) {
    super(context, SchemaUtil.merge(leftChild.getSchema(), rightChild.getSchema()),
        plan.getOutSchema(), leftChild, rightChild);
    this.plan = plan;

    List<EvalNode> joinQuals = Lists.newArrayList();
    List<EvalNode> joinFilters = Lists.newArrayList();
    for (EvalNode eachQual : AlgebraicUtil.toConjunctiveNormalFormArray(plan.getJoinQual())) {
      if (EvalTreeUtil.isJoinQual(eachQual, true)) {
        joinQuals.add(eachQual);
      } else {
        joinFilters.add(eachQual);
      }
    }

    this.joinQual = AlgebraicUtil.createSingletonExprFromCNF(joinQuals.toArray(new EvalNode[joinQuals.size()]));
    if (joinFilters.size() > 0) {
      this.joinFilter = AlgebraicUtil.createSingletonExprFromCNF(joinFilters.toArray(new EvalNode[joinFilters.size()]));
    } else {
      this.joinFilter = null;
    }

    // HashJoin only can manage equi join key pairs.
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, leftChild.getSchema(),
        rightChild.getSchema(), false);

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = leftChild.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = rightChild.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.size());
    leftKeyTuple = new VTuple(leftKeyList.length);

    rightNumCols = rightChild.getSchema().size();

    joinQual.bind(inSchema);
    if (joinFilter != null) {
      joinFilter.bind(inSchema);
    }
  }

  @Override
  protected void compile() {
    joinQual = context.getPrecompiledEval(inSchema, joinQual);
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
          // this left tuple doesn't have a match on the right, and output a tuple with the nulls padded rightTuple
          Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
          frameTuple.set(leftTuple, nullPaddedTuple);
          projector.eval(frameTuple, outTuple);
          // we simulate we found a match, which is exactly the null padded one
          shouldGetLeftTuple = true;
          return outTuple;
        }
      }

      // getting a next right tuple on in-memory hash table.
      rightTuple = iterator.next();
      if (!iterator.hasNext()) { // no more right tuples for this hash key
        shouldGetLeftTuple = true;
      }

      frameTuple.set(leftTuple, rightTuple); // evaluate a join condition on both tuples

      // if there is no join filter, it is always true.
      boolean satisfiedWithFilter = joinFilter == null || joinFilter.eval(frameTuple).isTrue();
      boolean satisfiedWithJoinCondition = joinQual.eval(frameTuple).isTrue();

      // if a composited tuple satisfies with both join filter and join condition
      if (satisfiedWithJoinCondition && satisfiedWithFilter) {
        projector.eval(frameTuple, outTuple);
        return outTuple;
      } else {

        // if join filter is satisfied, the left outer join (LOJ) operator should return the null padded tuple
        // only once. Then, LOJ operator should take the next left tuple.
        if (!satisfiedWithFilter) {
          shouldGetLeftTuple = true;
        }

        // null padding
        Tuple nullPaddedTuple = TupleUtil.createNullPaddedTuple(rightNumCols);
        frameTuple.set(leftTuple, nullPaddedTuple);

        projector.eval(frameTuple, outTuple);
        return outTuple;
      }
    }

    return outTuple;
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
    tupleSlots.clear();
    tupleSlots = null;
    iterator = null;
    plan = null;
    joinQual = null;
    joinFilter = null;
    projector = null;
  }

  public JoinNode getPlan() {
    return this.plan;
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

