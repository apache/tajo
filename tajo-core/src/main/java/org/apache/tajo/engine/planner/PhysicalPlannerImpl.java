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

/**
 *
 */
package org.apache.tajo.engine.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.SortSpecProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.DistinctAggregationAlgorithm;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.SortSpecArray;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.IndexUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Stack;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;
import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ColumnPartitionEnforcer.ColumnPartitionAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty;
import static org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty.EnforceType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.GroupbyEnforce.GroupbyAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.JoinEnforce.JoinAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.SortEnforce;

public class PhysicalPlannerImpl implements PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlannerImpl.class);
  private static final int UNGENERATED_PID = -1;
  private final long INNER_JOIN_INMEMORY_HASH_THRESHOLD;

  protected final TajoConf conf;
  protected final AbstractStorageManager sm;

  public PhysicalPlannerImpl(final TajoConf conf, final AbstractStorageManager sm) {
    this.conf = conf;
    this.sm = sm;

    this.INNER_JOIN_INMEMORY_HASH_THRESHOLD = conf.getLongVar(ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD);
  }

  public PhysicalExec createPlan(final TaskAttemptContext context, final LogicalNode logicalPlan)
      throws InternalException {

    PhysicalExec execPlan;

    try {
      execPlan = createPlanRecursive(context, logicalPlan, new Stack<LogicalNode>());
      if (execPlan instanceof StoreTableExec
          || execPlan instanceof RangeShuffleFileWriteExec
          || execPlan instanceof HashShuffleFileWriteExec
          || execPlan instanceof ColPartitionStoreExec) {
        return execPlan;
      } else if (context.getDataChannel() != null) {
        return buildOutputOperator(context, logicalPlan, execPlan);
      } else {
        return execPlan;
      }
    } catch (IOException ioe) {
      throw new InternalException(ioe);
    }
  }

  private PhysicalExec buildOutputOperator(TaskAttemptContext context, LogicalNode plan,
                                           PhysicalExec execPlan) throws IOException {
    DataChannel channel = context.getDataChannel();
    ShuffleFileWriteNode shuffleFileWriteNode = LogicalPlan.createNodeWithoutPID(ShuffleFileWriteNode.class);
    shuffleFileWriteNode.setStorageType(context.getDataChannel().getStoreType());
    shuffleFileWriteNode.setInSchema(plan.getOutSchema());
    shuffleFileWriteNode.setOutSchema(plan.getOutSchema());
    shuffleFileWriteNode.setShuffle(channel.getShuffleType(), channel.getShuffleKeys(), channel.getShuffleOutputNum());
    shuffleFileWriteNode.setChild(plan);

    PhysicalExec outExecPlan = createShuffleFileWritePlan(context, shuffleFileWriteNode, execPlan);
    return outExecPlan;
  }

  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode, Stack<LogicalNode> stack)
      throws IOException {
    PhysicalExec leftExec;
    PhysicalExec rightExec;

    switch (logicalNode.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
        stack.push(rootNode);
        leftExec = createPlanRecursive(ctx, rootNode.getChild(), stack);
        stack.pop();
        return leftExec;

      case EXPRS:
        EvalExprNode evalExpr = (EvalExprNode) logicalNode;
        return new EvalExprExec(ctx, evalExpr);

      case CREATE_TABLE:
      case INSERT:
      case STORE:
        StoreTableNode storeNode = (StoreTableNode) logicalNode;
        stack.push(storeNode);
        leftExec = createPlanRecursive(ctx, storeNode.getChild(), stack);
        stack.pop();
        return createStorePlan(ctx, storeNode, leftExec);

      case SELECTION:
        SelectionNode selNode = (SelectionNode) logicalNode;
        stack.push(selNode);
        leftExec = createPlanRecursive(ctx, selNode.getChild(), stack);
        stack.pop();
        return new SelectionExec(ctx, selNode, leftExec);

      case PROJECTION:
        ProjectionNode prjNode = (ProjectionNode) logicalNode;
        stack.push(prjNode);
        leftExec = createPlanRecursive(ctx, prjNode.getChild(), stack);
        stack.pop();
        return new ProjectionExec(ctx, prjNode, leftExec);

      case TABLE_SUBQUERY: {
        TableSubQueryNode subQueryNode = (TableSubQueryNode) logicalNode;
        stack.push(subQueryNode);
        leftExec = createPlanRecursive(ctx, subQueryNode.getSubQuery(), stack);
        stack.pop();
        return new ProjectionExec(ctx, subQueryNode, leftExec);
      }

      case PARTITIONS_SCAN:
      case SCAN:
        leftExec = createScanPlan(ctx, (ScanNode) logicalNode, stack);
        return leftExec;

      case GROUP_BY:
        GroupbyNode grpNode = (GroupbyNode) logicalNode;
        stack.push(grpNode);
        leftExec = createPlanRecursive(ctx, grpNode.getChild(), stack);
        stack.pop();
        return createGroupByPlan(ctx, grpNode, leftExec);

      case WINDOW_AGG:
        WindowAggNode windowAggNode = (WindowAggNode) logicalNode;
        stack.push(windowAggNode);
        leftExec = createPlanRecursive(ctx, windowAggNode.getChild(), stack);
        stack.pop();
        return createWindowAgg(ctx, windowAggNode, leftExec);

      case DISTINCT_GROUP_BY:
        DistinctGroupbyNode distinctNode = (DistinctGroupbyNode) logicalNode;
        stack.push(distinctNode);
        leftExec = createPlanRecursive(ctx, distinctNode.getChild(), stack);
        stack.pop();
        return createDistinctGroupByPlan(ctx, distinctNode, leftExec);

      case HAVING:
        HavingNode havingNode = (HavingNode) logicalNode;
        stack.push(havingNode);
        leftExec = createPlanRecursive(ctx, havingNode.getChild(), stack);
        stack.pop();
        return new HavingExec(ctx, havingNode, leftExec);

      case SORT:
        SortNode sortNode = (SortNode) logicalNode;
        stack.push(sortNode);
        leftExec = createPlanRecursive(ctx, sortNode.getChild(), stack);
        stack.pop();
        return createSortPlan(ctx, sortNode, leftExec);

      case JOIN:
        JoinNode joinNode = (JoinNode) logicalNode;
        stack.push(joinNode);
        leftExec = createPlanRecursive(ctx, joinNode.getLeftChild(), stack);
        rightExec = createPlanRecursive(ctx, joinNode.getRightChild(), stack);
        stack.pop();
        return createJoinPlan(ctx, joinNode, leftExec, rightExec);

      case UNION:
        UnionNode unionNode = (UnionNode) logicalNode;
        stack.push(unionNode);
        leftExec = createPlanRecursive(ctx, unionNode.getLeftChild(), stack);
        rightExec = createPlanRecursive(ctx, unionNode.getRightChild(), stack);
        stack.pop();
        return new UnionExec(ctx, leftExec, rightExec);

      case LIMIT:
        LimitNode limitNode = (LimitNode) logicalNode;
        stack.push(limitNode);
        leftExec = createPlanRecursive(ctx, limitNode.getChild(), stack);
        stack.pop();
        return new LimitExec(ctx, limitNode.getInSchema(),
            limitNode.getOutSchema(), leftExec, limitNode);

      case BST_INDEX_SCAN:
        IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
        leftExec = createIndexScanExec(ctx, indexScanNode);
        return leftExec;

      default:
        return null;
    }
  }

  @VisibleForTesting
  public long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) throws IOException {
    long size = 0;
    for (String tableId : tableIds) {
      // TODO - CSV is a hack.
      List<FileFragment> fragments = FragmentConvertor.convert(ctx.getConf(), CatalogProtos.StoreType.CSV,
          ctx.getTables(tableId));
      for (FileFragment frag : fragments) {
        size += frag.getEndKey();
      }
    }
    return size;
  }

  @VisibleForTesting
  public boolean checkIfInMemoryInnerJoinIsPossible(TaskAttemptContext context, LogicalNode node, boolean left)
      throws IOException {
    String [] lineage = PlannerUtil.getRelationLineage(node);
    long volume = estimateSizeRecursive(context, lineage);
    boolean inMemoryInnerJoinFlag = volume <= INNER_JOIN_INMEMORY_HASH_THRESHOLD;
    LOG.info(String.format("[%s] the volume of %s relations (%s) is %s and is %sfit to main maemory.",
        context.getTaskId().toString(),
        (left ? "Left" : "Right"),
        TUtil.arrayToString(lineage),
        FileUtil.humanReadableByteCount(volume, false),
        (inMemoryInnerJoinFlag ? "" : "not ")));
    return inMemoryInnerJoinFlag;
  }

  public PhysicalExec createJoinPlan(TaskAttemptContext context, JoinNode joinNode, PhysicalExec leftExec,
                                     PhysicalExec rightExec) throws IOException {

    switch (joinNode.getJoinType()) {
      case CROSS:
        return createCrossJoinPlan(context, joinNode, leftExec, rightExec);

      case INNER:
        return createInnerJoinPlan(context, joinNode, leftExec, rightExec);

      case LEFT_OUTER:
        return createLeftOuterJoinPlan(context, joinNode, leftExec, rightExec);

      case RIGHT_OUTER:
        return createRightOuterJoinPlan(context, joinNode, leftExec, rightExec);

      case FULL_OUTER:
        return createFullOuterJoinPlan(context, joinNode, leftExec, rightExec);

      case LEFT_SEMI:
        return createLeftSemiJoinPlan(context, joinNode, leftExec, rightExec);

      case RIGHT_SEMI:
        return createRightSemiJoinPlan(context, joinNode, leftExec, rightExec);

      case LEFT_ANTI:
        return createLeftAntiJoinPlan(context, joinNode, leftExec, rightExec);

      case RIGHT_ANTI:
        return createRightAntiJoinPlan(context, joinNode, leftExec, rightExec);

      default:
        throw new PhysicalPlanningException("Cannot support join type: " + joinNode.getJoinType().name());
    }
  }

  private PhysicalExec createCrossJoinPlan(TaskAttemptContext context, JoinNode plan,
                                           PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);

    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();

      switch (algorithm) {
        case NESTED_LOOP_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [Nested Loop Join]");
          return new NLJoinExec(context, plan, leftExec, rightExec);
        case BLOCK_NESTED_LOOP_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [Block Nested Loop Join]");
          return new BNLJoinExec(context, plan, leftExec, rightExec);
        default:
          // fallback algorithm
          LOG.error("Invalid Cross Join Algorithm Enforcer: " + algorithm.name());
          return new BNLJoinExec(context, plan, leftExec, rightExec);
      }

    } else {
      return new BNLJoinExec(context, plan, leftExec, rightExec);
    }
  }

  private PhysicalExec createInnerJoinPlan(TaskAttemptContext context, JoinNode plan,
                                           PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);

    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();

      switch (algorithm) {
        case NESTED_LOOP_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [Nested Loop Join]");
          return new NLJoinExec(context, plan, leftExec, rightExec);
        case BLOCK_NESTED_LOOP_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [Block Nested Loop Join]");
          return new BNLJoinExec(context, plan, leftExec, rightExec);
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [In-memory Hash Join]");
          // returns two PhysicalExec. smaller one is 0, and larger one is 1.
          PhysicalExec [] orderedChilds = switchJoinSidesIfNecessary(context, plan, leftExec, rightExec);
          return new HashJoinExec(context, plan, orderedChilds[1], orderedChilds[0]);
        case MERGE_JOIN:
          LOG.info("Join (" + plan.getPID() +") chooses [Sort Merge Join]");
          return createMergeInnerJoin(context, plan, leftExec, rightExec);
        case HYBRID_HASH_JOIN:

        default:
          LOG.error("Invalid Inner Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.MERGE_JOIN.name());
          return createMergeInnerJoin(context, plan, leftExec, rightExec);
      }
    } else {
      return createBestInnerJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  /**
   * It returns two {@link org.apache.tajo.engine.planner.physical.PhysicalExec}s sorted in an ascending order of
   * their child relations' total volume. In other words, the smaller side is returned as 0's PhysicalExec, and
   * the larger side is returned as 1's PhysicalExec.
   */
  @VisibleForTesting
  public PhysicalExec [] switchJoinSidesIfNecessary(TaskAttemptContext context, JoinNode plan,
                                                     PhysicalExec left, PhysicalExec right) throws IOException {
    String [] leftLineage = PlannerUtil.getRelationLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getRelationLineage(plan.getRightChild());
    long leftSize = estimateSizeRecursive(context, leftLineage);
    long rightSize = estimateSizeRecursive(context, rightLineage);

    PhysicalExec smaller;
    PhysicalExec larger;
    if (leftSize <= rightSize) {
      smaller = left;
      larger = right;
      LOG.info(String.format("[%s] Left relations %s (%s) is smaller than Right relations %s (%s).",
          context.getTaskId().toString(),
          TUtil.arrayToString(leftLineage),
          FileUtil.humanReadableByteCount(leftSize, false),
          TUtil.arrayToString(rightLineage),
          FileUtil.humanReadableByteCount(rightSize, false)));
    } else {
      smaller = right;
      larger = left;
      LOG.info(String.format("[%s] Right relations %s (%s) is smaller than Left relations %s (%s).",
          context.getTaskId().toString(),
          TUtil.arrayToString(rightLineage),
          FileUtil.humanReadableByteCount(rightSize, false),
          TUtil.arrayToString(leftLineage),
          FileUtil.humanReadableByteCount(leftSize, false)));
    }

    return new PhysicalExec [] {smaller, larger};
  }

  private PhysicalExec createBestInnerJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    boolean inMemoryHashJoin = false;
    if (checkIfInMemoryInnerJoinIsPossible(context, plan.getLeftChild(), true)
        || checkIfInMemoryInnerJoinIsPossible(context, plan.getRightChild(), false)) {
      inMemoryHashJoin = true;
    }

    if (inMemoryHashJoin) {
      LOG.info("Join (" + plan.getPID() +") chooses [In-memory Hash Join]");
      // returns two PhysicalExec. smaller one is 0, and larger one is 1.
      PhysicalExec [] orderedChilds = switchJoinSidesIfNecessary(context, plan, leftExec, rightExec);
      return new HashJoinExec(context, plan, orderedChilds[1], orderedChilds[0]);
    } else {
      return createMergeInnerJoin(context, plan, leftExec, rightExec);
    }
  }

  private MergeJoinExec createMergeInnerJoin(TaskAttemptContext context, JoinNode plan,
                                             PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
        plan.getJoinQual(), leftExec.getSchema(), rightExec.getSchema());

    SortNode leftSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    leftSortNode.setSortSpecs(sortSpecs[0]);
    leftSortNode.setInSchema(leftExec.getSchema());
    leftSortNode.setOutSchema(leftExec.getSchema());
    ExternalSortExec outerSort = new ExternalSortExec(context, sm, leftSortNode, leftExec);

    SortNode rightSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    rightSortNode.setSortSpecs(sortSpecs[1]);
    rightSortNode.setInSchema(rightExec.getSchema());
    rightSortNode.setOutSchema(rightExec.getSchema());
    ExternalSortExec innerSort = new ExternalSortExec(context, sm, rightSortNode, rightExec);

    LOG.info("Join (" + plan.getPID() +") chooses [Merge Join]");
    return new MergeJoinExec(context, plan, outerSort, innerSort, sortSpecs[0], sortSpecs[1]);
  }

  private PhysicalExec createLeftOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Left Outer Join (" + plan.getPID() +") chooses [Hash Join].");
          return new HashLeftOuterJoinExec(context, plan, leftExec, rightExec);
        case NESTED_LOOP_JOIN:
          //the right operand is too large, so we opt for NL implementation of left outer join
          LOG.info("Left Outer Join (" + plan.getPID() +") chooses [Nested Loop Join].");
          return new NLLeftOuterJoinExec(context, plan, leftExec, rightExec);
        default:
          LOG.error("Invalid Left Outer Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.IN_MEMORY_HASH_JOIN.name());
          return new HashLeftOuterJoinExec(context, plan, leftExec, rightExec);
      }
    } else {
      return createBestLeftOuterJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  private PhysicalExec createBestLeftOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                   PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    String [] rightLineage = PlannerUtil.getRelationLineage(plan.getRightChild());
    long rightTableVolume = estimateSizeRecursive(context, rightLineage);

    if (rightTableVolume < conf.getLongVar(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD)) {
      // we can implement left outer join using hash join, using the right operand as the build relation
      LOG.info("Left Outer Join (" + plan.getPID() +") chooses [Hash Join].");
      return new HashLeftOuterJoinExec(context, plan, leftExec, rightExec);
    }
    else {
      //the right operand is too large, so we opt for NL implementation of left outer join
      LOG.info("Left Outer Join (" + plan.getPID() +") chooses [Nested Loop Join].");
      return new NLLeftOuterJoinExec(context, plan, leftExec, rightExec);
    }
  }

  private PhysicalExec createBestRightJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    //if the left operand is small enough => implement it as a left outer hash join with exchanged operators (note:
    // blocking, but merge join is blocking as well)
    String [] outerLineage4 = PlannerUtil.getRelationLineage(plan.getLeftChild());
    long outerSize = estimateSizeRecursive(context, outerLineage4);
    if (outerSize < conf.getLongVar(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD)){
      LOG.info("Right Outer Join (" + plan.getPID() +") chooses [Hash Join].");
      return new HashLeftOuterJoinExec(context, plan, rightExec, leftExec);
    } else {
      return createRightOuterMergeJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  private PhysicalExec createRightOuterMergeJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                     PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    //the left operand is too large, so opt for merge join implementation
    LOG.info("Right Outer Join (" + plan.getPID() +") chooses [Merge Join].");
    SortSpec[][] sortSpecs2 = PlannerUtil.getSortKeysFromJoinQual(
        plan.getJoinQual(), leftExec.getSchema(), rightExec.getSchema());

    SortNode leftSortNode2 = LogicalPlan.createNodeWithoutPID(SortNode.class);
    leftSortNode2.setSortSpecs(sortSpecs2[0]);
    leftSortNode2.setInSchema(leftExec.getSchema());
    leftSortNode2.setOutSchema(leftExec.getSchema());
    ExternalSortExec outerSort2 = new ExternalSortExec(context, sm, leftSortNode2, leftExec);

    SortNode rightSortNode2 = LogicalPlan.createNodeWithoutPID(SortNode.class);
    rightSortNode2.setSortSpecs(sortSpecs2[1]);
    rightSortNode2.setInSchema(rightExec.getSchema());
    rightSortNode2.setOutSchema(rightExec.getSchema());
    ExternalSortExec innerSort2 = new ExternalSortExec(context, sm, rightSortNode2, rightExec);

    return new RightOuterMergeJoinExec(context, plan, outerSort2, innerSort2, sortSpecs2[0], sortSpecs2[1]);
  }

  private PhysicalExec createRightOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Right Outer Join (" + plan.getPID() +") chooses [Hash Join].");
          return new HashLeftOuterJoinExec(context, plan, rightExec, leftExec);
        case MERGE_JOIN:
          return createRightOuterMergeJoinPlan(context, plan, leftExec, rightExec);
        default:
          LOG.error("Invalid Right Outer Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback merge join algorithm: " + JoinAlgorithm.MERGE_JOIN.name());
          return createRightOuterMergeJoinPlan(context, plan, leftExec, rightExec);
      }
    } else {
      return createBestRightJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  private PhysicalExec createFullOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          return createFullOuterHashJoinPlan(context, plan, leftExec, rightExec);

        case MERGE_JOIN:
          return createFullOuterMergeJoinPlan(context, plan, leftExec, rightExec);

        default:
          LOG.error("Invalid Full Outer Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback merge join algorithm: " + JoinAlgorithm.MERGE_JOIN.name());
          return createFullOuterMergeJoinPlan(context, plan, leftExec, rightExec);
      }
    } else {
      return createBestFullOuterJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  private HashFullOuterJoinExec createFullOuterHashJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                            PhysicalExec leftExec, PhysicalExec rightExec)
      throws IOException {
    String [] leftLineage = PlannerUtil.getRelationLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getRelationLineage(plan.getRightChild());
    long outerSize2 = estimateSizeRecursive(context, leftLineage);
    long innerSize2 = estimateSizeRecursive(context, rightLineage);

    PhysicalExec selectedRight;
    PhysicalExec selectedLeft;

    // HashJoinExec loads the smaller relation to memory.
    if (outerSize2 <= innerSize2) {
      selectedLeft = leftExec;
      selectedRight = rightExec;
    } else {
      selectedLeft = rightExec;
      selectedRight = leftExec;
    }
    LOG.info("Full Outer Join (" + plan.getPID() +") chooses [Hash Join]");
    return new HashFullOuterJoinExec(context, plan, selectedRight, selectedLeft);
  }

  private MergeFullOuterJoinExec createFullOuterMergeJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                              PhysicalExec leftExec, PhysicalExec rightExec)
      throws IOException {
    // if size too large, full outer merge join implementation
    LOG.info("Full Outer Join (" + plan.getPID() +") chooses [Merge Join]");
    SortSpec[][] sortSpecs3 = PlannerUtil.getSortKeysFromJoinQual(plan.getJoinQual(),
        leftExec.getSchema(), rightExec.getSchema());

    SortNode leftSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    leftSortNode.setSortSpecs(sortSpecs3[0]);
    leftSortNode.setInSchema(leftExec.getSchema());
    leftSortNode.setOutSchema(leftExec.getSchema());
    ExternalSortExec outerSort3 = new ExternalSortExec(context, sm, leftSortNode, leftExec);

    SortNode rightSortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    rightSortNode.setSortSpecs(sortSpecs3[1]);
    rightSortNode.setInSchema(rightExec.getSchema());
    rightSortNode.setOutSchema(rightExec.getSchema());
    ExternalSortExec innerSort3 = new ExternalSortExec(context, sm, rightSortNode, rightExec);

    return new MergeFullOuterJoinExec(context, plan, outerSort3, innerSort3, sortSpecs3[0], sortSpecs3[1]);
  }

  private PhysicalExec createBestFullOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                   PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    String [] leftLineage = PlannerUtil.getRelationLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getRelationLineage(plan.getRightChild());
    long outerSize2 = estimateSizeRecursive(context, leftLineage);
    long innerSize2 = estimateSizeRecursive(context, rightLineage);
    final long threshold = 1048576 * 128;
    if (outerSize2 < threshold || innerSize2 < threshold) {
      return createFullOuterHashJoinPlan(context, plan, leftExec, rightExec);
    } else {
      return createFullOuterMergeJoinPlan(context, plan, leftExec, rightExec);
    }
  }

  /**
   *  Left semi join means that the left side is the IN side table, and the right side is the FROM side table.
   */
  private PhysicalExec createLeftSemiJoinPlan(TaskAttemptContext context, JoinNode plan,
                                              PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
          return new HashLeftSemiJoinExec(context, plan, leftExec, rightExec);

        default:
          LOG.error("Invalid Left Semi Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.IN_MEMORY_HASH_JOIN.name());
          return new HashLeftOuterJoinExec(context, plan, leftExec, rightExec);
      }
    } else {
      LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
      return new HashLeftSemiJoinExec(context, plan, leftExec, rightExec);
    }
  }

  /**
   *  Left semi join means that the left side is the FROM side table, and the right side is the IN side table.
   */
  private PhysicalExec createRightSemiJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
          return new HashLeftSemiJoinExec(context, plan, rightExec, leftExec);

        default:
          LOG.error("Invalid Left Semi Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.IN_MEMORY_HASH_JOIN.name());
          return new HashLeftOuterJoinExec(context, plan, rightExec, leftExec);
      }
    } else {
      LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
      return new HashLeftSemiJoinExec(context, plan, rightExec, leftExec);
    }
  }

  /**
   *  Left semi join means that the left side is the FROM side table, and the right side is the IN side table.
   */
  private PhysicalExec createLeftAntiJoinPlan(TaskAttemptContext context, JoinNode plan,
                                              PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
          return new HashLeftAntiJoinExec(context, plan, leftExec, rightExec);

        default:
          LOG.error("Invalid Left Semi Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.IN_MEMORY_HASH_JOIN.name());
          return new HashLeftAntiJoinExec(context, plan, leftExec, rightExec);
      }
    } else {
      LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
      return new HashLeftAntiJoinExec(context, plan, leftExec, rightExec);
    }
  }

  /**
   *  Left semi join means that the left side is the FROM side table, and the right side is the IN side table.
   */
  private PhysicalExec createRightAntiJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, plan);
    if (property != null) {
      JoinAlgorithm algorithm = property.getJoin().getAlgorithm();
      switch (algorithm) {
        case IN_MEMORY_HASH_JOIN:
          LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
          return new HashLeftSemiJoinExec(context, plan, rightExec, leftExec);

        default:
          LOG.error("Invalid Left Semi Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.IN_MEMORY_HASH_JOIN.name());
          return new HashLeftOuterJoinExec(context, plan, rightExec, leftExec);
      }
    } else {
      LOG.info("Left Semi Join (" + plan.getPID() +") chooses [In Memory Hash Join].");
      return new HashLeftSemiJoinExec(context, plan, rightExec, leftExec);
    }
  }


  /**
   * Create a shuffle file write executor to store intermediate data into local disks.
   */
  public PhysicalExec createShuffleFileWritePlan(TaskAttemptContext ctx,
                                                 ShuffleFileWriteNode plan, PhysicalExec subOp) throws IOException {
    switch (plan.getShuffleType()) {
    case HASH_SHUFFLE:
      return new HashShuffleFileWriteExec(ctx, sm, plan, subOp);

    case RANGE_SHUFFLE:
      SortExec sortExec = PhysicalPlanUtil.findExecutor(subOp, SortExec.class);

      SortSpec [] sortSpecs = null;
      if (sortExec != null) {
        sortSpecs = sortExec.getSortSpecs();
      } else {
        Column[] columns = ctx.getDataChannel().getShuffleKeys();
        SortSpec specs[] = new SortSpec[columns.length];
        for (int i = 0; i < columns.length; i++) {
          specs[i] = new SortSpec(columns[i]);
        }
      }
      return new RangeShuffleFileWriteExec(ctx, sm, subOp, plan.getInSchema(), plan.getInSchema(), sortSpecs);

    case NONE_SHUFFLE:
      return new StoreTableExec(ctx, plan, subOp);

    default:
      throw new IllegalStateException(ctx.getDataChannel().getShuffleType() + " is not supported yet.");
    }
  }

  /**
   * Create a executor to store a table into HDFS. This is used for CREATE TABLE ..
   * AS or INSERT (OVERWRITE) INTO statement.
   */
  public PhysicalExec createStorePlan(TaskAttemptContext ctx,
                                      StoreTableNode plan, PhysicalExec subOp) throws IOException {

    if (plan.getPartitionMethod() != null) {
      switch (plan.getPartitionMethod().getPartitionType()) {
      case COLUMN:
        return createColumnPartitionStorePlan(ctx, plan, subOp);
      default:
        throw new IllegalStateException(plan.getPartitionMethod().getPartitionType() + " is not supported yet.");
      }
    } else {
      return new StoreTableExec(ctx, plan, subOp);
    }
  }

  private PhysicalExec createColumnPartitionStorePlan(TaskAttemptContext context,
                                                      StoreTableNode storeTableNode,
                                                      PhysicalExec child) throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, storeTableNode);
    if (property != null) {
      ColumnPartitionAlgorithm algorithm = property.getColumnPartition().getAlgorithm();
      switch (algorithm) {
      case HASH_PARTITION:
        return createHashColumnPartitionStorePlan(context, storeTableNode, child);
      case SORT_PARTITION: // default algorithm
      default:
        return createSortBasedColumnPartitionStorePlan(context, storeTableNode, child);
      }
    } else { // default algorithm is sorted-based column partition
      return createSortBasedColumnPartitionStorePlan(context, storeTableNode, child);
    }
  }

  private PhysicalExec createHashColumnPartitionStorePlan(TaskAttemptContext context,
                                                          StoreTableNode storeTableNode,
                                                          PhysicalExec child) throws IOException {
    LOG.info("The planner chooses [Hash-based Column Partitioned Store] algorithm");
    return new HashBasedColPartitionStoreExec(context, storeTableNode, child);
  }

  private PhysicalExec createSortBasedColumnPartitionStorePlan(TaskAttemptContext context,
                                                               StoreTableNode storeTableNode,
                                                               PhysicalExec child) throws IOException {

    Column[] partitionKeyColumns = storeTableNode.getPartitionMethod().getExpressionSchema().toArray();
    SortSpec[] sortSpecs = new SortSpec[partitionKeyColumns.length];

    if (storeTableNode.getType() == NodeType.INSERT) {
      InsertNode insertNode = (InsertNode) storeTableNode;
      for (int i = 0; i < partitionKeyColumns.length; i++) {
        for (Column column : partitionKeyColumns) {
          int id = insertNode.getTableSchema().getColumnId(column.getQualifiedName());
          sortSpecs[i++] = new SortSpec(insertNode.getProjectedSchema().getColumn(id), true, false);
        }
      }
    } else {
      for (int i = 0; i < partitionKeyColumns.length; i++) {
        sortSpecs[i] = new SortSpec(partitionKeyColumns[i], true, false);
      }
    }

    SortNode sortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    sortNode.setSortSpecs(sortSpecs);
    sortNode.setInSchema(child.getSchema());
    sortNode.setOutSchema(child.getSchema());

    ExternalSortExec sortExec = new ExternalSortExec(context, sm, sortNode, child);
    LOG.info("The planner chooses [Sort-based Column Partitioned Store] algorithm");
    return new SortBasedColPartitionStoreExec(context, storeTableNode, sortExec);
  }

  private boolean checkIfSortEquivalance(TaskAttemptContext ctx, ScanNode scanNode, Stack<LogicalNode> node) {
    Enforcer enforcer = ctx.getEnforcer();
    List<EnforceProperty> property = enforcer.getEnforceProperties(EnforceType.SORTED_INPUT);
    if (property != null && property.size() > 0 && node.peek().getType() == NodeType.SORT) {
      SortNode sortNode = (SortNode) node.peek();
      TajoWorkerProtocol.SortedInputEnforce sortEnforcer = property.get(0).getSortedInput();

      boolean condition = scanNode.getTableName().equals(sortEnforcer.getTableName());
      SortSpec [] sortSpecs = PlannerUtil.convertSortSpecs(sortEnforcer.getSortSpecsList());
      return condition && TUtil.checkEquals(sortNode.getSortKeys(), sortSpecs);
    } else {
      return false;
    }
  }

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode, Stack<LogicalNode> node)
      throws IOException {
    // check if an input is sorted in the same order to the subsequence sort operator.
    // TODO - it works only if input files are raw files. We should check the file format.
    // Since the default intermediate file format is raw file, it is not problem right now.
    if (checkIfSortEquivalance(ctx, scanNode, node)) {
      if (ctx.getTable(scanNode.getCanonicalName()) == null) {
        return new SeqScanExec(ctx, sm, scanNode, null);
      }
      FragmentProto [] fragments = ctx.getTables(scanNode.getCanonicalName());
      return new ExternalSortExec(ctx, sm, (SortNode) node.peek(), fragments);
    } else {
      Enforcer enforcer = ctx.getEnforcer();

      // check if this table is broadcasted one or not.
      boolean broadcastFlag = false;
      if (enforcer != null && enforcer.hasEnforceProperty(EnforceType.BROADCAST)) {
        List<EnforceProperty> properties = enforcer.getEnforceProperties(EnforceType.BROADCAST);
        for (EnforceProperty property : properties) {
          broadcastFlag |= scanNode.getCanonicalName().equals(property.getBroadcast().getTableName());
        }
      }

      if (scanNode instanceof PartitionedTableScanNode
          && ((PartitionedTableScanNode)scanNode).getInputPaths() != null &&
          ((PartitionedTableScanNode)scanNode).getInputPaths().length > 0) {

        if (scanNode instanceof PartitionedTableScanNode) {
          if (broadcastFlag) {
            PartitionedTableScanNode partitionedTableScanNode = (PartitionedTableScanNode) scanNode;
            List<FileFragment> fileFragments = TUtil.newList();
            for (Path path : partitionedTableScanNode.getInputPaths()) {
              fileFragments.addAll(TUtil.newList(sm.split(scanNode.getCanonicalName(), path)));
            }

            return new PartitionMergeScanExec(ctx, sm, scanNode,
                FragmentConvertor.toFragmentProtoArray(fileFragments.toArray(new FileFragment[fileFragments.size()])));
          }
        }
      }

      if (ctx.getTable(scanNode.getCanonicalName()) == null) {
        return new SeqScanExec(ctx, sm, scanNode, null);
      }
      FragmentProto [] fragments = ctx.getTables(scanNode.getCanonicalName());
      return new SeqScanExec(ctx, sm, scanNode, fragments);
    }
  }

  public PhysicalExec createGroupByPlan(TaskAttemptContext context,GroupbyNode groupbyNode, PhysicalExec subOp)
      throws IOException {

    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, groupbyNode);
    if (property != null) {
      GroupbyAlgorithm algorithm = property.getGroupby().getAlgorithm();
      if (algorithm == GroupbyAlgorithm.HASH_AGGREGATION) {
        return createInMemoryHashAggregation(context, groupbyNode, subOp);
      } else {
        return createSortAggregation(context, property, groupbyNode, subOp);
      }
    }
    return createBestAggregationPlan(context, groupbyNode, subOp);
  }

  private PhysicalExec createInMemoryHashAggregation(TaskAttemptContext ctx,GroupbyNode groupbyNode, PhysicalExec subOp)
      throws IOException {
    LOG.info("The planner chooses [Hash Aggregation]");
    return new HashAggregateExec(ctx, groupbyNode, subOp);
  }

  private PhysicalExec createSortAggregation(TaskAttemptContext ctx, EnforceProperty property, GroupbyNode groupbyNode,
                                             PhysicalExec subOp) throws IOException {

    Column[] grpColumns = groupbyNode.getGroupingColumns();
    SortSpec[] sortSpecs = new SortSpec[grpColumns.length];
    for (int i = 0; i < grpColumns.length; i++) {
      sortSpecs[i] = new SortSpec(grpColumns[i], true, false);
    }

    if (property != null) {
      List<CatalogProtos.SortSpecProto> sortSpecProtos = property.getGroupby().getSortSpecsList();

      List<SortSpec> enforcedSortSpecList = Lists.newArrayList();
      int i = 0;
      outer:
      for (int j = 0; j < sortSpecProtos.size(); j++) {
        SortSpec enforcedSortSpecs = new SortSpec(sortSpecProtos.get(j));

        for (Column grpKey : grpColumns) { // if this sort key is included in grouping columns, skip it.
          if (enforcedSortSpecs.getSortKey().equals(grpKey)) {
            continue outer;
          }
        }

        enforcedSortSpecList.add(enforcedSortSpecs);
      }

      sortSpecs = ObjectArrays.concat(sortSpecs, TUtil.toArray(enforcedSortSpecList, SortSpec.class), SortSpec.class);
    }

    SortNode sortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
    sortNode.setSortSpecs(sortSpecs);
    sortNode.setInSchema(subOp.getSchema());
    sortNode.setOutSchema(subOp.getSchema());
    ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode, subOp);
    LOG.info("The planner chooses [Sort Aggregation] in (" + TUtil.arrayToString(sortSpecs) + ")");
    return new SortAggregateExec(ctx, groupbyNode, sortExec);
  }

  private PhysicalExec createBestAggregationPlan(TaskAttemptContext context, GroupbyNode groupbyNode,
                                                 PhysicalExec subOp) throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    if (grpColumns.length == 0) {
      return createInMemoryHashAggregation(context, groupbyNode, subOp);
    }

    String [] outerLineage = PlannerUtil.getRelationLineage(groupbyNode.getChild());
    long estimatedSize = estimateSizeRecursive(context, outerLineage);
    final long threshold = conf.getLongVar(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD);

    // if the relation size is less than the threshold,
    // the hash aggregation will be used.
    if (estimatedSize <= threshold) {
      LOG.info("The planner chooses [Hash Aggregation]");
      return createInMemoryHashAggregation(context, groupbyNode, subOp);
    } else {
      return createSortAggregation(context, null, groupbyNode, subOp);
    }
  }

  public PhysicalExec createWindowAgg(TaskAttemptContext context,WindowAggNode windowAggNode, PhysicalExec subOp)
      throws IOException {
    PhysicalExec child = subOp;
    if (windowAggNode.hasPartitionKeys()) {
      Column[] grpColumns = windowAggNode.getPartitionKeys();
      SortSpec[] sortSpecs = new SortSpec[grpColumns.length];
      for (int i = 0; i < grpColumns.length; i++) {
        sortSpecs[i] = new SortSpec(grpColumns[i], true, false);
      }

      SortNode sortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      sortNode.setSortSpecs(sortSpecs);
      sortNode.setInSchema(subOp.getSchema());
      sortNode.setOutSchema(subOp.getSchema());
      child = new ExternalSortExec(context, sm, sortNode, subOp);
      LOG.info("The planner chooses [Sort Aggregation] in (" + TUtil.arrayToString(sortSpecs) + ")");
    }

    return new WindowAggExec(context, windowAggNode, child);
  }

  public PhysicalExec createDistinctGroupByPlan(TaskAttemptContext context,
                                                DistinctGroupbyNode distinctNode, PhysicalExec subOp)
      throws IOException {
    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, distinctNode);
    if (property != null) {
      DistinctAggregationAlgorithm algorithm = property.getDistinct().getAlgorithm();
      if (algorithm == DistinctAggregationAlgorithm.HASH_AGGREGATION) {
        return createInMemoryDistinctGroupbyExec(context, distinctNode, subOp);
      } else {
        return createSortAggregationDistinctGroupbyExec(context, distinctNode, subOp, property.getDistinct());
      }
    } else {
      return createInMemoryDistinctGroupbyExec(context, distinctNode, subOp);
    }
  }

  private PhysicalExec createInMemoryDistinctGroupbyExec(TaskAttemptContext ctx,
      DistinctGroupbyNode distinctGroupbyNode, PhysicalExec subOp) throws IOException {
    return new DistinctGroupbyHashAggregationExec(ctx, distinctGroupbyNode, subOp);
  }

  private PhysicalExec createSortAggregationDistinctGroupbyExec(TaskAttemptContext ctx,
      DistinctGroupbyNode distinctGroupbyNode, PhysicalExec subOp,
      DistinctGroupbyEnforcer enforcer) throws IOException {
    List<GroupbyNode> groupbyNodes = distinctGroupbyNode.getGroupByNodes();

    SortAggregateExec[] sortAggregateExec = new SortAggregateExec[groupbyNodes.size()];

    List<SortSpecArray> sortSpecArrays = enforcer.getSortSpecArraysList();

    int index = 0;
    for (GroupbyNode eachGroupbyNode: groupbyNodes) {
      SortSpecArray sortSpecArray = sortSpecArrays.get(index);
      SortSpec[] sortSpecs = new SortSpec[sortSpecArray.getSortSpecsList().size()];
      int sortIndex = 0;
      for (SortSpecProto eachProto: sortSpecArray.getSortSpecsList()) {
        sortSpecs[sortIndex++] = new SortSpec(eachProto);
      }
      SortNode sortNode = LogicalPlan.createNodeWithoutPID(SortNode.class);
      sortNode.setSortSpecs(sortSpecs);
      sortNode.setInSchema(subOp.getSchema());
      sortNode.setOutSchema(eachGroupbyNode.getInSchema());
      ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode, subOp);

      sortAggregateExec[index++] = new SortAggregateExec(ctx, eachGroupbyNode, sortExec);
    }

    return new DistinctGroupbySortAggregationExec(ctx, distinctGroupbyNode, sortAggregateExec);
  }

  public PhysicalExec createSortPlan(TaskAttemptContext context, SortNode sortNode,
                                     PhysicalExec child) throws IOException {

    // check if it is a distributed merge sort
    // If so, it does need to create a sort executor because
    // the sort executor is created at the scan planning
    if (child instanceof SortExec) {
      SortExec childSortExec = (SortExec) child;
      if (TUtil.checkEquals(sortNode.getSortKeys(), childSortExec.getSortSpecs())) {
        return child;
      }
    }

    Enforcer enforcer = context.getEnforcer();
    EnforceProperty property = getAlgorithmEnforceProperty(enforcer, sortNode);
    if (property != null) {
      SortEnforce.SortAlgorithm algorithm = property.getSort().getAlgorithm();
      if (algorithm == SortEnforce.SortAlgorithm.IN_MEMORY_SORT) {
        return new MemSortExec(context, sortNode, child);
      } else {
        return new ExternalSortExec(context, sm, sortNode, child);
      }
    }

    return createBestSortPlan(context, sortNode, child);
  }

  public SortExec createBestSortPlan(TaskAttemptContext context, SortNode sortNode,
                                     PhysicalExec child) throws IOException {
    return new ExternalSortExec(context, sm, sortNode, child);
  }

  public PhysicalExec createIndexScanExec(TaskAttemptContext ctx,
                                          IndexScanNode annotation)
      throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getCanonicalName()),
        "Error: There is no table matched to %s", annotation.getCanonicalName());

    FragmentProto [] fragmentProtos = ctx.getTables(annotation.getTableName());
    List<FileFragment> fragments =
        FragmentConvertor.convert(ctx.getConf(), ctx.getDataChannel().getStoreType(), fragmentProtos);

    String indexName = IndexUtil.getIndexNameOfFrag(fragments.get(0), annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableName()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(ctx, sm, annotation, fragments.get(0), new Path(indexPath, indexName),
        annotation.getKeySchema(), comp, annotation.getDatum());

  }

  private EnforceProperty getAlgorithmEnforceProperty(Enforcer enforcer, LogicalNode node) {
    if (enforcer == null) {
      return null;
    }

    EnforceType type;
    if (node.getType() == NodeType.JOIN) {
      type = EnforceType.JOIN;
    } else if (node.getType() == NodeType.GROUP_BY) {
      type = EnforceType.GROUP_BY;
    } else if (node.getType() == NodeType.DISTINCT_GROUP_BY) {
      type = EnforceType.DISTINCT_GROUP_BY;
    } else if (node.getType() == NodeType.SORT) {
      type = EnforceType.SORT;
    } else if (node instanceof StoreTableNode
        && ((StoreTableNode)node).hasPartition()
        && ((StoreTableNode)node).getPartitionMethod().getPartitionType() == PartitionType.COLUMN) {
      type = EnforceType.COLUMN_PARTITION;
    } else {
      return null;
    }

    if (enforcer.hasEnforceProperty(type)) {
      List<EnforceProperty> properties = enforcer.getEnforceProperties(type);
      EnforceProperty found = null;
      for (EnforceProperty property : properties) {
        if (type == EnforceType.JOIN && property.getJoin().getPid() == node.getPID()) {
          found = property;
        } else if (type == EnforceType.GROUP_BY && property.getGroupby().getPid() == node.getPID()) {
          found = property;
        } else if (type == EnforceType.DISTINCT_GROUP_BY && property.getDistinct().getPid() == node.getPID()) {
          found = property;
        } else if (type == EnforceType.SORT && property.getSort().getPid() == node.getPID()) {
          found = property;
        } else if (type == EnforceType.COLUMN_PARTITION && property.getColumnPartition().getPid() == node.getPID()) {
          found = property;
        }
      }
      return found;
    } else {
      return null;
    }
  }
}
