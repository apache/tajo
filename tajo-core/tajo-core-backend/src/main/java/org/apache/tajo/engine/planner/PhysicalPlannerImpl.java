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

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.IndexUtil;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty.EnforceType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.GroupbyEnforce.GroupbyAlgorithm;
import static org.apache.tajo.ipc.TajoWorkerProtocol.JoinEnforce.JoinAlgorithm;

public class PhysicalPlannerImpl implements PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlannerImpl.class);
  private static final int UNGENERATED_PID = -1;

  protected final TajoConf conf;
  protected final AbstractStorageManager sm;

  public PhysicalPlannerImpl(final TajoConf conf, final AbstractStorageManager sm) {
    this.conf = conf;
    this.sm = sm;
  }

  public PhysicalExec createPlan(final TaskAttemptContext context, final LogicalNode logicalPlan)
      throws InternalException {

    PhysicalExec execPlan;

    try {
      execPlan = createPlanRecursive(context, logicalPlan);
      if (execPlan instanceof StoreTableExec
          || execPlan instanceof IndexedStoreExec
          || execPlan instanceof PartitionedStoreExec) {
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
    StoreTableNode storeTableNode = new StoreTableNode(UNGENERATED_PID, channel.getTargetId().toString());
    storeTableNode.setStorageType(CatalogProtos.StoreType.CSV);
    storeTableNode.setInSchema(plan.getOutSchema());
    storeTableNode.setOutSchema(plan.getOutSchema());
    if (channel.getPartitionType() != PartitionType.NONE_PARTITION) {
      storeTableNode.setPartitions(channel.getPartitionType(), channel.getPartitionKey(), channel.getPartitionNum());
    } else {
      storeTableNode.setDefaultParition();
    }
    storeTableNode.setChild(plan);

    PhysicalExec outExecPlan = createStorePlan(context, storeTableNode, execPlan);
    return outExecPlan;
  }

  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode) throws IOException {
    PhysicalExec leftExec;
    PhysicalExec rightExec;

    switch (logicalNode.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
        return createPlanRecursive(ctx, rootNode.getChild());

      case EXPRS:
        EvalExprNode evalExpr = (EvalExprNode) logicalNode;
        return new EvalExprExec(ctx, evalExpr);

      case STORE:
        StoreTableNode storeNode = (StoreTableNode) logicalNode;
        leftExec = createPlanRecursive(ctx, storeNode.getChild());
        return createStorePlan(ctx, storeNode, leftExec);

      case SELECTION:
        SelectionNode selNode = (SelectionNode) logicalNode;
        leftExec = createPlanRecursive(ctx, selNode.getChild());
        return new SelectionExec(ctx, selNode, leftExec);

      case PROJECTION:
        ProjectionNode prjNode = (ProjectionNode) logicalNode;
        leftExec = createPlanRecursive(ctx, prjNode.getChild());
        return new ProjectionExec(ctx, prjNode, leftExec);

      case TABLE_SUBQUERY: {
        TableSubQueryNode subQueryNode = (TableSubQueryNode) logicalNode;
        leftExec = createPlanRecursive(ctx, subQueryNode.getSubQuery());
        return leftExec;

      } case SCAN:
        leftExec = createScanPlan(ctx, (ScanNode) logicalNode);
        return leftExec;

      case GROUP_BY:
        GroupbyNode grpNode = (GroupbyNode) logicalNode;
        leftExec = createPlanRecursive(ctx, grpNode.getChild());
        return createGroupByPlan(ctx, grpNode, leftExec);

      case SORT:
        SortNode sortNode = (SortNode) logicalNode;
        leftExec = createPlanRecursive(ctx, sortNode.getChild());
        return createSortPlan(ctx, sortNode, leftExec);

      case JOIN:
        JoinNode joinNode = (JoinNode) logicalNode;
        leftExec = createPlanRecursive(ctx, joinNode.getLeftChild());
        rightExec = createPlanRecursive(ctx, joinNode.getRightChild());
        return createJoinPlan(ctx, joinNode, leftExec, rightExec);

      case UNION:
        UnionNode unionNode = (UnionNode) logicalNode;
        leftExec = createPlanRecursive(ctx, unionNode.getLeftChild());
        rightExec = createPlanRecursive(ctx, unionNode.getRightChild());
        return new UnionExec(ctx, leftExec, rightExec);

      case LIMIT:
        LimitNode limitNode = (LimitNode) logicalNode;
        leftExec = createPlanRecursive(ctx, limitNode.getChild());
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

  private long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) throws IOException {
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
          return new HashJoinExec(context, plan, leftExec, rightExec);
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

  private PhysicalExec createBestInnerJoinPlan(TaskAttemptContext context, JoinNode plan,
                                               PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    String [] leftLineage = PlannerUtil.getLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getLineage(plan.getRightChild());
    long leftSize = estimateSizeRecursive(context, leftLineage);
    long rightSize = estimateSizeRecursive(context, rightLineage);

    final long threshold = conf.getLongVar(TajoConf.ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD);

    boolean hashJoin = false;
    if (leftSize < threshold || rightSize < threshold) {
      hashJoin = true;
    }

    if (hashJoin) {
      PhysicalExec selectedOuter;
      PhysicalExec selectedInner;

      // HashJoinExec loads the inner relation to memory.
      if (leftSize <= rightSize) {
        selectedInner = leftExec;
        selectedOuter = rightExec;
      } else {
        selectedInner = rightExec;
        selectedOuter = leftExec;
      }

      LOG.info("Join (" + plan.getPID() +") chooses [InMemory Hash Join]");
      return new HashJoinExec(context, plan, selectedOuter, selectedInner);
    } else {
      return createMergeInnerJoin(context, plan, leftExec, rightExec);
    }
  }

  private MergeJoinExec createMergeInnerJoin(TaskAttemptContext context, JoinNode plan,
                                             PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
        plan.getJoinQual(), leftExec.getSchema(), rightExec.getSchema());
    ExternalSortExec outerSort = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID, sortSpecs[0], leftExec.getSchema(), leftExec.getSchema()),
        leftExec);
    ExternalSortExec innerSort = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID, sortSpecs[1], rightExec.getSchema(), rightExec.getSchema()),
        rightExec);

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
    String [] rightLineage = PlannerUtil.getLineage(plan.getRightChild());
    long rightTableVolume = estimateSizeRecursive(context, rightLineage);

    if (rightTableVolume < conf.getLongVar(TajoConf.ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD)) {
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
    String [] outerLineage4 = PlannerUtil.getLineage(plan.getLeftChild());
    long outerSize = estimateSizeRecursive(context, outerLineage4);
    if (outerSize < conf.getLongVar(TajoConf.ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD)){
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
    ExternalSortExec outerSort2 = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID,sortSpecs2[0], leftExec.getSchema(), leftExec.getSchema()), leftExec);
    ExternalSortExec innerSort2 = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID,sortSpecs2[1], rightExec.getSchema(), rightExec.getSchema()), rightExec);
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
    String [] leftLineage = PlannerUtil.getLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getLineage(plan.getRightChild());
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
    ExternalSortExec outerSort3 = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID,sortSpecs3[0], leftExec.getSchema(), leftExec.getSchema()), leftExec);
    ExternalSortExec innerSort3 = new ExternalSortExec(context, sm,
        new SortNode(UNGENERATED_PID,sortSpecs3[1], rightExec.getSchema(), rightExec.getSchema()), rightExec);

    return new MergeFullOuterJoinExec(context, plan, outerSort3, innerSort3, sortSpecs3[0], sortSpecs3[1]);
  }

  private PhysicalExec createBestFullOuterJoinPlan(TaskAttemptContext context, JoinNode plan,
                                                   PhysicalExec leftExec, PhysicalExec rightExec) throws IOException {
    String [] leftLineage = PlannerUtil.getLineage(plan.getLeftChild());
    String [] rightLineage = PlannerUtil.getLineage(plan.getRightChild());
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

  public PhysicalExec createStorePlan(TaskAttemptContext ctx,
                                      StoreTableNode plan, PhysicalExec subOp) throws IOException {
    if (plan.getPartitionType() == PartitionType.HASH_PARTITION
        || plan.getPartitionType() == PartitionType.RANGE_PARTITION) {
      switch (ctx.getDataChannel().getPartitionType()) {
        case HASH_PARTITION:
          return new PartitionedStoreExec(ctx, sm, plan, subOp);

        case RANGE_PARTITION:
          SortExec sortExec = PhysicalPlanUtil.findExecutor(subOp, SortExec.class);

          SortSpec [] sortSpecs = null;
          if (sortExec != null) {
            sortSpecs = sortExec.getSortSpecs();
          } else {
            Column[] columns = ctx.getDataChannel().getPartitionKey();
            SortSpec specs[] = new SortSpec[columns.length];
            for (int i = 0; i < columns.length; i++) {
              specs[i] = new SortSpec(columns[i]);
            }
          }

          return new IndexedStoreExec(ctx, sm, subOp,
              plan.getInSchema(), plan.getInSchema(), sortSpecs);
      }
    }
    if (plan instanceof StoreIndexNode) {
      return new TunnelExec(ctx, plan.getOutSchema(), subOp);
    }

    return new StoreTableExec(ctx, plan, subOp);
  }

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode) throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getCanonicalName()),
        "Error: There is no table matched to %s", scanNode.getCanonicalName() + "(" + scanNode.getTableName() + ")");

    FragmentProto [] fragments = ctx.getTables(scanNode.getCanonicalName());
    return new SeqScanExec(ctx, sm, scanNode, fragments);
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

  private PhysicalExec createSortAggregation(TaskAttemptContext ctx, EnforceProperty property, GroupbyNode groupbyNode, PhysicalExec subOp)
      throws IOException {

    Column[] grpColumns = groupbyNode.getGroupingColumns();
    SortSpec[] sortSpecs = new SortSpec[grpColumns.length];
    for (int i = 0; i < grpColumns.length; i++) {
      sortSpecs[i] = new SortSpec(grpColumns[i], true, false);
    }

    if (property != null) {
      List<CatalogProtos.SortSpecProto> sortSpecProtos = property.getGroupby().getSortSpecsList();
      SortSpec[] enforcedSortSpecs = new SortSpec[sortSpecProtos.size()];
      int i = 0;

      for (int j = 0; j < sortSpecProtos.size(); i++, j++) {
        enforcedSortSpecs[i] = new SortSpec(sortSpecProtos.get(j));
      }

      sortSpecs = ObjectArrays.concat(sortSpecs, enforcedSortSpecs, SortSpec.class);
    }

    SortNode sortNode = new SortNode(-1, sortSpecs);
    sortNode.setInSchema(subOp.getSchema());
    sortNode.setOutSchema(subOp.getSchema());
    // SortExec sortExec = new SortExec(sortNode, child);
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

    String [] outerLineage = PlannerUtil.getLineage(groupbyNode.getChild());
    long estimatedSize = estimateSizeRecursive(context, outerLineage);
    final long threshold = conf.getLongVar(TajoConf.ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD);

    // if the relation size is less than the threshold,
    // the hash aggregation will be used.
    if (estimatedSize <= threshold) {
      LOG.info("The planner chooses [Hash Aggregation]");
      return createInMemoryHashAggregation(context, groupbyNode, subOp);
    } else {
      return createSortAggregation(context, null, groupbyNode, subOp);
    }
  }

  public PhysicalExec createSortPlan(TaskAttemptContext context, SortNode sortNode,
                                     PhysicalExec child) throws IOException {
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
    String [] outerLineage = PlannerUtil.getLineage(sortNode.getChild());
    long estimatedSize = estimateSizeRecursive(context, outerLineage);
    final long threshold = 1048576 * 2000;

    // if the relation size is less than the reshold,
    // the in-memory sort will be used.
    if (estimatedSize <= threshold) {
      return new MemSortExec(context, sortNode, child);
    } else {
      return new ExternalSortExec(context, sm, sortNode, child);
    }
  }

  public PhysicalExec createIndexScanExec(TaskAttemptContext ctx,
                                          IndexScanNode annotation)
      throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getCanonicalName()),
        "Error: There is no table matched to %s", annotation.getCanonicalName());

    FragmentProto [] fragmentProtos = ctx.getTables(annotation.getTableName());
    List<FileFragment> fragments =
        FragmentConvertor.convert(ctx.getConf(), CatalogProtos.StoreType.CSV, fragmentProtos);

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
    } else if (node.getType() == NodeType.SORT) {
      type = EnforceType.SORT;
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
        } else if (type == EnforceType.SORT && property.getSort().getPid() == node.getPID()) {
          found = property;
        }
      }
      return found;
    } else {
      return null;
    }
  }
}
