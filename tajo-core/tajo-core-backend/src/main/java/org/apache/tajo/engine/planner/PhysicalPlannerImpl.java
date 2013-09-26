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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.DataChannel;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.IndexUtil;

import java.io.IOException;
import java.util.List;

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

  private long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) {
    long size = 0;
    for (String tableId : tableIds) {
      Fragment[] fragments = ctx.getTables(tableId);
      for (Fragment frag : fragments) {
        size += frag.getLength();
      }
    }
    return size;
  }

  public PhysicalExec createJoinPlan(TaskAttemptContext context, JoinNode joinNode, PhysicalExec leftExec,
                                     PhysicalExec rightExec) throws IOException {

    switch (joinNode.getJoinType()) {
      case CROSS:
        LOG.info("The planner chooses [Nested Loop Join]");
        return createCrossJoinPlan(context, joinNode, leftExec, rightExec);

      case INNER:
        return createInnerJoinPlan(context, joinNode, leftExec, rightExec);

      case FULL_OUTER:
      case LEFT_OUTER:
      case RIGHT_OUTER:

      case LEFT_SEMI:
      case RIGHT_SEMI:

      case LEFT_ANTI:
      case RIGHT_ANTI:

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
          return createMergeJoin(context, plan, leftExec, rightExec);
        case HYBRID_HASH_JOIN:

        default:
          LOG.error("Invalid Inner Join Algorithm Enforcer: " + algorithm.name());
          LOG.error("Choose a fallback inner join algorithm: " + JoinAlgorithm.MERGE_JOIN.name());
          return createMergeJoin(context, plan, leftExec, rightExec);
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

    final long threshold = conf.getLongVar(TajoConf.ConfVars.INMEMORY_HASH_JOIN_THRESHOLD);

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
      return createMergeJoin(context, plan, leftExec, rightExec);
    }
  }

  private MergeJoinExec createMergeJoin(TaskAttemptContext context, JoinNode plan,
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

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getCanonicalName()),
        "Error: There is no table matched to %s", scanNode.getCanonicalName() + "(" + scanNode.getTableName() + ")");

    Fragment[] fragments = ctx.getTables(scanNode.getCanonicalName());
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
        return createSortAggregation(context, groupbyNode, subOp);
      }
    }
    return createBestAggregationPlan(context, groupbyNode, subOp);
  }

  private PhysicalExec createInMemoryHashAggregation(TaskAttemptContext ctx,GroupbyNode groupbyNode, PhysicalExec subOp)
      throws IOException {
    LOG.info("The planner chooses [Hash Aggregation]");
    return new HashAggregateExec(ctx, groupbyNode, subOp);
  }

  private PhysicalExec createSortAggregation(TaskAttemptContext ctx,GroupbyNode groupbyNode, PhysicalExec subOp)
      throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    SortSpec[] specs = new SortSpec[grpColumns.length];
    for (int i = 0; i < grpColumns.length; i++) {
      specs[i] = new SortSpec(grpColumns[i], true, false);
    }
    SortNode sortNode = new SortNode(-1, specs);
    sortNode.setInSchema(subOp.getSchema());
    sortNode.setOutSchema(subOp.getSchema());
    // SortExec sortExec = new SortExec(sortNode, child);
    ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode, subOp);
    LOG.info("The planner chooses [Sort Aggregation]");
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
    final long threshold = conf.getLongVar(TajoConf.ConfVars.INMEMORY_HASH_AGGREGATION_THRESHOLD);

    // if the relation size is less than the threshold,
    // the hash aggregation will be used.
    if (estimatedSize <= threshold) {
      LOG.info("The planner chooses [Hash Aggregation]");
      return createInMemoryHashAggregation(context, groupbyNode, subOp);
    } else {
      return createSortAggregation(context, groupbyNode, subOp);
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

    Fragment[] fragments = ctx.getTables(annotation.getTableName());

    String indexName = IndexUtil.getIndexNameOfFrag(fragments[0],
        annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableName()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(ctx, sm, annotation, fragments[0], new Path(
        indexPath, indexName), annotation.getKeySchema(), comp,
        annotation.getDatum());

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
