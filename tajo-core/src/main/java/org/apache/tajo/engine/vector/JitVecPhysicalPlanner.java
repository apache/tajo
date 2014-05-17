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

package org.apache.tajo.engine.vector;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Stack;

public class JitVecPhysicalPlanner extends BasicLogicalPlanVisitor<Object, LogicalNode> {
//  private static final Log LOG = LogFactory.getLog(JitVecPhysicalPlanner.class);
//  private static final int UNGENERATED_PID = -1;
//
//  protected final TajoConf conf;
//  protected final AbstractStorageManager sm;
//
//  public JitVecPhysicalPlanner(final TajoConf conf, final AbstractStorageManager sm) {
//    this.conf = conf;
//    this.sm = sm;
//  }
//
//  public PhysicalExec createPlan(final TaskAttemptContext context, final LogicalNode logicalPlan)
//      throws InternalException {
//
//    PhysicalExec execPlan;
//
//    try {
//      execPlan = createPlanRecursive(context, logicalPlan, new Stack<LogicalNode>());
//      if (execPlan instanceof StoreTableExec
//          || execPlan instanceof RangeShuffleFileWriteExec
//          || execPlan instanceof HashShuffleFileWriteExec
//          || execPlan instanceof ColPartitionStoreExec) {
//        return execPlan;
//      } else if (context.getDataChannel() != null) {
//        return buildOutputOperator(context, logicalPlan, execPlan);
//      } else {
//        return execPlan;
//      }
//    } catch (IOException ioe) {
//      throw new InternalException(ioe);
//    }
//  }
//
//  private PhysicalExec buildOutputOperator(TaskAttemptContext context, LogicalNode plan,
//                                           PhysicalExec execPlan) throws IOException {
//    DataChannel channel = context.getDataChannel();
//    ShuffleFileWriteNode shuffleFileWriteNode = LogicalPlan.createNodeWithoutPID(ShuffleFileWriteNode.class);
//    shuffleFileWriteNode.setStorageType(context.getDataChannel().getStoreType());
//    shuffleFileWriteNode.setInSchema(plan.getOutSchema());
//    shuffleFileWriteNode.setOutSchema(plan.getOutSchema());
//    shuffleFileWriteNode.setShuffle(channel.getShuffleType(), channel.getShuffleKeys(), channel.getShuffleOutputNum());
//    shuffleFileWriteNode.setChild(plan);
//
//    PhysicalExec outExecPlan = createShuffleFileWritePlan(context, shuffleFileWriteNode, execPlan);
//    return outExecPlan;
//  }
//
//  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode, Stack<LogicalNode> stack)
//      throws IOException {
//    PhysicalExec leftExec;
//    PhysicalExec rightExec;
//
//    switch (logicalNode.getType()) {
//
//    case ROOT:
//      LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
//      stack.push(rootNode);
//      leftExec = createPlanRecursive(ctx, rootNode.getChild(), stack);
//      stack.pop();
//      return leftExec;
//
//    case EXPRS:
//      EvalExprNode evalExpr = (EvalExprNode) logicalNode;
//      return new EvalExprExec(ctx, evalExpr);
//
//    case CREATE_TABLE:
//    case INSERT:
//    case STORE:
//      StoreTableNode storeNode = (StoreTableNode) logicalNode;
//      stack.push(storeNode);
//      leftExec = createPlanRecursive(ctx, storeNode.getChild(), stack);
//      stack.pop();
//      return createStorePlan(ctx, storeNode, leftExec);
//
//    case SELECTION:
//      SelectionNode selNode = (SelectionNode) logicalNode;
//      stack.push(selNode);
//      leftExec = createPlanRecursive(ctx, selNode.getChild(), stack);
//      stack.pop();
//      return new SelectionExec(ctx, selNode, leftExec);
//
//    case PROJECTION:
//      ProjectionNode prjNode = (ProjectionNode) logicalNode;
//      stack.push(prjNode);
//      leftExec = createPlanRecursive(ctx, prjNode.getChild(), stack);
//      stack.pop();
//      return new ProjectionExec(ctx, prjNode, leftExec);
//
//    case TABLE_SUBQUERY: {
//      TableSubQueryNode subQueryNode = (TableSubQueryNode) logicalNode;
//      stack.push(subQueryNode);
//      leftExec = createPlanRecursive(ctx, subQueryNode.getSubQuery(), stack);
//      stack.pop();
//      return new ProjectionExec(ctx, subQueryNode, leftExec);
//
//    }
//
//    case PARTITIONS_SCAN:
//    case SCAN:
//      leftExec = createScanPlan(ctx, (ScanNode) logicalNode, stack);
//      return leftExec;
//
//    case GROUP_BY:
//      GroupbyNode grpNode = (GroupbyNode) logicalNode;
//      stack.push(grpNode);
//      leftExec = createPlanRecursive(ctx, grpNode.getChild(), stack);
//      stack.pop();
//      return createGroupByPlan(ctx, grpNode, leftExec);
//
//    case HAVING:
//      HavingNode havingNode = (HavingNode) logicalNode;
//      stack.push(havingNode);
//      leftExec = createPlanRecursive(ctx, havingNode.getChild(), stack);
//      stack.pop();
//      return new HavingExec(ctx, havingNode, leftExec);
//
//    case SORT:
//      SortNode sortNode = (SortNode) logicalNode;
//      stack.push(sortNode);
//      leftExec = createPlanRecursive(ctx, sortNode.getChild(), stack);
//      stack.pop();
//      return createSortPlan(ctx, sortNode, leftExec);
//
//    case JOIN:
//      JoinNode joinNode = (JoinNode) logicalNode;
//      stack.push(joinNode);
//      leftExec = createPlanRecursive(ctx, joinNode.getLeftChild(), stack);
//      rightExec = createPlanRecursive(ctx, joinNode.getRightChild(), stack);
//      stack.pop();
//      return createJoinPlan(ctx, joinNode, leftExec, rightExec);
//
//    case UNION:
//      UnionNode unionNode = (UnionNode) logicalNode;
//      stack.push(unionNode);
//      leftExec = createPlanRecursive(ctx, unionNode.getLeftChild(), stack);
//      rightExec = createPlanRecursive(ctx, unionNode.getRightChild(), stack);
//      stack.pop();
//      return new UnionExec(ctx, leftExec, rightExec);
//
//    case LIMIT:
//      LimitNode limitNode = (LimitNode) logicalNode;
//      stack.push(limitNode);
//      leftExec = createPlanRecursive(ctx, limitNode.getChild(), stack);
//      stack.pop();
//      return new LimitExec(ctx, limitNode.getInSchema(),
//          limitNode.getOutSchema(), leftExec, limitNode);
//
//    case BST_INDEX_SCAN:
//      IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
//      leftExec = createIndexScanExec(ctx, indexScanNode);
//      return leftExec;
//
//    default:
//      return null;
//    }
//  }
//
//  /**
//   * Create a shuffle file write executor to store intermediate data into local disks.
//   */
//  public PhysicalExec createShuffleFileWritePlan(TaskAttemptContext ctx,
//                                                 ShuffleFileWriteNode plan, PhysicalExec subOp) throws IOException {
//    switch (plan.getShuffleType()) {
//    case HASH_SHUFFLE:
//      return new HashShuffleFileWriteExec(ctx, sm, plan, subOp);
//
//    case RANGE_SHUFFLE:
//      SortExec sortExec = PhysicalPlanUtil.findExecutor(subOp, SortExec.class);
//
//      SortSpec[] sortSpecs = null;
//      if (sortExec != null) {
//        sortSpecs = sortExec.getSortSpecs();
//      } else {
//        Column[] columns = ctx.getDataChannel().getShuffleKeys();
//        SortSpec specs[] = new SortSpec[columns.length];
//        for (int i = 0; i < columns.length; i++) {
//          specs[i] = new SortSpec(columns[i]);
//        }
//      }
//      return new RangeShuffleFileWriteExec(ctx, sm, subOp, plan.getInSchema(), plan.getInSchema(), sortSpecs);
//
//    case NONE_SHUFFLE:
//      return new StoreTableExec(ctx, plan, subOp);
//
//    default:
//      throw new IllegalStateException(ctx.getDataChannel().getShuffleType() + " is not supported yet.");
//    }
//  }
//
//  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode, Stack<LogicalNode> node)
//      throws IOException {
//    if (ctx.getTable(scanNode.getCanonicalName()) == null) {
//      return new SeqScanExec(ctx, sm, scanNode, null);
//    }
//    Preconditions.checkNotNull(ctx.getTable(scanNode.getCanonicalName()),
//        "Error: There is no table matched to %s", scanNode.getCanonicalName() + "(" + scanNode.getTableName() + ")");
//
//    // check if an input is sorted in the same order to the subsequence sort operator.
//    // TODO - it works only if input files are raw files. We should check the file format.
//    // Since the default intermediate file format is raw file, it is not problem right now.
//    if (checkIfSortEquivalance(ctx, scanNode, node)) {
//      CatalogProtos.FragmentProto[] fragments = ctx.getTables(scanNode.getCanonicalName());
//      return new ExternalSortExec(ctx, sm, (SortNode) node.peek(), fragments);
//    } else {
//      Enforcer enforcer = ctx.getEnforcer();
//
//      // check if this table is broadcasted one or not.
//      boolean broadcastFlag = false;
//      if (enforcer != null && enforcer.hasEnforceProperty(TajoWorkerProtocol.EnforceProperty.EnforceType.BROADCAST)) {
//        List<TajoWorkerProtocol.EnforceProperty> properties = enforcer.getEnforceProperties(TajoWorkerProtocol
//            .EnforceProperty.EnforceType.BROADCAST);
//        for (TajoWorkerProtocol.EnforceProperty property : properties) {
//          broadcastFlag |= scanNode.getCanonicalName().equals(property.getBroadcast().getTableName());
//        }
//      }
//
//      if (scanNode instanceof PartitionedTableScanNode
//          && ((PartitionedTableScanNode)scanNode).getInputPaths() != null &&
//          ((PartitionedTableScanNode)scanNode).getInputPaths().length > 0) {
//
//        if (scanNode instanceof PartitionedTableScanNode) {
//          if (broadcastFlag) {
//            PartitionedTableScanNode partitionedTableScanNode = (PartitionedTableScanNode) scanNode;
//            List<FileFragment> fileFragments = TUtil.newList();
//            for (Path path : partitionedTableScanNode.getInputPaths()) {
//              fileFragments.addAll(TUtil.newList(sm.split(scanNode.getCanonicalName(), path)));
//            }
//
//            return new PartitionMergeScanExec(ctx, sm, scanNode,
//                FragmentConvertor.toFragmentProtoArray(fileFragments.toArray(new FileFragment[fileFragments.size()])));
//          }
//        }
//      }
//
//      CatalogProtos.FragmentProto[] fragments = ctx.getTables(scanNode.getCanonicalName());
//      return new SeqScanExec(ctx, sm, scanNode, fragments);
//    }
//  }
}
