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

package org.apache.tajo.engine.planner.global;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.global.builder.DistinctGroupbyBuilder;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.rewrite.ProjectionPushDownRule;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TajoWorker;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.*;

/**
 * Build DAG
 */
public class GlobalPlanner {
  private static Log LOG = LogFactory.getLog(GlobalPlanner.class);

  private final TajoConf conf;
  private final CatalogProtos.StoreType storeType;
  private CatalogService catalog;
  private TajoWorker.WorkerContext workerContext;

  public GlobalPlanner(final TajoConf conf, final CatalogService catalog) throws IOException {
    this.conf = conf;
    this.catalog = catalog;
    this.storeType = CatalogProtos.StoreType.valueOf(conf.getVar(ConfVars.SHUFFLE_FILE_FORMAT).toUpperCase());
    Preconditions.checkArgument(storeType != null);
  }

  public GlobalPlanner(final TajoConf conf, final TajoWorker.WorkerContext workerContext) throws IOException {
    this.conf = conf;
    this.workerContext = workerContext;
    this.storeType = CatalogProtos.StoreType.valueOf(conf.getVar(ConfVars.SHUFFLE_FILE_FORMAT).toUpperCase());
    Preconditions.checkArgument(storeType != null);
  }

  /**
   * TODO: this is hack. it must be refactored at TAJO-602.
   */
  public CatalogService getCatalog() {
    if (workerContext.getCatalog() != null) {
      return workerContext.getCatalog();
    } else if (catalog != null) {
      return catalog;
    } else {
      throw new IllegalStateException("No Catalog Instance");
    }
  }

  public CatalogProtos.StoreType getStoreType() {
    return storeType;
  }

  public class GlobalPlanContext {
    MasterPlan plan;
    Map<Integer, ExecutionBlock> execBlockMap = Maps.newHashMap();

    public MasterPlan getPlan() {
      return plan;
    }

    public Map<Integer, ExecutionBlock> getExecBlockMap() {
      return execBlockMap;
    }
  }

  /**
   * Builds a master plan from the given logical plan.
   */
  public void build(MasterPlan masterPlan) throws IOException, PlanningException {

    DistributedPlannerVisitor planner = new DistributedPlannerVisitor();
    GlobalPlanContext globalPlanContext = new GlobalPlanContext();
    globalPlanContext.plan = masterPlan;

    LOG.info(masterPlan.getLogicalPlan());

    // copy a logical plan in order to keep the original logical plan. The distributed planner can modify
    // an input logical plan.
    LogicalNode inputPlan = PlannerUtil.clone(masterPlan.getLogicalPlan(),
        masterPlan.getLogicalPlan().getRootBlock().getRoot());

    boolean autoBroadcast = QueryContext.getBoolVar(masterPlan.getContext(), conf,
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO);
    if (autoBroadcast) {

      // pre-visit the master plan in order to find tables to be broadcasted
      // this visiting does not make any execution block and change plan.
      BroadcastJoinMarkCandidateVisitor markCandidateVisitor = new BroadcastJoinMarkCandidateVisitor();
      markCandidateVisitor.visit(globalPlanContext,
          masterPlan.getLogicalPlan(), masterPlan.getLogicalPlan().getRootBlock(), inputPlan, new Stack<LogicalNode>());

      BroadcastJoinPlanVisitor broadcastJoinPlanVisitor = new BroadcastJoinPlanVisitor();
      broadcastJoinPlanVisitor.visit(globalPlanContext,
          masterPlan.getLogicalPlan(), masterPlan.getLogicalPlan().getRootBlock(), inputPlan, new Stack<LogicalNode>());
    }

    // create a distributed execution plan by visiting each logical node.
    // Its output is a graph, where each vertex is an execution block, and each edge is a data channel.
    // MasterPlan contains them.
    LogicalNode lastNode = planner.visit(globalPlanContext,
        masterPlan.getLogicalPlan(), masterPlan.getLogicalPlan().getRootBlock(), inputPlan, new Stack<LogicalNode>());
    ExecutionBlock childExecBlock = globalPlanContext.execBlockMap.get(lastNode.getPID());

    ExecutionBlock terminalBlock;
    // TODO - consider two terminal types: specified output or not
    if (childExecBlock.getPlan() != null) {
      terminalBlock = masterPlan.createTerminalBlock();
      DataChannel finalChannel = new DataChannel(childExecBlock.getId(), terminalBlock.getId());
      setFinalOutputChannel(finalChannel, lastNode.getOutSchema());
      masterPlan.addConnect(finalChannel);
    } else { // if one or more unions is terminal
      terminalBlock = childExecBlock;
      for (DataChannel outputChannel : masterPlan.getIncomingChannels(terminalBlock.getId())) {
        setFinalOutputChannel(outputChannel, lastNode.getOutSchema());
      }
    }

    masterPlan.setTerminal(terminalBlock);
    LOG.info("\n" + masterPlan.toString());
  }

  private static void setFinalOutputChannel(DataChannel outputChannel, Schema outputSchema) {
    outputChannel.setShuffleType(NONE_SHUFFLE);
    outputChannel.setShuffleOutputNum(1);
    outputChannel.setStoreType(CatalogProtos.StoreType.CSV);
    outputChannel.setSchema(outputSchema);
  }

  public static ScanNode buildInputExecutor(LogicalPlan plan, DataChannel channel) {
    Preconditions.checkArgument(channel.getSchema() != null,
        "Channel schema (" + channel.getSrcId().getId() + " -> " + channel.getTargetId().getId() +
            ") is not initialized");
    TableMeta meta = new TableMeta(channel.getStoreType(), new KeyValueSet());
    TableDesc desc = new TableDesc(channel.getSrcId().toString(), channel.getSchema(), meta, new Path("/"));
    ScanNode scanNode = plan.createNode(ScanNode.class);
    scanNode.init(desc);
    return scanNode;
  }

  private DataChannel createDataChannelFromJoin(ExecutionBlock leftBlock, ExecutionBlock rightBlock,
                                                ExecutionBlock parent, JoinNode join, boolean leftTable) {
    ExecutionBlock childBlock = leftTable ? leftBlock : rightBlock;

    DataChannel channel = new DataChannel(childBlock, parent, HASH_SHUFFLE, 32);
    channel.setStoreType(storeType);
    if (join.getJoinType() != JoinType.CROSS) {
      // ShuffleKeys need to not have thea-join condition because Tajo supports only equi-join.
      Column [][] joinColumns = PlannerUtil.joinJoinKeyForEachTable(join.getJoinQual(),
          leftBlock.getPlan().getOutSchema(), rightBlock.getPlan().getOutSchema(), false);
      if (leftTable) {
        channel.setShuffleKeys(joinColumns[0]);
      } else {
        channel.setShuffleKeys(joinColumns[1]);
      }
    }
    return channel;
  }

  /**
   * It calculates the total volume of all descendent relation nodes.
   */
  public static long computeDescendentVolume(LogicalNode node) throws PlanningException {

    if (node instanceof RelationNode) {
      switch (node.getType()) {
      case SCAN:
        ScanNode scanNode = (ScanNode) node;
        if (scanNode.getTableDesc().getStats() == null) {
          // TODO - this case means that data is not located in HDFS. So, we need additional
          // broadcast method.
          return Long.MAX_VALUE;
        } else {
          return scanNode.getTableDesc().getStats().getNumBytes();
        }
      case PARTITIONS_SCAN:
        PartitionedTableScanNode pScanNode = (PartitionedTableScanNode) node;
        if (pScanNode.getTableDesc().getStats() == null) {
          // TODO - this case means that data is not located in HDFS. So, we need additional
          // broadcast method.
          return Long.MAX_VALUE;
        } else {
          // if there is no selected partition
          if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
            return 0;
          } else {
            return pScanNode.getTableDesc().getStats().getNumBytes();
          }
        }
      case TABLE_SUBQUERY:
        return computeDescendentVolume(((TableSubQueryNode) node).getSubQuery());
      default:
        throw new IllegalArgumentException("Not RelationNode");
      }
    } else if (node instanceof UnaryNode) {
      return computeDescendentVolume(((UnaryNode) node).getChild());
    } else if (node instanceof BinaryNode) {
      BinaryNode binaryNode = (BinaryNode) node;
      return computeDescendentVolume(binaryNode.getLeftChild()) + computeDescendentVolume(binaryNode.getRightChild());
    }

    throw new PlanningException("Invalid State");
  }

  private static boolean checkIfCanBeOneOfBroadcastJoin(LogicalNode node) {
    return node.getType() == NodeType.SCAN || node.getType() == NodeType.PARTITIONS_SCAN;
  }

  /**
   * Get a volume of a table of a partitioned table
   * @param scanNode ScanNode corresponding to a table
   * @return table volume (bytes)
   */
  private static long getTableVolume(ScanNode scanNode) {
    long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
    if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
      PartitionedTableScanNode pScanNode = (PartitionedTableScanNode)scanNode;
      if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
        scanBytes = 0L;
      }
    }

    return scanBytes;
  }

  private ExecutionBlock buildJoinPlan(GlobalPlanContext context, JoinNode joinNode,
                                        ExecutionBlock leftBlock, ExecutionBlock rightBlock)
      throws PlanningException {
    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    boolean autoBroadcast = QueryContext.getBoolVar(context.getPlan().getContext(), conf,
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO);
    long broadcastThreshold = QueryContext.getLongVar(context.getPlan().getContext(), conf,
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD);

    // to check when the tajo.dist-query.join.broadcast.auto property is true
    if (autoBroadcast && joinNode.isCandidateBroadcast()) {
      LogicalNode leftNode = joinNode.getLeftChild();
      LogicalNode rightNode = joinNode.getRightChild();

      List<ScanNode> broadcastTargetScanNodes = new ArrayList<ScanNode>();
      int numLargeTables = 0;
      boolean leftBroadcast = false;
      boolean rightBroadcast = false;

      // TODO - in the the current implementation, a broadcast join on a bush join tree is not supported yet.
      //
      //        Join
      //       /    \
      //   Join     Join
      //   /  \     /  \
      // Scan Scan Scan Scan


      // Checking Left Side of Join
      if (ScanNode.isScanNode(leftNode)) {
        ScanNode scanNode = (ScanNode)leftNode;
        if (getTableVolume(scanNode) >= broadcastThreshold) {
          numLargeTables++;
        } else {
          leftBroadcast = true;
          broadcastTargetScanNodes.add(scanNode);
          LOG.info("JoinNode's left table " + scanNode.getCanonicalName() + " ("
              + getTableVolume(scanNode) + ") is marked a broadcasted table");
        }
      }

      // Checking Right Side OF Join
      if (ScanNode.isScanNode(rightNode)) {
        ScanNode scanNode = (ScanNode)rightNode;
        if (getTableVolume(scanNode) >= broadcastThreshold) {
          numLargeTables++;
        } else {
          rightBroadcast = true;
          broadcastTargetScanNodes.add(scanNode);
          LOG.info("JoinNode's right table " + scanNode.getCanonicalName() + " ("
              + getTableVolume(scanNode) + ") is marked a broadcasted table");
        }
      }

      JoinNode blockJoinNode = null;
      if (!leftBroadcast && !rightBroadcast) {
        // In the case of large, large, small, small
        // all small tables broadcast to right large table
        numLargeTables = 1;
      }
      for(LogicalNode eachNode: joinNode.getBroadcastCandidateTargets()) {
        if (eachNode.getPID() == joinNode.getPID()) {
          continue;
        }
        if (numLargeTables >= 2) {
          break;
        }
        JoinNode broadcastJoinNode = (JoinNode)eachNode;
        ScanNode scanNode = broadcastJoinNode.getRightChild();
        if (getTableVolume(scanNode) < broadcastThreshold) {
          broadcastTargetScanNodes.add(scanNode);
          blockJoinNode = broadcastJoinNode;
          LOG.info("The table " + scanNode.getCanonicalName() + " ("
              + getTableVolume(scanNode) + ") is marked a broadcasted table");
        } else {
          numLargeTables++;
          if (numLargeTables < 2) {
            blockJoinNode = broadcastJoinNode;
          }
        }
      }

      if (!broadcastTargetScanNodes.isEmpty()) {
        // make new execution block
        currentBlock = masterPlan.newExecutionBlock();

        if (!leftBroadcast && !rightBroadcast) {
          DataChannel leftChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, true);
          ScanNode leftScan = buildInputExecutor(masterPlan.getLogicalPlan(), leftChannel);
          joinNode.setLeftChild(leftScan);
          masterPlan.addConnect(leftChannel);

          DataChannel rightChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, false);
          ScanNode rightScan = buildInputExecutor(masterPlan.getLogicalPlan(), rightChannel);
          joinNode.setRightChild(rightScan);
          masterPlan.addConnect(rightChannel);
        }

        if (blockJoinNode != null) {
          LOG.info("Set execution's plan with join " + blockJoinNode + " for broadcast join");
          // set current execution block's plan with last broadcast join node
          currentBlock.setPlan(blockJoinNode);
        } else {
          currentBlock.setPlan(joinNode);
        }

        for (ScanNode eachBroadcastTarget: broadcastTargetScanNodes) {
          currentBlock.addBroadcastTable(eachBroadcastTarget.getCanonicalName());
          context.execBlockMap.remove(eachBroadcastTarget.getPID());
        }

        return currentBlock;
      }
    }

    LogicalNode leftNode = joinNode.getLeftChild();
    LogicalNode rightNode = joinNode.getRightChild();

    // symmetric repartition join
    boolean leftUnion = leftNode.getType() == NodeType.TABLE_SUBQUERY &&
        ((TableSubQueryNode)leftNode).getSubQuery().getType() == NodeType.UNION;
    boolean rightUnion = rightNode.getType() == NodeType.TABLE_SUBQUERY &&
        ((TableSubQueryNode)rightNode).getSubQuery().getType() == NodeType.UNION;

    if (leftUnion || rightUnion) { // if one of child execution block is union
      /*
       Join with tableC and result of union tableA, tableB is expected the following physical plan.
       But Union execution block is not necessary.
       |-eb_0001_000006 (Terminal)
          |-eb_0001_000005 (Join eb_0001_000003, eb_0001_000004)
             |-eb_0001_000004 (Scan TableC)
             |-eb_0001_000003 (Union TableA, TableB)
               |-eb_0001_000002 (Scan TableB)
               |-eb_0001_000001 (Scan TableA)

       The above plan can be changed to the following plan.
       |-eb_0001_000005 (Terminal)
          |-eb_0001_000003    (Join [eb_0001_000001, eb_0001_000002], eb_0001_000004)
             |-eb_0001_000004 (Scan TableC)
             |-eb_0001_000002 (Scan TableB)
             |-eb_0001_000001 (Scan TableA)

       eb_0001_000003's left child should be eb_0001_000001 + eb_0001_000001 and right child should be eb_0001_000004.
       For this eb_0001_000001 is representative of eb_0001_000001, eb_0001_000002.
       So eb_0001_000003's left child is eb_0001_000001
       */
      Column[][] joinColumns = null;
      if (joinNode.getJoinType() != JoinType.CROSS) {
        // ShuffleKeys need to not have thea-join condition because Tajo supports only equi-join.
        joinColumns = PlannerUtil.joinJoinKeyForEachTable(joinNode.getJoinQual(),
            leftNode.getOutSchema(), rightNode.getOutSchema(), false);
      }

      if (leftUnion && !rightUnion) { // if only left is union
        currentBlock = leftBlock;
        context.execBlockMap.remove(leftNode.getPID());
        Column[] shuffleKeys = (joinColumns != null) ? joinColumns[0] : null;
        Column[] otherSideShuffleKeys = (joinColumns != null) ? joinColumns[1] : null;
        buildJoinPlanWithUnionChannel(context, joinNode, currentBlock, leftBlock, rightBlock, leftNode,
            shuffleKeys, otherSideShuffleKeys, true);
        currentBlock.setPlan(joinNode);
      } else if (!leftUnion && rightUnion) { // if only right is union
        currentBlock = rightBlock;
        context.execBlockMap.remove(rightNode.getPID());
        Column[] shuffleKeys = (joinColumns != null) ? joinColumns[1] : null;
        Column[] otherSideShuffleKeys = (joinColumns != null) ? joinColumns[0] : null;
        buildJoinPlanWithUnionChannel(context, joinNode, currentBlock, rightBlock, leftBlock, rightNode,
            shuffleKeys, otherSideShuffleKeys, false);
        currentBlock.setPlan(joinNode);
      } else { // if both are unions
        currentBlock = leftBlock;
        context.execBlockMap.remove(leftNode.getPID());
        context.execBlockMap.remove(rightNode.getPID());
        buildJoinPlanWithUnionChannel(context, joinNode, currentBlock, leftBlock, null, leftNode,
            (joinColumns != null ? joinColumns[0] : null), null, true);
        buildJoinPlanWithUnionChannel(context, joinNode, currentBlock, rightBlock, null, rightNode,
            (joinColumns != null ? joinColumns[1] : null), null, false);
        currentBlock.setPlan(joinNode);
      }

      return currentBlock;
    } else {
      // !leftUnion && !rightUnion
      currentBlock = masterPlan.newExecutionBlock();
      DataChannel leftChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, true);
      DataChannel rightChannel = createDataChannelFromJoin(leftBlock, rightBlock, currentBlock, joinNode, false);

      ScanNode leftScan = buildInputExecutor(masterPlan.getLogicalPlan(), leftChannel);
      ScanNode rightScan = buildInputExecutor(masterPlan.getLogicalPlan(), rightChannel);

      joinNode.setLeftChild(leftScan);
      joinNode.setRightChild(rightScan);
      currentBlock.setPlan(joinNode);

      masterPlan.addConnect(leftChannel);
      masterPlan.addConnect(rightChannel);

      return currentBlock;
    }
  }

  private void buildJoinPlanWithUnionChannel(GlobalPlanContext context, JoinNode joinNode,
                                             ExecutionBlock targetBlock,
                                             ExecutionBlock sourceBlock,
                                             ExecutionBlock otherSideBlock,
                                             LogicalNode childNode,
                                             Column[] shuffleKeys,
                                             Column[] otherSideShuffleKeys,
                                             boolean left) {
    MasterPlan masterPlan = context.getPlan();
    String subQueryRelationName = ((TableSubQueryNode)childNode).getCanonicalName();
    ExecutionBlockId dedicatedScanNodeBlock = null;
    for (DataChannel channel : masterPlan.getIncomingChannels(sourceBlock.getId())) {
      // If all union and right, add channel to left
      if (otherSideBlock == null && !left) {
        DataChannel oldChannel = channel;
        masterPlan.disconnect(oldChannel.getSrcId(), oldChannel.getTargetId());
        channel = new DataChannel(oldChannel.getSrcId(), targetBlock.getId());
      }
      channel.setSchema(childNode.getOutSchema());
      channel.setShuffleType(HASH_SHUFFLE);
      channel.setShuffleOutputNum(32);
      if (shuffleKeys != null) {
        channel.setShuffleKeys(shuffleKeys);
      }

      ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
      scanNode.getOutSchema().setQualifier(subQueryRelationName);
      if (dedicatedScanNodeBlock == null) {
        dedicatedScanNodeBlock = channel.getSrcId();
        if (left) {
          joinNode.setLeftChild(scanNode);
        } else {
          joinNode.setRightChild(scanNode);
        }
      }
      masterPlan.addConnect(channel);
      targetBlock.addUnionScan(channel.getSrcId(), dedicatedScanNodeBlock);
    }

    // create other side channel
    if (otherSideBlock != null) {
      DataChannel otherSideChannel = new DataChannel(otherSideBlock, targetBlock, HASH_SHUFFLE, 32);
      otherSideChannel.setStoreType(storeType);
      if (otherSideShuffleKeys != null) {
        otherSideChannel.setShuffleKeys(otherSideShuffleKeys);
      }
      masterPlan.addConnect(otherSideChannel);

      ScanNode scan = buildInputExecutor(masterPlan.getLogicalPlan(), otherSideChannel);
      if (left) {
        joinNode.setRightChild(scan);
      } else {
        joinNode.setLeftChild(scan);
      }
    }
  }

  private AggregationFunctionCallEval createSumFunction(EvalNode [] args) throws InternalException {
    FunctionDesc functionDesc = getCatalog().getFunction("sum", CatalogProtos.FunctionType.AGGREGATION,
        args[0].getValueType());
    return new AggregationFunctionCallEval(functionDesc, (AggFunction) functionDesc.newInstance(), args);
  }

  private AggregationFunctionCallEval createCountFunction(EvalNode [] args) throws InternalException {
    FunctionDesc functionDesc = getCatalog().getFunction("count", CatalogProtos.FunctionType.AGGREGATION,
        args[0].getValueType());
    return new AggregationFunctionCallEval(functionDesc, (AggFunction) functionDesc.newInstance(), args);
  }

  private AggregationFunctionCallEval createCountRowFunction(EvalNode[] args) throws InternalException {
    FunctionDesc functionDesc = getCatalog().getFunction("count", CatalogProtos.FunctionType.AGGREGATION,
        new TajoDataTypes.DataType[]{});
    return new AggregationFunctionCallEval(functionDesc, (AggFunction) functionDesc.newInstance(), args);
  }

  private AggregationFunctionCallEval createMaxFunction(EvalNode [] args) throws InternalException {
    FunctionDesc functionDesc = getCatalog().getFunction("max", CatalogProtos.FunctionType.AGGREGATION,
        args[0].getValueType());
    return new AggregationFunctionCallEval(functionDesc, (AggFunction) functionDesc.newInstance(), args);
  }

  private AggregationFunctionCallEval createMinFunction(EvalNode [] args) throws InternalException {
    FunctionDesc functionDesc = getCatalog().getFunction("min", CatalogProtos.FunctionType.AGGREGATION,
        args[0].getValueType());
    return new AggregationFunctionCallEval(functionDesc, (AggFunction) functionDesc.newInstance(), args);
  }

  /**
   * It contains transformed functions and it related data.
   * Each non-distinct function is transformed into two functions for both first and second stages.
   */
  private static class RewrittenFunctions {
    AggregationFunctionCallEval [] firstStageEvals;
    Target [] firstStageTargets;
    AggregationFunctionCallEval secondStageEvals;

    public RewrittenFunctions(int firstStageEvalNum) {
      firstStageEvals = new AggregationFunctionCallEval[firstStageEvalNum];
      firstStageTargets = new Target[firstStageEvalNum];
    }
  }

  /**
   * Tajo uses three execution blocks for an aggregation operator including distinct aggregations.
   * We call this approach <i><b>three-phase aggregation</b></i>.
   *
   * In this case, non-distinct set functions (i.e., <code>count(1), sum(col1)</code>) should be rewritten
   * to other forms. Please see the following example. This is a rewriting case for a query which includes distinct
   * aggregation functions. In this example, <code>count(*)</code> functions are transformed into two
   * functions: count(*) in the inner query and sum() in the outer query.
   *
   * <h2>Original query</h2>
   * <pre>
   * SELECT
   *   grp1, grp2, count(*) as total, count(distinct grp3) as distinct_col
   * from
   *   rel1
   * group by
   *   grp1, grp2;
   * </pre>
   *
   * <h2>Rewritten query</h2>
   * <pre>
   * SELECT grp1, grp2, sum(cnt) as total, count(grp3) as distinct_col from (
   *   SELECT
   *     grp1, grp2, grp3, count(*) as cnt
   *   from
   *     rel1
   *   group by
   *     grp1, grp2, grp3) tmp1
   * group by
   *   grp1, grp2
   * ) table1;
   * </pre>
   *
   * The main objective of this method is to transform non-distinct aggregation functions for three-phase aggregation.
   */
  private RewrittenFunctions rewriteAggFunctionsForDistinctAggregation(GlobalPlanContext context,
                                                                       AggregationFunctionCallEval function)
      throws PlanningException {

    LogicalPlan plan = context.plan.getLogicalPlan();
    RewrittenFunctions rewritten = null;

    try {
      if (function.getName().equalsIgnoreCase("count")) {
        rewritten = new RewrittenFunctions(1);

        if (function.getArgs().length == 0) {
          rewritten.firstStageEvals[0] = createCountRowFunction(function.getArgs());
        } else {
          rewritten.firstStageEvals[0] = createCountFunction(function.getArgs());
        }
        String referenceName = plan.generateUniqueColumnName(rewritten.firstStageEvals[0]);
        FieldEval fieldEval = new FieldEval(referenceName, rewritten.firstStageEvals[0].getValueType());
        rewritten.firstStageTargets[0] = new Target(fieldEval);
        rewritten.secondStageEvals = createSumFunction(new EvalNode[] {fieldEval});
      } else if (function.getName().equalsIgnoreCase("sum")) {
        rewritten = new RewrittenFunctions(1);

        rewritten.firstStageEvals[0] = createSumFunction(function.getArgs());
        String referenceName = plan.generateUniqueColumnName(rewritten.firstStageEvals[0]);
        FieldEval fieldEval = new FieldEval(referenceName, rewritten.firstStageEvals[0].getValueType());
        rewritten.firstStageTargets[0] = new Target(fieldEval);
        rewritten.secondStageEvals = createSumFunction(new EvalNode[] {fieldEval});

      } else if (function.getName().equals("max")) {
        rewritten = new RewrittenFunctions(1);

        rewritten.firstStageEvals[0] = createMaxFunction(function.getArgs());
        String referenceName = plan.generateUniqueColumnName(rewritten.firstStageEvals[0]);
        FieldEval fieldEval = new FieldEval(referenceName, rewritten.firstStageEvals[0].getValueType());
        rewritten.firstStageTargets[0] = new Target(fieldEval);
        rewritten.secondStageEvals = createMaxFunction(new EvalNode[]{fieldEval});

      } else if (function.getName().equals("min")) {

        rewritten = new RewrittenFunctions(1);

        rewritten.firstStageEvals[0] = createMinFunction(function.getArgs());
        String referenceName = plan.generateUniqueColumnName(rewritten.firstStageEvals[0]);
        FieldEval fieldEval = new FieldEval(referenceName, rewritten.firstStageEvals[0].getValueType());
        rewritten.firstStageTargets[0] = new Target(fieldEval);
        rewritten.secondStageEvals = createMinFunction(new EvalNode[]{fieldEval});

      } else {
        throw new PlanningException("Cannot support a mix of other functions");
      }
    } catch (InternalException e) {
      LOG.error(e);
    }

    return rewritten;
  }

  public ExecutionBlock buildDistinctGroupbyAndUnionPlan(MasterPlan masterPlan, ExecutionBlock lastBlock,
                                                  DistinctGroupbyNode firstPhaseGroupBy,
                                                  DistinctGroupbyNode secondPhaseGroupBy) {
    DataChannel lastDataChannel = null;

    // It pushes down the first phase group-by operator into all child blocks.
    //
    // (second phase)    G (currentBlock)
    //                  /|\
    //                / / | \
    // (first phase) G G  G  G (child block)

    // They are already connected one another.
    // So, we don't need to connect them again.
    for (DataChannel dataChannel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      if (firstPhaseGroupBy.isEmptyGrouping()) {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 1);
      } else {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 32);
      }
      dataChannel.setSchema(firstPhaseGroupBy.getOutSchema());
      ExecutionBlock childBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

      // Why must firstPhaseGroupby be copied?
      //
      // A groupby in each execution block can have different child.
      // It affects groupby's input schema.
      DistinctGroupbyNode firstPhaseGroupbyCopy = PlannerUtil.clone(masterPlan.getLogicalPlan(), firstPhaseGroupBy);
      firstPhaseGroupbyCopy.setChild(childBlock.getPlan());
      childBlock.setPlan(firstPhaseGroupbyCopy);

      // just keep the last data channel.
      lastDataChannel = dataChannel;
    }

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), lastDataChannel);
    secondPhaseGroupBy.setChild(scanNode);
    lastBlock.setPlan(secondPhaseGroupBy);
    return lastBlock;
  }

  /**
   * If there are at least one distinct aggregation function, a query works as if the query is rewritten as follows:
   *
   * <h2>Original query</h2>
   * <pre>
   * SELECT
   *   grp1, grp2, count(*) as total, count(distinct grp3) as distinct_col
   * from
   *   rel1
   * group by
   *   grp1, grp2;
   * </pre>
   *
   * The query will work as if the query is rewritten into two queries as follows:
   *
   * <h2>Rewritten query</h2>
   * <pre>
   * SELECT grp1, grp2, sum(cnt) as total, count(grp3) as distinct_col from (
   *   SELECT
   *     grp1, grp2, grp3, count(*) as cnt
   *   from
   *     rel1
   *   group by
   *     grp1, grp2, grp3) tmp1
   * group by
   *   grp1, grp2
   * ) table1;
   * </pre>
   *
   * In more detail, the first aggregation aggregates not only original grouping fields but also distinct columns.
   * Non-distinct aggregation functions should be transformed to proper functions.
   * Then, the second aggregation aggregates only original grouping fields with distinct aggregation functions and
   * transformed non-distinct aggregation functions.
   *
   * As a result, although a no-distinct aggregation requires two stages, a distinct aggregation requires three
   * execution blocks.
   */
  private ExecutionBlock buildGroupByIncludingDistinctFunctionsMultiStage(GlobalPlanContext context,
                                                                ExecutionBlock latestExecBlock,
                                                                GroupbyNode groupbyNode) throws PlanningException {

    Column [] originalGroupingColumns = groupbyNode.getGroupingColumns();
    LinkedHashSet<Column> firstStageGroupingColumns =
        Sets.newLinkedHashSet(Arrays.asList(groupbyNode.getGroupingColumns()));
    List<AggregationFunctionCallEval> firstStageAggFunctions = Lists.newArrayList();
    List<AggregationFunctionCallEval> secondPhaseEvalNodes = Lists.newArrayList();
    List<Target> firstPhaseEvalNodeTargets = Lists.newArrayList();

    for (AggregationFunctionCallEval aggFunction : groupbyNode.getAggFunctions()) {
      if (aggFunction.isDistinct()) {
        // add distinct columns to first stage's grouping columns
        firstStageGroupingColumns.addAll(EvalTreeUtil.findUniqueColumns(aggFunction));
        // keep distinct aggregation functions for the second stage
        secondPhaseEvalNodes.add(aggFunction);

      } else {
        // Rewrite non-distinct aggregation functions
        RewrittenFunctions rewritten = rewriteAggFunctionsForDistinctAggregation(context, aggFunction);
        firstStageAggFunctions.addAll(Lists.newArrayList(rewritten.firstStageEvals));
        firstPhaseEvalNodeTargets.addAll(Lists.newArrayList(rewritten.firstStageTargets));

        // keep rewritten non-aggregation functions for the second stage
        secondPhaseEvalNodes.add(rewritten.secondStageEvals);
      }
    }

    int firstStageAggFunctionNum = firstStageAggFunctions.size();
    int firstStageGroupingKeyNum = firstStageGroupingColumns.size();

    int i = 0;
    Target [] firstStageTargets = new Target[firstStageGroupingKeyNum + firstStageAggFunctionNum];
    for (Column column : firstStageGroupingColumns) {
      Target target = new Target(new FieldEval(column));
      firstStageTargets[i++] = target;
    }
    for (Target target : firstPhaseEvalNodeTargets) {
      firstStageTargets[i++] = target;
    }

    // Create the groupby node for the first stage and set all necessary descriptions
    GroupbyNode firstStageGroupby = new GroupbyNode(context.plan.getLogicalPlan().newPID());
    firstStageGroupby.setGroupingColumns(TUtil.toArray(firstStageGroupingColumns, Column.class));
    firstStageGroupby.setAggFunctions(TUtil.toArray(firstStageAggFunctions, AggregationFunctionCallEval.class));
    firstStageGroupby.setTargets(firstStageTargets);
    firstStageGroupby.setChild(groupbyNode.getChild());
    firstStageGroupby.setInSchema(groupbyNode.getInSchema());

    // Makes two execution blocks for the first stage
    ExecutionBlock firstStage = buildGroupBy(context, latestExecBlock, firstStageGroupby);

    // Create the groupby node for the second stage.
    GroupbyNode secondPhaseGroupby = new GroupbyNode(context.plan.getLogicalPlan().newPID());
    secondPhaseGroupby.setGroupingColumns(originalGroupingColumns);
    secondPhaseGroupby.setAggFunctions(TUtil.toArray(secondPhaseEvalNodes, AggregationFunctionCallEval.class));
    secondPhaseGroupby.setTargets(groupbyNode.getTargets());

    ExecutionBlock secondStage = context.plan.newExecutionBlock();
    secondStage.setPlan(secondPhaseGroupby);
    SortSpec [] sortSpecs = PlannerUtil.columnsToSortSpecs(firstStageGroupingColumns);
    secondStage.getEnforcer().enforceSortAggregation(secondPhaseGroupby.getPID(), sortSpecs);

    // Create a data channel between the first and second stages
    DataChannel channel;
    channel = new DataChannel(firstStage, secondStage, HASH_SHUFFLE, 32);
    channel.setShuffleKeys(secondPhaseGroupby.getGroupingColumns().clone());
    channel.setSchema(firstStage.getPlan().getOutSchema());
    channel.setStoreType(storeType);

    // Setting for the second phase's logical plan
    ScanNode scanNode = buildInputExecutor(context.plan.getLogicalPlan(), channel);
    secondPhaseGroupby.setChild(scanNode);
    secondPhaseGroupby.setInSchema(scanNode.getOutSchema());
    secondStage.setPlan(secondPhaseGroupby);

    context.plan.addConnect(channel);

    return secondStage;
  }

  private ExecutionBlock buildGroupBy(GlobalPlanContext context, ExecutionBlock lastBlock,
                                      GroupbyNode groupbyNode) throws PlanningException {
    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    if (groupbyNode.isDistinct()) { // if there is at one distinct aggregation function
      DistinctGroupbyBuilder builder = new DistinctGroupbyBuilder(this);
      return builder.buildPlan(context, lastBlock, groupbyNode);
    } else {
      GroupbyNode firstPhaseGroupby = createFirstPhaseGroupBy(masterPlan.getLogicalPlan(), groupbyNode);

      if (hasUnionChild(firstPhaseGroupby)) {
        currentBlock = buildGroupbyAndUnionPlan(masterPlan, lastBlock, firstPhaseGroupby, groupbyNode);
      } else {
        // general hash-shuffled aggregation
        currentBlock = buildTwoPhaseGroupby(masterPlan, lastBlock, firstPhaseGroupby, groupbyNode);
      }
    }

    return currentBlock;
  }

  public static boolean hasUnionChild(UnaryNode node) {

    // there are two cases:
    //
    // The first case is:
    //
    //  create table [tbname] as select * from ( select ... UNION select ...) T
    //
    // We can generalize this case as 'a store operator on the top of union'.
    // In this case, a store operator determines a shuffle method.
    //
    // The second case is:
    //
    // select avg(..) from (select ... UNION select ) T
    //
    // We can generalize this case as 'a shuffle required operator on the top of union'.

    if (node.getChild() instanceof UnaryNode) { // first case
      UnaryNode child = node.getChild();

      if (child.getChild().getType() == NodeType.PROJECTION) {
        child = child.getChild();
      }

      if (child.getChild().getType() == NodeType.TABLE_SUBQUERY) {
        TableSubQueryNode tableSubQuery = child.getChild();
        return tableSubQuery.getSubQuery().getType() == NodeType.UNION;
      }

    } else if (node.getChild().getType() == NodeType.TABLE_SUBQUERY) { // second case
      TableSubQueryNode tableSubQuery = node.getChild();
      return tableSubQuery.getSubQuery().getType() == NodeType.UNION;
    }

    return false;
  }

  private ExecutionBlock buildGroupbyAndUnionPlan(MasterPlan masterPlan, ExecutionBlock lastBlock,
                                                  GroupbyNode firstPhaseGroupBy, GroupbyNode secondPhaseGroupBy) {
    DataChannel lastDataChannel = null;

    // It pushes down the first phase group-by operator into all child blocks.
    //
    // (second phase)    G (currentBlock)
    //                  /|\
    //                / / | \
    // (first phase) G G  G  G (child block)

    // They are already connected one another.
    // So, we don't need to connect them again.
    for (DataChannel dataChannel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      if (firstPhaseGroupBy.isEmptyGrouping()) {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 1);
      } else {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 32);
      }
      dataChannel.setSchema(firstPhaseGroupBy.getOutSchema());
      ExecutionBlock childBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

      // Why must firstPhaseGroupby be copied?
      //
      // A groupby in each execution block can have different child.
      // It affects groupby's input schema.
      GroupbyNode firstPhaseGroupbyCopy = PlannerUtil.clone(masterPlan.getLogicalPlan(), firstPhaseGroupBy);
      firstPhaseGroupbyCopy.setChild(childBlock.getPlan());
      childBlock.setPlan(firstPhaseGroupbyCopy);

      // just keep the last data channel.
      lastDataChannel = dataChannel;
    }

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), lastDataChannel);
    secondPhaseGroupBy.setChild(scanNode);
    lastBlock.setPlan(secondPhaseGroupBy);
    return lastBlock;
  }

  private ExecutionBlock buildTwoPhaseGroupby(MasterPlan masterPlan, ExecutionBlock latestBlock,
                                                     GroupbyNode firstPhaseGroupby, GroupbyNode secondPhaseGroupby) {

    ExecutionBlock childBlock = latestBlock;
    childBlock.setPlan(firstPhaseGroupby);
    ExecutionBlock currentBlock = masterPlan.newExecutionBlock();

    DataChannel channel;
    if (firstPhaseGroupby.isEmptyGrouping()) {
      channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 1);
      channel.setShuffleKeys(firstPhaseGroupby.getGroupingColumns());
    } else {
      channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 32);
      channel.setShuffleKeys(firstPhaseGroupby.getGroupingColumns());
    }
    channel.setSchema(firstPhaseGroupby.getOutSchema());
    channel.setStoreType(storeType);

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
    secondPhaseGroupby.setChild(scanNode);
    secondPhaseGroupby.setInSchema(scanNode.getOutSchema());
    currentBlock.setPlan(secondPhaseGroupby);

    masterPlan.addConnect(channel);

    return currentBlock;
  }

  public static GroupbyNode createFirstPhaseGroupBy(LogicalPlan plan, GroupbyNode groupBy) {
    Preconditions.checkNotNull(groupBy);

    GroupbyNode firstPhaseGroupBy = PlannerUtil.clone(plan, groupBy);
    GroupbyNode secondPhaseGroupBy = groupBy;

    // Set first phase expressions
    if (secondPhaseGroupBy.hasAggFunctions()) {
      int evalNum = secondPhaseGroupBy.getAggFunctions().length;
      AggregationFunctionCallEval [] secondPhaseEvals = secondPhaseGroupBy.getAggFunctions();
      AggregationFunctionCallEval [] firstPhaseEvals = new AggregationFunctionCallEval[evalNum];

      String [] firstPhaseEvalNames = new String[evalNum];
      for (int i = 0; i < evalNum; i++) {
        try {
          firstPhaseEvals[i] = (AggregationFunctionCallEval) secondPhaseEvals[i].clone();
        } catch (CloneNotSupportedException e) {
          throw new RuntimeException(e);
        }

        firstPhaseEvals[i].setFirstPhase();
        firstPhaseEvalNames[i] = plan.generateUniqueColumnName(firstPhaseEvals[i]);
        FieldEval param = new FieldEval(firstPhaseEvalNames[i], firstPhaseEvals[i].getValueType());
        secondPhaseEvals[i].setArgs(new EvalNode[] {param});
      }

      secondPhaseGroupBy.setAggFunctions(secondPhaseEvals);
      firstPhaseGroupBy.setAggFunctions(firstPhaseEvals);
      Target [] firstPhaseTargets = ProjectionPushDownRule.buildGroupByTarget(firstPhaseGroupBy, null,
          firstPhaseEvalNames);
      firstPhaseGroupBy.setTargets(firstPhaseTargets);
      secondPhaseGroupBy.setInSchema(PlannerUtil.targetToSchema(firstPhaseTargets));
    }
    return firstPhaseGroupBy;
  }

  private ExecutionBlock buildSortPlan(GlobalPlanContext context, ExecutionBlock childBlock, SortNode currentNode) {
    MasterPlan masterPlan = context.plan;
    ExecutionBlock currentBlock;

    SortNode firstSortNode = PlannerUtil.clone(context.plan.getLogicalPlan(), currentNode);

    if (firstSortNode.getChild().getType() == NodeType.TABLE_SUBQUERY &&
        ((TableSubQueryNode)firstSortNode.getChild()).getSubQuery().getType() == NodeType.UNION) {

      currentBlock = childBlock;
      for (DataChannel channel : masterPlan.getIncomingChannels(childBlock.getId())) {
        channel.setShuffle(RANGE_SHUFFLE, PlannerUtil.sortSpecsToSchema(currentNode.getSortKeys()).toArray(), 32);
        channel.setSchema(firstSortNode.getOutSchema());

        ExecutionBlock subBlock = masterPlan.getExecBlock(channel.getSrcId());
        SortNode s1 = PlannerUtil.clone(context.plan.getLogicalPlan(), firstSortNode);
        s1.setChild(subBlock.getPlan());
        subBlock.setPlan(s1);

        ScanNode secondScan = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
        currentNode.setChild(secondScan);
        currentNode.setInSchema(secondScan.getOutSchema());
        currentBlock.setPlan(currentNode);
        currentBlock.getEnforcer().addSortedInput(secondScan.getTableName(), currentNode.getSortKeys());
      }
    } else {
      LogicalNode childBlockPlan = childBlock.getPlan();
      firstSortNode.setChild(childBlockPlan);
      // sort is a non-projectable operator. So, in/out schemas are the same to its child operator.
      firstSortNode.setInSchema(childBlockPlan.getOutSchema());
      firstSortNode.setOutSchema(childBlockPlan.getOutSchema());
      childBlock.setPlan(firstSortNode);

      currentBlock = masterPlan.newExecutionBlock();
      DataChannel channel = new DataChannel(childBlock, currentBlock, RANGE_SHUFFLE, 32);
      channel.setShuffleKeys(PlannerUtil.sortSpecsToSchema(currentNode.getSortKeys()).toArray());
      channel.setSchema(firstSortNode.getOutSchema());

      ScanNode secondScan = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
      currentNode.setChild(secondScan);
      currentNode.setInSchema(secondScan.getOutSchema());
      currentBlock.setPlan(currentNode);
      currentBlock.getEnforcer().addSortedInput(secondScan.getTableName(), currentNode.getSortKeys());
      masterPlan.addConnect(channel);
    }

    return currentBlock;
  }

  /**
   * It builds a distributed execution block for CTAS, InsertNode, and StoreTableNode.
   */
  private ExecutionBlock buildStorePlan(GlobalPlanContext context,
                                        ExecutionBlock lastBlock,
                                        StoreTableNode currentNode) throws PlanningException {


    if(currentNode.hasPartition()) { // if a target table is a partitioned table

      // Verify supported partition types
      PartitionMethodDesc partitionMethod = currentNode.getPartitionMethod();
      if (partitionMethod.getPartitionType() != CatalogProtos.PartitionType.COLUMN) {
        throw new PlanningException(String.format("Not supported partitionsType :%s",
            partitionMethod.getPartitionType()));
      }

      if (hasUnionChild(currentNode)) { // if it has union children
        return buildShuffleAndStorePlanToPartitionedTableWithUnion(context, currentNode, lastBlock);
      } else { // otherwise
        return buildShuffleAndStorePlanToPartitionedTable(context, currentNode, lastBlock);
      }
    } else { // if result table is not a partitioned table, directly store it
      return buildNoPartitionedStorePlan(context, currentNode, lastBlock);
    }
  }

  /**
   * It makes a plan to store directly union plans into a non-partitioned table.
   */
  private ExecutionBlock buildShuffleAndStorePlanNoPartitionedTableWithUnion(GlobalPlanContext context,
                                                                             StoreTableNode currentNode,
                                                                             ExecutionBlock childBlock) {
    for (ExecutionBlock grandChildBlock : context.plan.getChilds(childBlock)) {
      StoreTableNode copy = PlannerUtil.clone(context.plan.getLogicalPlan(), currentNode);
      copy.setChild(grandChildBlock.getPlan());
      grandChildBlock.setPlan(copy);
    }
    return childBlock;
  }

  /**
   * It inserts shuffle and adds store plan on a partitioned table,
   * and it push downs those plans into child unions.
   */
  private ExecutionBlock buildShuffleAndStorePlanToPartitionedTableWithUnion(GlobalPlanContext context,
                                                                             StoreTableNode currentNode,
                                                                             ExecutionBlock lastBlock)
      throws PlanningException {

    MasterPlan masterPlan = context.plan;
    DataChannel lastChannel = null;
    for (DataChannel channel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      ExecutionBlock childBlock = masterPlan.getExecBlock(channel.getSrcId());
      setShuffleKeysFromPartitionedTableStore(currentNode, channel);
      channel.setSchema(childBlock.getPlan().getOutSchema());
      channel.setStoreType(storeType);
      lastChannel = channel;
    }

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), lastChannel);
    currentNode.setChild(scanNode);
    currentNode.setInSchema(scanNode.getOutSchema());
    lastBlock.setPlan(currentNode);
    return lastBlock;
  }

  /**
   * It inserts shuffle and adds store plan on a partitioned table.
   */
  private ExecutionBlock buildShuffleAndStorePlanToPartitionedTable(GlobalPlanContext context,
                                                                    StoreTableNode currentNode,
                                                                    ExecutionBlock lastBlock)
      throws PlanningException {
    MasterPlan masterPlan = context.plan;

    ExecutionBlock nextBlock = masterPlan.newExecutionBlock();
    DataChannel channel = new DataChannel(lastBlock, nextBlock, HASH_SHUFFLE, 32);
    setShuffleKeysFromPartitionedTableStore(currentNode, channel);
    channel.setSchema(lastBlock.getPlan().getOutSchema());
    channel.setStoreType(storeType);

    ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);
    currentNode.setChild(scanNode);
    currentNode.setInSchema(scanNode.getOutSchema());
    nextBlock.setPlan(currentNode);

    masterPlan.addConnect(channel);

    return nextBlock;
  }

  private ExecutionBlock buildNoPartitionedStorePlan(GlobalPlanContext context,
                                                     StoreTableNode currentNode,
                                                     ExecutionBlock childBlock) {
    if (hasUnionChild(currentNode)) { // when the below is union
      return buildShuffleAndStorePlanNoPartitionedTableWithUnion(context, currentNode, childBlock);
    } else {
      currentNode.setChild(childBlock.getPlan());
      currentNode.setInSchema(childBlock.getPlan().getOutSchema());
      childBlock.setPlan(currentNode);
      return childBlock;
    }
  }

  private void setShuffleKeysFromPartitionedTableStore(StoreTableNode node, DataChannel channel) {
    Preconditions.checkState(node.hasTargetTable(), "A target table must be a partitioned table.");
    PartitionMethodDesc partitionMethod = node.getPartitionMethod();

    if (node.getType() == NodeType.INSERT) {
      InsertNode insertNode = (InsertNode) node;
      channel.setSchema(((InsertNode)node).getProjectedSchema());
      Column [] shuffleKeys = new Column[partitionMethod.getExpressionSchema().size()];
      int i = 0;
      for (Column column : partitionMethod.getExpressionSchema().getColumns()) {
        int id = insertNode.getTableSchema().getColumnId(column.getQualifiedName());
        shuffleKeys[i++] = insertNode.getProjectedSchema().getColumn(id);
      }
      channel.setShuffleKeys(shuffleKeys);
      channel.setShuffleType(SCATTERED_HASH_SHUFFLE);
    } else {
      channel.setShuffleKeys(partitionMethod.getExpressionSchema().toArray());
      channel.setShuffleType(HASH_SHUFFLE);
    }
    channel.setShuffleOutputNum(32);
  }

  public class DistributedPlannerVisitor extends BasicLogicalPlanVisitor<GlobalPlanContext, LogicalNode> {

    @Override
    public LogicalNode visitRoot(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 LogicalRootNode node, Stack<LogicalNode> stack) throws PlanningException {
      return super.visitRoot(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitProjection(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                       ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitProjection(context, plan, block, node, stack);

      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());

      if (child.getType() == NodeType.TABLE_SUBQUERY &&
          ((TableSubQueryNode)child).getSubQuery().getType() == NodeType.UNION) {
        MasterPlan masterPlan = context.plan;
        for (DataChannel dataChannel : masterPlan.getIncomingChannels(execBlock.getId())) {
          ExecutionBlock subBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

          ProjectionNode copy = PlannerUtil.clone(plan, node);
          copy.setChild(subBlock.getPlan());
          subBlock.setPlan(copy);
        }
        execBlock.setPlan(null);
      } else {
        node.setChild(execBlock.getPlan());
        node.setInSchema(execBlock.getPlan().getOutSchema());
        execBlock.setPlan(node);
      }

      context.execBlockMap.put(node.getPID(), execBlock);
      return node;
    }

    @Override
    public LogicalNode visitLimit(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitLimit(context, plan, block, node, stack);

      ExecutionBlock execBlock;
      execBlock = context.execBlockMap.remove(child.getPID());
      if (child.getType() == NodeType.SORT) {
        node.setChild(execBlock.getPlan());
        execBlock.setPlan(node);

        ExecutionBlock childBlock = context.plan.getChild(execBlock, 0);
        LimitNode childLimit = PlannerUtil.clone(context.plan.getLogicalPlan(), node);
        childLimit.setChild(childBlock.getPlan());
        childBlock.setPlan(childLimit);

        DataChannel channel = context.plan.getChannel(childBlock, execBlock);
        channel.setShuffleOutputNum(1);
        context.execBlockMap.put(node.getPID(), execBlock);
      } else {
        node.setChild(execBlock.getPlan());
        execBlock.setPlan(node);

        ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
        DataChannel newChannel = new DataChannel(execBlock, newExecBlock, HASH_SHUFFLE, 1);
        newChannel.setShuffleKeys(new Column[]{});
        newChannel.setSchema(node.getOutSchema());
        newChannel.setStoreType(storeType);

        ScanNode scanNode = buildInputExecutor(plan, newChannel);
        LimitNode parentLimit = PlannerUtil.clone(context.plan.getLogicalPlan(), node);
        parentLimit.setChild(scanNode);
        newExecBlock.setPlan(parentLimit);
        context.plan.addConnect(newChannel);
        context.execBlockMap.put(parentLimit.getPID(), newExecBlock);
        node = parentLimit;
      }

      return node;
    }

    @Override
    public LogicalNode visitSort(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SortNode node, Stack<LogicalNode> stack) throws PlanningException {

      LogicalNode child = super.visitSort(context, plan, block, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildSortPlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitHaving(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    HavingNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitHaving(context, plan, block, node, stack);

      // Don't separate execution block. Having is pushed to the second grouping execution block.
      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      node.setChild(childBlock.getPlan());
      childBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), childBlock);

      return node;
    }

    @Override
    public LogicalNode visitWindowAgg(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    WindowAggNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitWindowAgg(context, plan, block, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildWindowAgg(context, childBlock, node);
      context.execBlockMap.put(newExecBlock.getPlan().getPID(), newExecBlock);

      return newExecBlock.getPlan();
    }

    private ExecutionBlock buildWindowAgg(GlobalPlanContext context, ExecutionBlock lastBlock,
                                        WindowAggNode windowAgg) throws PlanningException {
      MasterPlan masterPlan = context.plan;

      ExecutionBlock childBlock = lastBlock;
      ExecutionBlock currentBlock = masterPlan.newExecutionBlock();
      DataChannel channel;
      if (windowAgg.hasPartitionKeys()) { // if there is at one distinct aggregation function
        channel = new DataChannel(childBlock, currentBlock, RANGE_SHUFFLE, 32);
        channel.setShuffleKeys(windowAgg.getPartitionKeys());
      } else {
        channel = new DataChannel(childBlock, currentBlock, HASH_SHUFFLE, 1);
        channel.setShuffleKeys(null);
      }
      channel.setSchema(windowAgg.getInSchema());
      channel.setStoreType(storeType);

      LogicalNode childNode = windowAgg.getChild();
      ScanNode scanNode = buildInputExecutor(masterPlan.getLogicalPlan(), channel);

      if (windowAgg.hasPartitionKeys()) {
        SortNode sortNode = masterPlan.getLogicalPlan().createNode(SortNode.class);
        sortNode.setOutSchema(scanNode.getOutSchema());
        sortNode.setInSchema(scanNode.getOutSchema());
        sortNode.setSortSpecs(PlannerUtil.columnsToSortSpecs(windowAgg.getPartitionKeys()));
        sortNode.setChild(childNode);
        childBlock.setPlan(sortNode);

        windowAgg.setChild(scanNode);
      } else {
        windowAgg.setInSchema(scanNode.getOutSchema());
        windowAgg.setChild(scanNode);
        childBlock.setPlan(childNode);
      }

      currentBlock.setPlan(windowAgg);
      context.plan.addConnect(channel);

      return currentBlock;
    }

    @Override
    public LogicalNode visitGroupBy(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitGroupBy(context, plan, block, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildGroupBy(context, childBlock, node);
      context.execBlockMap.put(newExecBlock.getPlan().getPID(), newExecBlock);

      return newExecBlock.getPlan();
    }

    @Override
    public LogicalNode visitFilter(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitFilter(context, plan, block, node, stack);

      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());
      node.setChild(execBlock.getPlan());
      node.setInSchema(execBlock.getPlan().getOutSchema());
      execBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    @Override
    public LogicalNode visitJoin(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode leftChild = visit(context, plan, block, node.getLeftChild(), stack);
      ExecutionBlock leftChildBlock = context.execBlockMap.get(leftChild.getPID());

      if (leftChild.getType() == NodeType.JOIN && checkIfCanBeOneOfBroadcastJoin(node.getRightChild())) {
        ScanNode scanNode = node.getRightChild();
        if (leftChildBlock.isBroadcastTable(scanNode.getCanonicalName())) {
          context.execBlockMap.put(node.getPID(), leftChildBlock);
          return node;
        }

        // if left execution block's plan is replaced with parent node(join node)
        if (leftChildBlock.getPlan().getPID() == node.getPID()) {
          context.execBlockMap.put(node.getPID(), leftChildBlock);
          return node;
        }
      }

      LogicalNode rightChild = visit(context, plan, block, node.getRightChild(), stack);
      ExecutionBlock rightChildBlock = context.execBlockMap.get(rightChild.getPID());

      // In the case of broadcast join leftChildBlock can be replaced with upper join node.
      // So if the current join node is a child node of leftChildBlock's plan(join node)
      // the current join node already participates in broadcast join.
      LogicalNode leftChildBlockNode = leftChildBlock.getPlan();
      // If child block is union, child block has not plan
      if (leftChildBlockNode != null && leftChildBlockNode.getType() == NodeType.JOIN) {
        if (leftChildBlockNode.getPID() > node.getPID()) {
          context.execBlockMap.put(node.getPID(), leftChildBlock);
          return node;
        }
      }

      ExecutionBlock newExecBlock = buildJoinPlan(context, node, leftChildBlock, rightChildBlock);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitUnion(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                  UnionNode node, Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);
      LogicalPlan.QueryBlock leftQueryBlock = plan.getBlock(node.getLeftChild());
      LogicalNode leftChild = visit(context, plan, leftQueryBlock, leftQueryBlock.getRoot(), stack);

      LogicalPlan.QueryBlock rightQueryBlock = plan.getBlock(node.getRightChild());
      LogicalNode rightChild = visit(context, plan, rightQueryBlock, rightQueryBlock.getRoot(), stack);
      stack.pop();

      List<ExecutionBlock> unionBlocks = Lists.newArrayList();
      List<ExecutionBlock> queryBlockBlocks = Lists.newArrayList();

      ExecutionBlock leftBlock = context.execBlockMap.remove(leftChild.getPID());
      ExecutionBlock rightBlock = context.execBlockMap.remove(rightChild.getPID());
      if (leftChild.getType() == NodeType.UNION) {
        unionBlocks.add(leftBlock);
      } else {
        queryBlockBlocks.add(leftBlock);
      }
      if (rightChild.getType() == NodeType.UNION) {
        unionBlocks.add(rightBlock);
      } else {
        queryBlockBlocks.add(rightBlock);
      }

      ExecutionBlock execBlock;
      if (unionBlocks.size() == 0) {
        execBlock = context.plan.newExecutionBlock();
      } else {
        execBlock = unionBlocks.get(0);
      }

      for (ExecutionBlock childBlocks : unionBlocks) {
        for (ExecutionBlock grandChildBlock : context.plan.getChilds(childBlocks)) {
          queryBlockBlocks.add(grandChildBlock);
        }
      }

      for (ExecutionBlock childBlocks : queryBlockBlocks) {
        DataChannel channel = new DataChannel(childBlocks, execBlock, NONE_SHUFFLE, 1);
        channel.setStoreType(storeType);
        context.plan.addConnect(channel);
      }

      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    private LogicalNode handleUnaryNode(GlobalPlanContext context, LogicalNode child, LogicalNode node) {
      ExecutionBlock execBlock = context.execBlockMap.remove(child.getPID());
      execBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), execBlock);

      return node;
    }

    @Override
    public LogicalNode visitExcept(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                   ExceptNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitExcept(context, plan, queryBlock, node, stack);
      return handleUnaryNode(context, child, node);
    }

    @Override
    public LogicalNode visitIntersect(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                      IntersectNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitIntersect(context, plan, queryBlock, node, stack);
      return handleUnaryNode(context, child, node);
    }

    @Override
    public LogicalNode visitTableSubQuery(GlobalPlanContext context, LogicalPlan plan,
                                          LogicalPlan.QueryBlock queryBlock,
                                          TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitTableSubQuery(context, plan, queryBlock, node, stack);
      node.setSubQuery(child);

      ExecutionBlock currentBlock = context.execBlockMap.remove(child.getPID());

      if (child.getType() == NodeType.UNION) {
        List<TableSubQueryNode> addedTableSubQueries = new ArrayList<TableSubQueryNode>();
        TableSubQueryNode leftMostSubQueryNode = null;
        for (ExecutionBlock childBlock : context.plan.getChilds(currentBlock.getId())) {
          TableSubQueryNode copy = PlannerUtil.clone(plan, node);
          copy.setSubQuery(childBlock.getPlan());
          childBlock.setPlan(copy);
          addedTableSubQueries.add(copy);

          //Find a SubQueryNode which contains all columns in InputSchema matched with Target and OutputSchema's column
          if (copy.getInSchema().containsAll(copy.getOutSchema().getColumns())) {
            for (Target eachTarget : copy.getTargets()) {
              Set<Column> columns = EvalTreeUtil.findUniqueColumns(eachTarget.getEvalTree());
              if (copy.getInSchema().containsAll(columns)) {
                leftMostSubQueryNode = copy;
                break;
              }
            }
          }
        }
        if (leftMostSubQueryNode != null) {
          // replace target column name
          Target[] targets = leftMostSubQueryNode.getTargets();
          int[] targetMappings = new int[targets.length];
          for (int i = 0; i < targets.length; i++) {
            if (targets[i].getEvalTree().getType() != EvalType.FIELD) {
              throw new PlanningException("Target of a UnionNode's subquery should be FieldEval.");
            }
            int index = leftMostSubQueryNode.getInSchema().getColumnId(targets[i].getNamedColumn().getQualifiedName());
            if (index < 0) {
              // If a target has alias, getNamedColumn() only returns alias
              Set<Column> columns = EvalTreeUtil.findUniqueColumns(targets[i].getEvalTree());
              Column column = columns.iterator().next();
              index = leftMostSubQueryNode.getInSchema().getColumnId(column.getQualifiedName());
            }
            if (index < 0) {
              throw new PlanningException("Can't find matched Target in UnionNode's input schema: " + targets[i]
                  + "->" + leftMostSubQueryNode.getInSchema());
            }
            targetMappings[i] = index;
          }

          for (TableSubQueryNode eachNode: addedTableSubQueries) {
            if (eachNode.getPID() == leftMostSubQueryNode.getPID()) {
              continue;
            }
            Target[] eachNodeTargets = eachNode.getTargets();
            if (eachNodeTargets.length != targetMappings.length) {
              throw new PlanningException("Union query can't have different number of target columns.");
            }
            for (int i = 0; i < eachNodeTargets.length; i++) {
              Column inColumn = eachNode.getInSchema().getColumn(targetMappings[i]);
              eachNodeTargets[i].setAlias(eachNodeTargets[i].getNamedColumn().getQualifiedName());
              EvalNode evalNode = eachNodeTargets[i].getEvalTree();
              if (evalNode.getType() != EvalType.FIELD) {
                throw new PlanningException("Target of a UnionNode's subquery should be FieldEval.");
              }
              FieldEval fieldEval = (FieldEval) evalNode;
              EvalTreeUtil.changeColumnRef(fieldEval,
                  fieldEval.getColumnRef().getQualifiedName(), inColumn.getQualifiedName());
            }
          }
        } else {
          LOG.warn("Can't find left most SubQuery in the UnionNode.");
        }
      } else {
        currentBlock.setPlan(node);
      }
      context.execBlockMap.put(node.getPID(), currentBlock);
      return node;
    }

    @Override
    public LogicalNode visitScan(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                 ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
      ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
      newExecBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), newExecBlock);
      return node;
    }

    @Override
    public LogicalNode visitPartitionedTableScan(GlobalPlanContext context, LogicalPlan plan,
                                                 LogicalPlan.QueryBlock block, PartitionedTableScanNode node,
                                                 Stack<LogicalNode> stack)throws PlanningException {
      ExecutionBlock newExecBlock = context.plan.newExecutionBlock();
      newExecBlock.setPlan(node);
      context.execBlockMap.put(node.getPID(), newExecBlock);
      return node;
    }

    @Override
    public LogicalNode visitStoreTable(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                       StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitStoreTable(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitCreateTable(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                       CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode child = super.visitStoreTable(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }

    @Override
    public LogicalNode visitInsert(GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                   InsertNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      LogicalNode child = super.visitInsert(context, plan, queryBlock, node, stack);

      ExecutionBlock childBlock = context.execBlockMap.remove(child.getPID());
      ExecutionBlock newExecBlock = buildStorePlan(context, childBlock, node);
      context.execBlockMap.put(node.getPID(), newExecBlock);

      return node;
    }
  }

  @SuppressWarnings("unused")
  private class ConsecutiveUnionFinder extends BasicLogicalPlanVisitor<List<UnionNode>, LogicalNode> {
    @Override
    public LogicalNode visitUnion(List<UnionNode> unionNodeList, LogicalPlan plan, LogicalPlan.QueryBlock queryBlock,
                                  UnionNode node, Stack<LogicalNode> stack)
        throws PlanningException {
      if (node.getType() == NodeType.UNION) {
        unionNodeList.add(node);
      }

      stack.push(node);
      TableSubQueryNode leftSubQuery = node.getLeftChild();
      TableSubQueryNode rightSubQuery = node.getRightChild();
      if (leftSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visit(unionNodeList, plan, queryBlock, leftSubQuery, stack);
      }
      if (rightSubQuery.getSubQuery().getType() == NodeType.UNION) {
        visit(unionNodeList, plan, queryBlock, rightSubQuery, stack);
      }
      stack.pop();

      return node;
    }
  }
}
