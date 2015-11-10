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

package org.apache.tajo.querymaster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.rules.GlobalPlanRewriteUtil;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.logical.SortNode.SortPurpose;
import org.apache.tajo.plan.serder.PlanProto.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.plan.serder.PlanProto.EnforceProperty;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.querymaster.Task.IntermediateEntry;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.worker.FetchImpl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.*;

/**
 * Repartitioner creates non-leaf tasks and shuffles intermediate data.
 * It supports two repartition methods, such as hash and range repartition.
 */
public class Repartitioner {
  private static final Log LOG = LogFactory.getLog(Repartitioner.class);

  private final static int HTTP_REQUEST_MAXIMUM_LENGTH = 1900;
  private final static String UNKNOWN_HOST = "unknown";

  public static void scheduleFragmentsForJoinQuery(TaskSchedulerContext schedulerContext, Stage stage)
      throws IOException, TajoException {
    ExecutionBlock execBlock = stage.getBlock();
    QueryMasterTask.QueryMasterTaskContext masterContext = stage.getContext();

    ScanNode[] scans = execBlock.getScanNodes();
    Fragment[] fragments = new Fragment[scans.length];
    long[] stats = new long[scans.length];

    // initialize variables from the child operators
    for (int i = 0; i < scans.length; i++) {
      TableDesc tableDesc = masterContext.getTableDesc(scans[i]);

      if (tableDesc == null) { // if it is a real table stored on storage
        if (execBlock.getUnionScanMap() != null && !execBlock.getUnionScanMap().isEmpty()) {
          for (Map.Entry<ExecutionBlockId, ExecutionBlockId> unionScanEntry: execBlock.getUnionScanMap().entrySet()) {
            ExecutionBlockId originScanEbId = unionScanEntry.getKey();
            stats[i] += masterContext.getStage(originScanEbId).getResultStats().getNumBytes();
          }
        } else {
          ExecutionBlockId scanEBId = TajoIdUtils.createExecutionBlockId(scans[i].getTableName());
          stats[i] = masterContext.getStage(scanEBId).getResultStats().getNumBytes();
        }

        // TODO - We should remove dummy fragment usages
        fragments[i] = new FileFragment(scans[i].getCanonicalName(), new Path("/dummy"), 0, 0,
            new String[]{UNKNOWN_HOST});

      } else {

        stats[i] = GlobalPlanRewriteUtil.computeDescendentVolume(scans[i]);

        // if table has no data, tablespace will return empty FileFragment.
        // So, we need to handle FileFragment by its size.
        // If we don't check its size, it can cause IndexOutOfBoundsException.
        Tablespace space = TablespaceManager.get(tableDesc.getUri());
        List<Fragment> fileFragments = space.getSplits(scans[i].getCanonicalName(), tableDesc, null);
        if (fileFragments.size() > 0) {
          fragments[i] = fileFragments.get(0);
        } else {
          fragments[i] = new FileFragment(scans[i].getCanonicalName(),
              new Path(tableDesc.getUri()), 0, 0, new String[]{UNKNOWN_HOST});
        }
      }
    }

    // If one of inner join tables has no input data, it means that this execution block has no result row.
    JoinNode joinNode = PlannerUtil.findMostBottomNode(execBlock.getPlan(), NodeType.JOIN);
    if (joinNode != null) {
      if ( (joinNode.getJoinType() == JoinType.INNER)) {
        LogicalNode leftNode = joinNode.getLeftChild();
        LogicalNode rightNode = joinNode.getRightChild();
        for (int i = 0; i < stats.length; i++) {
          if (scans[i].getPID() == leftNode.getPID() || scans[i].getPID() == rightNode.getPID()) {
            if (stats[i] == 0) {
              LOG.info(scans[i] + " 's input data is zero. Inner join's result is empty.");
              return;
            }
          }
        }
      }
    }

    // If node is outer join and a preserved relation is empty, it should return zero rows.
    joinNode = PlannerUtil.findTopNode(execBlock.getPlan(), NodeType.JOIN);
    if (joinNode != null) {
      // If all stats are zero, return
      boolean isEmptyAllJoinTables = true;
      for (int i = 0; i < stats.length; i++) {
        if (stats[i] > 0) {
          isEmptyAllJoinTables = false;
          break;
        }
      }
      if (isEmptyAllJoinTables) {
        LOG.info("All input join tables are empty.");
        return;
      }

      // find left top scan node
      ScanNode leftScanNode = PlannerUtil.findTopNode(joinNode.getLeftChild(), NodeType.SCAN);
      ScanNode rightScanNode = PlannerUtil.findTopNode(joinNode.getRightChild(), NodeType.SCAN);

      long leftStats = -1;
      long rightStats = -1;
      if (stats.length == 2) {
        for (int i = 0; i < stats.length; i++) {
          if (scans[i].equals(leftScanNode)) {
            leftStats = stats[i];
          } else if (scans[i].equals(rightScanNode)) {
            rightStats = stats[i];
          }
        }
        if (joinNode.getJoinType() == JoinType.LEFT_OUTER) {
          if (leftStats == 0) {
            return;
          }
        }
        if (joinNode.getJoinType() == JoinType.RIGHT_OUTER) {
          if (rightStats == 0) {
            return;
          }
        }
      }
    }

    // Assigning either fragments or fetch urls to query units
    if (execBlock.hasBroadcastRelation()) { // If some relations of this EB are broadcasted
      boolean hasNonLeafNode = false;
      List<Integer> largeScanIndexList = new ArrayList<>();
      List<Integer> broadcastIndexList = new ArrayList<>();
      String nonLeafScanNames = "";
      String namePrefix = "";
      long maxStats = Long.MIN_VALUE;
      int maxStatsScanIdx = -1;
      StringBuilder nonLeafScanNamesBuilder = new StringBuilder();

      String intermediateDataFormat = schedulerContext.getMasterContext().getConf().getVar(ConfVars.SHUFFLE_FILE_FORMAT);
      for (int i = 0; i < scans.length; i++) {

        if (scans[i].getTableDesc().getMeta().getDataFormat().equalsIgnoreCase(intermediateDataFormat)) {
          // Intermediate data scan
          hasNonLeafNode = true;
          largeScanIndexList.add(i);
          nonLeafScanNamesBuilder.append(namePrefix).append(scans[i].getCanonicalName());
          namePrefix = ",";
        }
        if (execBlock.isBroadcastRelation(scans[i])) {
          broadcastIndexList.add(i);
        } else {
          // finding largest table.
          if (stats[i] > 0 && stats[i] > maxStats) {
            maxStats = stats[i];
            maxStatsScanIdx = i;
          }
        }
      }
      nonLeafScanNames = nonLeafScanNamesBuilder.toString();
      if (maxStatsScanIdx == -1) {
        maxStatsScanIdx = 0;
      }

      if (!hasNonLeafNode) {
        if (largeScanIndexList.size() > 1) {
          StringBuilder largeTableNamesBuilder = new StringBuilder();
          for (Integer eachId : largeScanIndexList) {
            largeTableNamesBuilder.append(scans[eachId].getTableName()).append(',');
          }
          throw new IOException("Broadcast join with leaf node should have only one large table, " +
              "but " + largeScanIndexList.size() + ", tables=" + largeTableNamesBuilder.toString());
        }
        int baseScanIdx = largeScanIndexList.isEmpty() ? maxStatsScanIdx : largeScanIndexList.get(0);
        LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join, base_table=%s, base_volume=%d",
            scans[baseScanIdx].getCanonicalName(), stats[baseScanIdx]));
        scheduleLeafTasksWithBroadcastTable(schedulerContext, stage, baseScanIdx, fragments);
      } else {
        if (largeScanIndexList.size() > 2) {
          throw new IOException("Symmetric Repartition Join should have two scan node, but " + nonLeafScanNames);
        }

        //select intermediate scan and stats
        long[] intermediateScanStats = new long[largeScanIndexList.size()];
        Fragment[] intermediateFragments = new Fragment[largeScanIndexList.size()];
        int index = 0;
        for (Integer eachIdx : largeScanIndexList) {
          intermediateScanStats[index] = stats[eachIdx];
          intermediateFragments[index++] = fragments[eachIdx];
        }
        Fragment[] broadcastFragments = new Fragment[broadcastIndexList.size()];
        ScanNode[] broadcastScans = new ScanNode[broadcastIndexList.size()];
        long[] broadcastStats = new long[broadcastIndexList.size()];
        index = 0;
        for (Integer eachIdx : broadcastIndexList) {
          scans[eachIdx].setBroadcastTable(true);
          broadcastScans[index] = scans[eachIdx];
          broadcastStats[index] = stats[eachIdx];
          broadcastFragments[index] = fragments[eachIdx];
          index++;
        }
        LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join, join_node=%s", nonLeafScanNames));
        scheduleSymmetricRepartitionJoin(masterContext, schedulerContext, stage,
            intermediateScanStats, intermediateFragments, broadcastScans, broadcastStats, broadcastFragments);
      }
    } else {
      LOG.info("[Distributed Join Strategy] : Symmetric Repartition Join");
      scheduleSymmetricRepartitionJoin(masterContext, schedulerContext, stage, stats, fragments, null, null, null);
    }
  }

  /**
   * Scheduling in tech case of Symmetric Repartition Join
   * @param masterContext
   * @param schedulerContext
   * @param stage
   * @param stats
   * @param fragments
   * @throws IOException
   */
  private static void scheduleSymmetricRepartitionJoin(QueryMasterTask.QueryMasterTaskContext masterContext,
                                                       TaskSchedulerContext schedulerContext,
                                                       Stage stage,
                                                       long[] stats,
                                                       Fragment[] fragments,
                                                       ScanNode[] broadcastScans,
                                                       long[] broadcastStats,
                                                       Fragment[] broadcastFragments) throws IOException, TajoException {
    MasterPlan masterPlan = stage.getMasterPlan();
    ExecutionBlock execBlock = stage.getBlock();
    // The hash map is modeling as follows:
    // <Part Id, <EbId, List<Intermediate Data>>>
    Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> hashEntries =
            new HashMap<>();

    // Grouping IntermediateData by a partition key and a table name
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(stage.getId());

    // In the case of join with union, there is one ScanNode for union.
    Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = execBlock.getUnionScanMap();
    for (ExecutionBlock childBlock : childBlocks) {
      ExecutionBlockId scanEbId = unionScanMap.get(childBlock.getId());
      if (scanEbId == null) {
        scanEbId = childBlock.getId();
      }
      Stage childExecSM = stage.getContext().getStage(childBlock.getId());

      if (childExecSM.getHashShuffleIntermediateEntries() != null &&
          !childExecSM.getHashShuffleIntermediateEntries().isEmpty()) {
        for (IntermediateEntry intermediateEntry: childExecSM.getHashShuffleIntermediateEntries()) {
          intermediateEntry.setEbId(childBlock.getId());
          if (hashEntries.containsKey(intermediateEntry.getPartId())) {
            Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
                hashEntries.get(intermediateEntry.getPartId());

            if (tbNameToInterm.containsKey(scanEbId)) {
              tbNameToInterm.get(scanEbId).add(intermediateEntry);
            } else {
              tbNameToInterm.put(scanEbId, TUtil.newList(intermediateEntry));
            }
          } else {
            Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
                    new HashMap<>();
            tbNameToInterm.put(scanEbId, TUtil.newList(intermediateEntry));
            hashEntries.put(intermediateEntry.getPartId(), tbNameToInterm);
          }
        }
      } else {
        //if no intermidatedata(empty table), make empty entry
        int emptyPartitionId = 0;
        if (hashEntries.containsKey(emptyPartitionId)) {
          Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm = hashEntries.get(emptyPartitionId);
          if (tbNameToInterm.containsKey(scanEbId))
            tbNameToInterm.get(scanEbId).addAll(new ArrayList<>());
          else
            tbNameToInterm.put(scanEbId, new ArrayList<>());
        } else {
          Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
                  new HashMap<>();
          tbNameToInterm.put(scanEbId, new ArrayList<>());
          hashEntries.put(emptyPartitionId, tbNameToInterm);
        }
      }
    }

    // hashEntries can be zero if there are no input data.
    // In the case, it will cause the zero divided exception.
    // it avoids this problem.
    long leftStats = stats[0];
    long rightStats = stats.length == 2 ? stats[1] : broadcastStats[0];
    int[] avgSize = new int[2];
    avgSize[0] = hashEntries.size() == 0 ? 0 : (int) (leftStats / hashEntries.size());
    avgSize[1] = hashEntries.size() == 0 ? 0 : (int) (stats.length == 2 ? (rightStats / hashEntries.size()) : rightStats);
    int bothFetchSize = avgSize[0] + avgSize[1];

    // Getting the desire number of join tasks according to the volumn
    // of a larger table
    long largerStat = leftStats >= rightStats ? leftStats : rightStats;
    int desireJoinTaskVolumn = stage.getMasterPlan().getContext().getInt(SessionVars.JOIN_TASK_INPUT_SIZE);

    // calculate the number of tasks according to the data size
    int mb = (int) Math.ceil((double) largerStat / 1048576);
    LOG.info("Larger intermediate data is approximately " + mb + " MB");
    // determine the number of task per 64MB
    int maxTaskNum = (int) Math.ceil((double) mb / desireJoinTaskVolumn);
    LOG.info("The calculated number of tasks is " + maxTaskNum);
    LOG.info("The number of total shuffle keys is " + hashEntries.size());
    // the number of join tasks cannot be larger than the number of
    // distinct partition ids.
    int joinTaskNum = Math.min(maxTaskNum, hashEntries.size());
    LOG.info("The determined number of join tasks is " + joinTaskNum);

    List<Fragment> rightFragments = new ArrayList<>();
    if (fragments.length == 2) {
      rightFragments.add(fragments[1]);
    }

    if (broadcastFragments != null) {
      //In this phase a ScanNode has a single fragment.
      //If there are more than one data files, that files should be added to fragments or partition path

      for (ScanNode eachScan: broadcastScans) {

        Path[] partitionScanPaths = null;
        TableDesc tableDesc = masterContext.getTableDesc(eachScan);
        Tablespace space = TablespaceManager.get(tableDesc.getUri());

        if (eachScan.getType() == NodeType.PARTITIONS_SCAN) {

          PartitionedTableScanNode partitionScan = (PartitionedTableScanNode)eachScan;
          partitionScanPaths = partitionScan.getInputPaths();
          // set null to inputPaths in getFragmentsFromPartitionedTable()
          getFragmentsFromPartitionedTable((FileTablespace) space, eachScan, tableDesc);
          partitionScan.setInputPaths(partitionScanPaths);

        } else {

          Collection<Fragment> scanFragments =
              space.getSplits(eachScan.getCanonicalName(), tableDesc, eachScan.getQual());
          if (scanFragments != null) {
            rightFragments.addAll(scanFragments);
          }

        }
      }
    }
    Stage.scheduleFragment(stage, fragments[0], rightFragments);

    // Assign partitions to tasks in a round robin manner.
    for (Entry<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> entry
        : hashEntries.entrySet()) {
      addJoinShuffle(stage, entry.getKey(), entry.getValue());
    }

    schedulerContext.setTaskSize((int) Math.ceil((double) bothFetchSize / joinTaskNum));
    schedulerContext.setEstimatedTaskNum(joinTaskNum);
  }

  /**
   * merge intermediate entry by ebid, pullhost
   * @param hashEntries
   * @return
   */
  public static Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> mergeIntermediateByPullHost(
      Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> hashEntries) {
    Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> mergedHashEntries =
            new HashMap<>();

    for(Entry<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> entry: hashEntries.entrySet()) {
      Integer partId = entry.getKey();
      for (Entry<ExecutionBlockId, List<IntermediateEntry>> partEntry: entry.getValue().entrySet()) {
        List<IntermediateEntry> intermediateList = partEntry.getValue();
        if (intermediateList == null || intermediateList.isEmpty()) {
          continue;
        }
        ExecutionBlockId ebId = partEntry.getKey();
        // EBID + PullHost -> IntermediateEntry
        // In the case of union partEntry.getKey() return's delegated EBID.
        // Intermediate entries are merged by real EBID.
        Map<String, IntermediateEntry> ebMerged = new HashMap<>();

        for (IntermediateEntry eachIntermediate: intermediateList) {
          String ebMergedKey = eachIntermediate.getEbId().toString() + eachIntermediate.getPullHost().getPullAddress();
          IntermediateEntry intermediateEntryPerPullHost = ebMerged.get(ebMergedKey);
          if (intermediateEntryPerPullHost == null) {
            intermediateEntryPerPullHost = new IntermediateEntry(-1, -1, partId, eachIntermediate.getPullHost());
            intermediateEntryPerPullHost.setEbId(eachIntermediate.getEbId());
            ebMerged.put(ebMergedKey, intermediateEntryPerPullHost);
          }
          intermediateEntryPerPullHost.setVolume(intermediateEntryPerPullHost.getVolume() + eachIntermediate.getVolume());
        }

        List<IntermediateEntry> ebIntermediateEntries = new ArrayList<>(ebMerged.values());

        Map<ExecutionBlockId, List<IntermediateEntry>> mergedPartEntries = mergedHashEntries.get(partId);
        if (mergedPartEntries == null) {
          mergedPartEntries = new HashMap<>();
          mergedHashEntries.put(partId, mergedPartEntries);
        }
        mergedPartEntries.put(ebId, ebIntermediateEntries);
      }
    }
    return mergedHashEntries;
  }

  /**
   * It creates a number of fragments for all partitions.
   */
  public static List<Fragment> getFragmentsFromPartitionedTable(Tablespace tsHandler,
                                                                          ScanNode scan,
                                                                          TableDesc table) throws IOException {
    Preconditions.checkArgument(tsHandler instanceof FileTablespace, "tsHandler must be FileTablespace");
    if (!(scan instanceof PartitionedTableScanNode)) {
      throw new IllegalArgumentException("scan should be a PartitionedTableScanNode type.");
    }
    List<Fragment> fragments = Lists.newArrayList();
    PartitionedTableScanNode partitionsScan = (PartitionedTableScanNode) scan;
    fragments.addAll(((FileTablespace) tsHandler).getSplits(
        scan.getCanonicalName(), table.getMeta(), table.getSchema(), partitionsScan.getInputPaths()));
    partitionsScan.setInputPaths(null);
    return fragments;
  }

  private static void scheduleLeafTasksWithBroadcastTable(TaskSchedulerContext schedulerContext, Stage stage,
                                                          int baseScanId, Fragment[] fragments)
      throws IOException, TajoException {
    ExecutionBlock execBlock = stage.getBlock();
    ScanNode[] scans = execBlock.getScanNodes();

    for (int i = 0; i < scans.length; i++) {
      if (i != baseScanId) {
        scans[i].setBroadcastTable(true);
      }
    }

    // Large table(baseScan)
    //  -> add all fragment to baseFragments
    //  -> each fragment is assigned to a Task by DefaultTaskScheduler.handle()
    // Broadcast table
    //  all fragments or paths assigned every Large table's scan task.
    //  -> PARTITIONS_SCAN
    //     . add all partition paths to node's inputPaths variable
    //  -> SCAN
    //     . add all fragments to broadcastFragments
    Collection<Fragment> baseFragments = null;
    List<Fragment> broadcastFragments = new ArrayList<>();
    for (int i = 0; i < scans.length; i++) {
      ScanNode scan = scans[i];
      TableDesc desc = stage.getContext().getTableDesc(scan);

      Collection<Fragment> scanFragments;
      Path[] partitionScanPaths = null;


      Tablespace space = TablespaceManager.get(desc.getUri());

      if (scan.getType() == NodeType.PARTITIONS_SCAN) {
        PartitionedTableScanNode partitionScan = (PartitionedTableScanNode) scan;
        partitionScanPaths = partitionScan.getInputPaths();
        // set null to inputPaths in getFragmentsFromPartitionedTable()
        scanFragments = getFragmentsFromPartitionedTable(space, scan, desc);
      } else {
        scanFragments = space.getSplits(scan.getCanonicalName(), desc, scan.getQual());
      }

      if (scanFragments != null) {
        if (i == baseScanId) {
          baseFragments = scanFragments;
        } else {
          if (scan.getType() == NodeType.PARTITIONS_SCAN) {
            PartitionedTableScanNode partitionScan = (PartitionedTableScanNode)scan;
            // PhisicalPlanner make PartitionMergeScanExec when table is boradcast table and inputpaths is not empty
            partitionScan.setInputPaths(partitionScanPaths);
          } else {
            broadcastFragments.addAll(scanFragments);
          }
        }
      }
    }

    if (baseFragments == null) {
      throw new IOException("No fragments for " + scans[baseScanId].getTableName());
    }

    Stage.scheduleFragments(stage, baseFragments, broadcastFragments);
    schedulerContext.setEstimatedTaskNum(baseFragments.size());
  }

  private static void addJoinShuffle(Stage stage, int partitionId,
                                     Map<ExecutionBlockId, List<IntermediateEntry>> grouppedPartitions) {
    Map<String, List<FetchImpl>> fetches = new HashMap<>();
    for (ExecutionBlock execBlock : stage.getMasterPlan().getChilds(stage.getId())) {
      if (grouppedPartitions.containsKey(execBlock.getId())) {
        Collection<FetchImpl> requests = mergeShuffleRequest(partitionId, HASH_SHUFFLE,
            grouppedPartitions.get(execBlock.getId()));
        fetches.put(execBlock.getId().toString(), Lists.newArrayList(requests));
      }
    }

    if (fetches.isEmpty()) {
      LOG.info(stage.getId() + "'s " + partitionId + " partition has empty result.");
      return;
    }
    Stage.scheduleFetches(stage, fetches);
  }

  /**
   * This method merges the partition request associated with the pullserver's address.
   * It reduces the number of TCP connections.
   *
   * @return key: pullserver's address, value: a list of requests
   */
  private static Collection<FetchImpl> mergeShuffleRequest(int partitionId,
                                                          ShuffleType type,
                                                          List<IntermediateEntry> partitions) {
    // ebId + pullhost -> FetchImmpl
    Map<String, FetchImpl> mergedPartitions = new HashMap<>();

    for (IntermediateEntry partition : partitions) {
      String mergedKey = partition.getEbId().toString() + "," + partition.getPullHost();

      if (mergedPartitions.containsKey(mergedKey)) {
        FetchImpl fetch = mergedPartitions.get(mergedKey);
        fetch.addPart(partition.getTaskId(), partition.getAttemptId());
      } else {
        // In some cases like union each IntermediateEntry has different EBID.
        FetchImpl fetch = new FetchImpl(partition.getPullHost(), type, partition.getEbId(), partitionId);
        fetch.addPart(partition.getTaskId(), partition.getAttemptId());
        mergedPartitions.put(mergedKey, fetch);
      }
    }
    return mergedPartitions.values();
  }

  public static void scheduleFragmentsForNonLeafTasks(TaskSchedulerContext schedulerContext,
                                                      MasterPlan masterPlan, Stage stage, int maxNum)
      throws IOException {
    DataChannel channel = masterPlan.getIncomingChannels(stage.getBlock().getId()).get(0);
    if (channel.getShuffleType() == HASH_SHUFFLE
        || channel.getShuffleType() == SCATTERED_HASH_SHUFFLE) {
      scheduleHashShuffledFetches(schedulerContext, masterPlan, stage, channel, maxNum);
    } else if (channel.getShuffleType() == RANGE_SHUFFLE) {
      scheduleRangeShuffledFetches(schedulerContext, masterPlan, stage, channel, maxNum);
    } else {
      throw new TajoInternalError("Cannot support partition type");
    }
  }

  private static TableStats computeChildBlocksStats(QueryMasterTask.QueryMasterTaskContext context, MasterPlan masterPlan,
                                                    ExecutionBlockId parentBlockId) {
    List<TableStats> tableStatses = new ArrayList<>();
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(parentBlockId);
    for (ExecutionBlock childBlock : childBlocks) {
      Stage childStage = context.getStage(childBlock.getId());
      tableStatses.add(childStage.getResultStats());
    }
    return StatisticsUtil.aggregateTableStat(tableStatses);
  }

  public static void scheduleRangeShuffledFetches(TaskSchedulerContext schedulerContext, MasterPlan masterPlan,
                                                  Stage stage, DataChannel channel, int maxNum)
      throws IOException {
    ExecutionBlock execBlock = stage.getBlock();
    ScanNode scan = execBlock.getScanNodes()[0];

    ExecutionBlock sampleChildBlock = masterPlan.getChild(stage.getId(), 0);
    SortNode sortNode = PlannerUtil.findTopNode(sampleChildBlock.getPlan(), NodeType.SORT);
    SortSpec [] sortSpecs = sortNode.getSortKeys();
    Schema sortSchema = new Schema(channel.getShuffleKeys());

    TupleRange[] ranges;
    int determinedTaskNum;

    // calculate the number of maximum query ranges
    TableStats totalStat = computeChildBlocksStats(stage.getContext(), masterPlan, stage.getId());

    // If there is an empty table in inner join, it should return zero rows.
    if (totalStat.getNumBytes() == 0 && totalStat.getColumnStats().size() == 0) {
      return;
    }
    TupleRange mergedRange = TupleUtil.columnStatToRange(sortSpecs, sortSchema, totalStat.getColumnStats(), false);

    if (sortNode.getSortPurpose() == SortPurpose.STORAGE_SPECIFIED) {
      String dataFormat = PlannerUtil.getDataFormat(masterPlan.getLogicalPlan());
      CatalogService catalog = stage.getContext().getQueryMasterContext().getWorkerContext().getCatalog();
      LogicalRootNode rootNode = masterPlan.getLogicalPlan().getRootBlock().getRoot();
      TableDesc tableDesc = null;
      try {
        tableDesc = PlannerUtil.getTableDesc(catalog, rootNode.getChild());
      } catch (UndefinedTableException e) {
        throw new IOException("Can't get table meta data from catalog: " +
            PlannerUtil.getStoreTableName(masterPlan.getLogicalPlan()));
      }

      Tablespace space = TablespaceManager.getAnyByScheme(dataFormat).get();
      ranges = space.getInsertSortRanges(
          stage.getContext().getQueryContext(),
          tableDesc,
          sortNode.getInSchema(),
          sortSpecs,
          mergedRange);

      determinedTaskNum = ranges.length;
    } else {
      RangePartitionAlgorithm partitioner = new UniformRangePartition(mergedRange, sortSpecs);
      BigInteger card = partitioner.getTotalCardinality();

      // if the number of the range cardinality is less than the desired number of tasks,
      // we set the the number of tasks to the number of range cardinality.
      if (card.compareTo(BigInteger.valueOf(maxNum)) < 0) {
        LOG.info(stage.getId() + ", The range cardinality (" + card
            + ") is less then the desired number of tasks (" + maxNum + ")");
        determinedTaskNum = card.intValue();
      } else {
        determinedTaskNum = maxNum;
      }

      LOG.info(stage.getId() + ", Try to divide " + mergedRange + " into " + determinedTaskNum +
          " sub ranges (total units: " + determinedTaskNum + ")");
      ranges = partitioner.partition(determinedTaskNum);
      if (ranges == null) {
        throw new NullPointerException("ranges is null on " + stage.getId() + " stage.");
      }

      if (ranges.length == 0) {
        LOG.warn(stage.getId() + " no range infos.");
      }

      TupleUtil.setMaxRangeIfNull(sortSpecs, sortSchema, totalStat.getColumnStats(), ranges);
      if (LOG.isDebugEnabled()) {
        if (ranges != null) {
          for (TupleRange eachRange : ranges) {
            LOG.debug(stage.getId() + " range: " + eachRange.getStart() + " ~ " + eachRange.getEnd());
          }
        }
      }
    }

    // TODO - We should remove dummy fragment.
    FileFragment dummyFragment = new FileFragment(scan.getTableName(), new Path("/dummy"), 0, 0,
        new String[]{UNKNOWN_HOST});
    Stage.scheduleFragment(stage, dummyFragment);

    List<FetchImpl> fetches = new ArrayList<>();
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(stage.getId());
    for (ExecutionBlock childBlock : childBlocks) {
      Stage childExecSM = stage.getContext().getStage(childBlock.getId());
      for (Task qu : childExecSM.getTasks()) {
        for (IntermediateEntry p : qu.getIntermediateData()) {
          FetchImpl fetch = new FetchImpl(p.getPullHost(), RANGE_SHUFFLE, childBlock.getId(), 0);
          fetch.addPart(p.getTaskId(), p.getAttemptId());
          fetches.add(fetch);
        }
      }
    }

    SortedMap<TupleRange, Collection<FetchImpl>> map;
    map = new TreeMap<>();

    Set<FetchImpl> fetchSet;
    try {
      RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(sortSchema);
      for (int i = 0; i < ranges.length; i++) {
        fetchSet = new HashSet<>();
        for (FetchImpl fetch: fetches) {
          String rangeParam =
              TupleUtil.rangeToQuery(ranges[i], i == (ranges.length - 1) , encoder);
          FetchImpl copy = null;
          try {
            copy = fetch.clone();
          } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
          }
          copy.setRangeParams(rangeParam);
          fetchSet.add(copy);
        }
        map.put(ranges[i], fetchSet);
      }

    } catch (UnsupportedEncodingException e) {
      LOG.error(e);
    }

    scheduleFetchesByRoundRobin(stage, map, scan.getTableName(), determinedTaskNum);

    schedulerContext.setEstimatedTaskNum(determinedTaskNum);
  }

  public static void scheduleFetchesByRoundRobin(Stage stage, Map<?, Collection<FetchImpl>> partitions,
                                                   String tableName, int num) {
    int i;
    Map<String, List<FetchImpl>>[] fetchesArray = new Map[num];
    for (i = 0; i < num; i++) {
      fetchesArray[i] = new HashMap<>();
    }
    i = 0;
    for (Entry<?, Collection<FetchImpl>> entry : partitions.entrySet()) {
      Collection<FetchImpl> value = entry.getValue();
      TUtil.putCollectionToNestedList(fetchesArray[i++], tableName, value);
      if (i == num) i = 0;
    }
    for (Map<String, List<FetchImpl>> eachFetches : fetchesArray) {
      Stage.scheduleFetches(stage, eachFetches);
    }
  }

  @VisibleForTesting
  public static class FetchGroupMeta {
    long totalVolume;
    List<FetchImpl> fetchUrls;

    public FetchGroupMeta(long volume, FetchImpl fetchUrls) {
      this.totalVolume = volume;
      this.fetchUrls = Lists.newArrayList(fetchUrls);
    }

    public FetchGroupMeta addFetche(FetchImpl fetches) {
      this.fetchUrls.add(fetches);
      return this;
    }

    public void increaseVolume(long volume) {
      this.totalVolume += volume;
    }

    public long getVolume() {
      return totalVolume;
    }

  }

  public static void scheduleHashShuffledFetches(TaskSchedulerContext schedulerContext, MasterPlan masterPlan,
                                                 Stage stage, DataChannel channel,
                                                 int maxNum) throws IOException {
    ExecutionBlock execBlock = stage.getBlock();
    ScanNode scan = execBlock.getScanNodes()[0];

    // TODO - We should remove dummy fragment usages
    Fragment frag = new FileFragment(scan.getCanonicalName(), new Path("/dummy"), 0, 0, new String[]{UNKNOWN_HOST});
    List<Fragment> fragments = new ArrayList<>();
    fragments.add(frag);
    Stage.scheduleFragments(stage, fragments);

    Map<Integer, FetchGroupMeta> finalFetches = new HashMap<>();
    Map<ExecutionBlockId, List<IntermediateEntry>> intermediates = new HashMap<>();

    for (ExecutionBlock block : masterPlan.getChilds(execBlock)) {
      List<IntermediateEntry> partitions = new ArrayList<>();
      partitions.addAll(stage.getContext().getStage(block.getId()).getHashShuffleIntermediateEntries());

      // In scattered hash shuffle, Collecting each IntermediateEntry
      if (channel.getShuffleType() == SCATTERED_HASH_SHUFFLE) {
        if (intermediates.containsKey(block.getId())) {
          intermediates.get(block.getId()).addAll(partitions);
        } else {
          intermediates.put(block.getId(), partitions);
        }
      }

      // make FetchImpl per PullServer, PartId
      Map<Integer, List<IntermediateEntry>> hashed = hashByKey(partitions);
      for (Entry<Integer, List<IntermediateEntry>> interm : hashed.entrySet()) {
        Map<Task.PullHost, List<IntermediateEntry>> hashedByHost = hashByHost(interm.getValue());
        for (Entry<Task.PullHost, List<IntermediateEntry>> e : hashedByHost.entrySet()) {

          FetchImpl fetch = new FetchImpl(e.getKey(), channel.getShuffleType(),
              block.getId(), interm.getKey(), e.getValue());

          long volumeSum = 0;
          for (IntermediateEntry ie : e.getValue()) {
            volumeSum += ie.getVolume();
          }

          if (finalFetches.containsKey(interm.getKey())) {
            finalFetches.get(interm.getKey()).addFetche(fetch).increaseVolume(volumeSum);
          } else {
            finalFetches.put(interm.getKey(), new FetchGroupMeta(volumeSum, fetch));
          }
        }
      }
    }

    int groupingColumns = 0;
    LogicalNode[] groupbyNodes = PlannerUtil.findAllNodes(stage.getBlock().getPlan(),
        new NodeType[]{NodeType.GROUP_BY, NodeType.DISTINCT_GROUP_BY});
    if (groupbyNodes != null && groupbyNodes.length > 0) {
      LogicalNode bottomNode = groupbyNodes[0];
      if (bottomNode.getType() == NodeType.GROUP_BY) {
        groupingColumns = ((GroupbyNode)bottomNode).getGroupingColumns().length;
      } else if (bottomNode.getType() == NodeType.DISTINCT_GROUP_BY) {
        DistinctGroupbyNode distinctNode = PlannerUtil.findMostBottomNode(stage.getBlock().getPlan(), NodeType.DISTINCT_GROUP_BY);
        if (distinctNode == null) {
          LOG.warn(stage.getId() + ", Can't find current DistinctGroupbyNode");
          distinctNode = (DistinctGroupbyNode)bottomNode;
        }
        groupingColumns = distinctNode.getGroupingColumns().length;

        Enforcer enforcer = execBlock.getEnforcer();
        EnforceProperty property = PhysicalPlannerImpl.getAlgorithmEnforceProperty(enforcer, distinctNode);
        if (property != null) {
          if (property.getDistinct().getIsMultipleAggregation()) {
            MultipleAggregationStage mulAggStage = property.getDistinct().getMultipleAggregationStage();
            if (mulAggStage != MultipleAggregationStage.THRID_STAGE) {
              groupingColumns = distinctNode.getOutSchema().size();
            }
          }
        }
      }
    }
    // get a proper number of tasks
    int determinedTaskNum = Math.min(maxNum, finalFetches.size());
    LOG.info(stage.getId() + ", ScheduleHashShuffledFetches - Max num=" + maxNum + ", finalFetchURI=" + finalFetches.size());

    if (groupingColumns == 0) {
      determinedTaskNum = 1;
      LOG.info(stage.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
    } else {
      TableStats totalStat = computeChildBlocksStats(stage.getContext(), masterPlan, stage.getId());
      if (totalStat.getNumRows() == 0) {
        determinedTaskNum = 1;
      }
    }

    // set the proper number of tasks to the estimated task num
    if (channel.getShuffleType() == SCATTERED_HASH_SHUFFLE) {
      scheduleScatteredHashShuffleFetches(schedulerContext, stage, intermediates,
          scan.getTableName());
    } else {
      schedulerContext.setEstimatedTaskNum(determinedTaskNum);
      // divide fetch uris into the the proper number of tasks according to volumes
      scheduleFetchesByEvenDistributedVolumes(stage, finalFetches, scan.getTableName(), determinedTaskNum);
      LOG.info(stage.getId() + ", DeterminedTaskNum : " + determinedTaskNum);
    }
  }

  public static Pair<Long [], Map<String, List<FetchImpl>>[]> makeEvenDistributedFetchImpl(
      Map<Integer, FetchGroupMeta> partitions, String tableName, int num) {

    // Sort fetchGroupMeta in a descending order of data volumes.
    List<FetchGroupMeta> fetchGroupMetaList = Lists.newArrayList(partitions.values());
    Collections.sort(fetchGroupMetaList, new Comparator<FetchGroupMeta>() {
      @Override
      public int compare(FetchGroupMeta o1, FetchGroupMeta o2) {
        return o1.getVolume() < o2.getVolume() ? 1 : (o1.getVolume() > o2.getVolume() ? -1 : 0);
      }
    });

    // Initialize containers
    Map<String, List<FetchImpl>>[] fetchesArray = new Map[num];
    Long [] assignedVolumes = new Long[num];
    // initialization
    for (int i = 0; i < num; i++) {
      fetchesArray[i] = new HashMap<>();
      assignedVolumes[i] = 0l;
    }

    // This algorithm assignes bigger first manner by using a sorted iterator. It is a kind of greedy manner.
    // Its complexity is O(n). Since FetchGroup can be more than tens of thousands, we should consider its complexity.
    // In terms of this point, it will show reasonable performance and results. even though it is not an optimal
    // algorithm.
    Iterator<FetchGroupMeta> iterator = fetchGroupMetaList.iterator();

    int p;
    while(iterator.hasNext()) {
      p = 0;
      while (p < num && iterator.hasNext()) {
        FetchGroupMeta fetchGroupMeta = iterator.next();
        assignedVolumes[p] += fetchGroupMeta.getVolume();

        TUtil.putCollectionToNestedList(fetchesArray[p], tableName, fetchGroupMeta.fetchUrls);
        p++;
      }

      p = num - 1;
      while (p >= 0 && iterator.hasNext()) {
        FetchGroupMeta fetchGroupMeta = iterator.next();
        assignedVolumes[p] += fetchGroupMeta.getVolume();
        TUtil.putCollectionToNestedList(fetchesArray[p], tableName, fetchGroupMeta.fetchUrls);

        // While the current one is smaller than next one, it adds additional fetches to current one.
        while(iterator.hasNext() && (p > 0 && assignedVolumes[p - 1] > assignedVolumes[p])) {
          FetchGroupMeta additionalFetchGroup = iterator.next();
          assignedVolumes[p] += additionalFetchGroup.getVolume();
          TUtil.putCollectionToNestedList(fetchesArray[p], tableName, additionalFetchGroup.fetchUrls);
        }

        p--;
      }
    }

    return new Pair<>(assignedVolumes, fetchesArray);
  }

  public static void scheduleFetchesByEvenDistributedVolumes(Stage stage, Map<Integer, FetchGroupMeta> partitions,
                                                             String tableName, int num) {
    Map<String, List<FetchImpl>>[] fetchsArray = makeEvenDistributedFetchImpl(partitions, tableName, num).getSecond();
    // Schedule FetchImpls
    for (Map<String, List<FetchImpl>> eachFetches : fetchsArray) {
      Stage.scheduleFetches(stage, eachFetches);
    }
  }

  // Scattered hash shuffle hashes the key columns and groups the hash keys associated with
  // the same hash key. Then, if the volume of a group is larger
  // than $DIST_QUERY_TABLE_PARTITION_VOLUME, it divides the group into more than two sub groups
  // according to $DIST_QUERY_TABLE_PARTITION_VOLUME (default size = 256MB).
  // As a result, each group size always becomes the less than or equal
  // to $DIST_QUERY_TABLE_PARTITION_VOLUME. Finally, each subgroup is assigned to a query unit.
  // It is usually used for writing partitioned tables.
  public static void scheduleScatteredHashShuffleFetches(TaskSchedulerContext schedulerContext,
       Stage stage, Map<ExecutionBlockId, List<IntermediateEntry>> intermediates,
       String tableName) {
    long splitVolume = (long)StorageUnit.MB *
        stage.getMasterPlan().getContext().getInt(SessionVars.TABLE_PARTITION_PER_SHUFFLE_SIZE);
    long pageSize = ((long)StorageUnit.MB) *
        stage.getContext().getConf().getIntVar(ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME); // in bytes
    if (pageSize >= splitVolume) {
      throw new RuntimeException("tajo.dist-query.table-partition.task-volume-mb should be great than " +
          "tajo.shuffle.hash.appender.page.volumn-mb");
    }
    List<List<FetchImpl>> fetches = new ArrayList<>();

    long totalIntermediateSize = 0L;
    for (Entry<ExecutionBlockId, List<IntermediateEntry>> listEntry : intermediates.entrySet()) {
      // merge by PartitionId
      Map<Integer, List<IntermediateEntry>> partitionIntermMap = new HashMap<>();
      for (IntermediateEntry eachInterm: listEntry.getValue()) {
        totalIntermediateSize += eachInterm.getVolume();
        int partId = eachInterm.getPartId();
        List<IntermediateEntry> partitionInterms = partitionIntermMap.get(partId);
        if (partitionInterms == null) {
          partitionInterms = TUtil.newList(eachInterm);
          partitionIntermMap.put(partId, partitionInterms);
        } else {
          partitionInterms.add(eachInterm);
        }
      }

      // Grouping or splitting to fit $DIST_QUERY_TABLE_PARTITION_VOLUME size
      for (List<IntermediateEntry> partitionEntries : partitionIntermMap.values()) {
        List<List<FetchImpl>> eachFetches = splitOrMergeIntermediates(listEntry.getKey(), partitionEntries,
            splitVolume, pageSize);
        if (eachFetches != null && !eachFetches.isEmpty()) {
          fetches.addAll(eachFetches);
        }
      }
    }

    schedulerContext.setEstimatedTaskNum(fetches.size());

    int i = 0;
    Map<String, List<FetchImpl>>[] fetchesArray = new Map[fetches.size()];
    for(List<FetchImpl> entry : fetches) {
      fetchesArray[i] = new HashMap<>();
      fetchesArray[i].put(tableName, entry);

      Stage.scheduleFetches(stage, fetchesArray[i]);
      i++;
    }

    LOG.info(stage.getId()
        + ", ShuffleType:" + SCATTERED_HASH_SHUFFLE.name()
        + ", Intermediate Size: " + totalIntermediateSize
        + ", splitSize: " + splitVolume
        + ", DeterminedTaskNum: " + fetches.size());
  }

  /**
   * If a IntermediateEntry is large than splitVolume, List<FetchImpl> has single element.
   * @param ebId
   * @param entries
   * @param splitVolume
   * @return
   */
  public static List<List<FetchImpl>> splitOrMergeIntermediates(
      ExecutionBlockId ebId, List<IntermediateEntry> entries, long splitVolume, long pageSize) {
    // Each List<FetchImpl> has splitVolume size.
    List<List<FetchImpl>> fetches = new ArrayList<>();

    Iterator<IntermediateEntry> iter = entries.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    List<FetchImpl> fetchListForSingleTask = new ArrayList<>();
    long fetchListVolume = 0;

    while (iter.hasNext()) {
      IntermediateEntry currentInterm = iter.next();

      long firstSplitVolume = splitVolume - fetchListVolume;
      if (firstSplitVolume < pageSize) {
        firstSplitVolume = splitVolume;
      }

      //Each Pair object in the splits variable is assigned to the next ExectionBlock's task.
      //The first long value is a offset of the intermediate file and the second long value is length.
      List<Pair<Long, Long>> splits = currentInterm.split(firstSplitVolume, splitVolume);
      if (splits == null || splits.isEmpty()) {
        break;
      }

      for (Pair<Long, Long> eachSplit: splits) {
        if (fetchListVolume > 0 && fetchListVolume + eachSplit.getSecond() >= splitVolume) {
          if (!fetchListForSingleTask.isEmpty()) {
            fetches.add(fetchListForSingleTask);
          }
          fetchListForSingleTask = new ArrayList<>();
          fetchListVolume = 0;
        }
        FetchImpl fetch = new FetchImpl(currentInterm.getPullHost(), SCATTERED_HASH_SHUFFLE,
            ebId, currentInterm.getPartId(), TUtil.newList(currentInterm));
        fetch.setOffset(eachSplit.getFirst());
        fetch.setLength(eachSplit.getSecond());
        fetchListForSingleTask.add(fetch);
        fetchListVolume += eachSplit.getSecond();
      }
    }
    if (!fetchListForSingleTask.isEmpty()) {
      fetches.add(fetchListForSingleTask);
    }
    return fetches;
  }

  public static List<URI> createFetchURL(FetchImpl fetch, boolean includeParts) {
    String scheme = "http://";

    StringBuilder urlPrefix = new StringBuilder(scheme);
    urlPrefix.append(fetch.getPullHost().getHost()).append(":").append(fetch.getPullHost().getPort()).append("/?")
        .append("qid=").append(fetch.getExecutionBlockId().getQueryId().toString())
        .append("&sid=").append(fetch.getExecutionBlockId().getId())
        .append("&p=").append(fetch.getPartitionId())
        .append("&type=");
    if (fetch.getType() == HASH_SHUFFLE) {
      urlPrefix.append("h");
    } else if (fetch.getType() == RANGE_SHUFFLE) {
      urlPrefix.append("r").append("&").append(fetch.getRangeParams());
    } else if (fetch.getType() == SCATTERED_HASH_SHUFFLE) {
      urlPrefix.append("s");
    }

    if (fetch.getLength() >= 0) {
      urlPrefix.append("&offset=").append(fetch.getOffset()).append("&length=").append(fetch.getLength());
    }

    List<URI> fetchURLs = new ArrayList<>();
    if(includeParts) {
      if (fetch.getType() == HASH_SHUFFLE || fetch.getType() == SCATTERED_HASH_SHUFFLE) {
        fetchURLs.add(URI.create(urlPrefix.toString()));
      } else {
        // If the get request is longer than 2000 characters,
        // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
        // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
        // The below code transforms a long request to multiple requests.
        List<String> taskIdsParams = new ArrayList<>();
        StringBuilder taskIdListBuilder = new StringBuilder();
        List<Integer> taskIds = fetch.getTaskIds();
        List<Integer> attemptIds = fetch.getAttemptIds();
        boolean first = true;

        for (int i = 0; i < taskIds.size(); i++) {
          StringBuilder taskAttemptId = new StringBuilder();

          if (!first) { // when comma is added?
            taskAttemptId.append(",");
          } else {
            first = false;
          }

          int taskId = taskIds.get(i);
          if (taskId < 0) {
            // In the case of hash shuffle each partition has single shuffle file per worker.
            // TODO If file is large, consider multiple fetching(shuffle file can be split)
            continue;
          }
          int attemptId = attemptIds.get(i);
          taskAttemptId.append(taskId).append("_").append(attemptId);

          if (taskIdListBuilder.length() + taskAttemptId.length()
              > HTTP_REQUEST_MAXIMUM_LENGTH) {
            taskIdsParams.add(taskIdListBuilder.toString());
            taskIdListBuilder = new StringBuilder(taskId + "_" + attemptId);
          } else {
            taskIdListBuilder.append(taskAttemptId);
          }
        }
        // if the url params remain
        if (taskIdListBuilder.length() > 0) {
          taskIdsParams.add(taskIdListBuilder.toString());
        }
        urlPrefix.append("&ta=");
        for (String param : taskIdsParams) {
          fetchURLs.add(URI.create(urlPrefix + param));
        }
      }
    } else {
      fetchURLs.add(URI.create(urlPrefix.toString()));
    }

    return fetchURLs;
  }

  public static Map<Integer, List<IntermediateEntry>> hashByKey(List<IntermediateEntry> entries) {
    Map<Integer, List<IntermediateEntry>> hashed = new HashMap<>();
    for (IntermediateEntry entry : entries) {
      if (hashed.containsKey(entry.getPartId())) {
        hashed.get(entry.getPartId()).add(entry);
      } else {
        hashed.put(entry.getPartId(), TUtil.newList(entry));
      }
    }

    return hashed;
  }

  public static Map<Task.PullHost, List<IntermediateEntry>> hashByHost(List<IntermediateEntry> entries) {
    Map<Task.PullHost, List<IntermediateEntry>> hashed = new HashMap<>();

    Task.PullHost host;
    for (IntermediateEntry entry : entries) {
      host = entry.getPullHost();
      if (hashed.containsKey(host)) {
        hashed.get(host).add(entry);
      } else {
        hashed.put(host, TUtil.newList(entry));
      }
    }

    return hashed;
  }

  public static Stage setShuffleOutputNumForTwoPhase(Stage stage, final int desiredNum, DataChannel channel) {
    ExecutionBlock execBlock = stage.getBlock();
    Column[] keys;
    // if the next query is join,
    // set the partition number for the current logicalUnit
    // TODO: the union handling is required when a join has unions as its child
    MasterPlan masterPlan = stage.getMasterPlan();
    keys = channel.getShuffleKeys();
    if (!masterPlan.isRoot(stage.getBlock()) ) {
      ExecutionBlock parentBlock = masterPlan.getParent(stage.getBlock());
      if (parentBlock.getPlan().getType() == NodeType.JOIN) {
        channel.setShuffleOutputNum(desiredNum);
      }
    }

    // set the partition number for group by and sort
    if (channel.getShuffleType() == HASH_SHUFFLE) {
      if (execBlock.getPlan().getType() == NodeType.GROUP_BY ||
          execBlock.getPlan().getType() == NodeType.DISTINCT_GROUP_BY) {
        keys = channel.getShuffleKeys();
      }
    } else if (channel.getShuffleType() == RANGE_SHUFFLE) {
      if (execBlock.getPlan().getType() == NodeType.SORT) {
        SortNode sort = (SortNode) execBlock.getPlan();
        keys = new Column[sort.getSortKeys().length];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = sort.getSortKeys()[i].getSortKey();
        }
      }
    }
    if (keys != null) {
      if (keys.length == 0) {
        channel.setShuffleKeys(new Column[]{});
        channel.setShuffleOutputNum(1);
      } else {
        channel.setShuffleKeys(keys);
        channel.setShuffleOutputNum(desiredNum);
      }
    }
    return stage;
  }
}
