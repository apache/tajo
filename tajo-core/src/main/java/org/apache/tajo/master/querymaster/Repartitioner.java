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

package org.apache.tajo.master.querymaster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty;
import org.apache.tajo.master.TaskSchedulerContext;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.Pair;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.worker.FetchImpl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.*;

/**
 * Repartitioner creates non-leaf tasks and shuffles intermediate data.
 * It supports two repartition methods, such as hash and range repartition.
 */
public class Repartitioner {
  private static final Log LOG = LogFactory.getLog(Repartitioner.class);

  private final static int HTTP_REQUEST_MAXIMUM_LENGTH = 1900;
  private final static String UNKNOWN_HOST = "unknown";

  public static void scheduleFragmentsForJoinQuery(TaskSchedulerContext schedulerContext, SubQuery subQuery)
      throws IOException {
    MasterPlan masterPlan = subQuery.getMasterPlan();
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryMasterTask.QueryMasterTaskContext masterContext = subQuery.getContext();
    AbstractStorageManager storageManager = subQuery.getStorageManager();

    ScanNode[] scans = execBlock.getScanNodes();

    Path tablePath;
    FileFragment[] fragments = new FileFragment[scans.length];
    long[] stats = new long[scans.length];

    // initialize variables from the child operators
    for (int i = 0; i < scans.length; i++) {
      TableDesc tableDesc = masterContext.getTableDescMap().get(scans[i].getCanonicalName());
      if (tableDesc == null) { // if it is a real table stored on storage
        tablePath = storageManager.getTablePath(scans[i].getTableName());
        if (execBlock.getUnionScanMap() != null && !execBlock.getUnionScanMap().isEmpty()) {
          for (Map.Entry<ExecutionBlockId, ExecutionBlockId> unionScanEntry: execBlock.getUnionScanMap().entrySet()) {
            ExecutionBlockId originScanEbId = unionScanEntry.getKey();
            stats[i] += masterContext.getSubQuery(originScanEbId).getResultStats().getNumBytes();
          }
        } else {
          ExecutionBlockId scanEBId = TajoIdUtils.createExecutionBlockId(scans[i].getTableName());
          stats[i] = masterContext.getSubQuery(scanEBId).getResultStats().getNumBytes();
        }
        fragments[i] = new FileFragment(scans[i].getCanonicalName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
      } else {
        tablePath = tableDesc.getPath();
        try {
          stats[i] = GlobalPlanner.computeDescendentVolume(scans[i]);
        } catch (PlanningException e) {
          throw new IOException(e);
        }

        // if table has no data, storageManager will return empty FileFragment.
        // So, we need to handle FileFragment by its size.
        // If we don't check its size, it can cause IndexOutOfBoundsException.
        List<FileFragment> fileFragments = storageManager.getSplits(scans[i].getCanonicalName(), tableDesc.getMeta(), tableDesc.getSchema(), tablePath);
        if (fileFragments.size() > 0) {
          fragments[i] = fileFragments.get(0);
        } else {
          fragments[i] = new FileFragment(scans[i].getCanonicalName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
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
    boolean isAllBroadcastTable = true;
    for (int i = 0; i < scans.length; i++) {
      if (!execBlock.isBroadcastTable(scans[i].getCanonicalName())) {
        isAllBroadcastTable = false;
        break;
      }
    }


    if (isAllBroadcastTable) { // if all relations of this EB are broadcasted
      // set largest table to normal mode
      long maxStats = Long.MIN_VALUE;
      int maxStatsScanIdx = -1;
      for (int i = 0; i < scans.length; i++) {
        // finding largest table.
        // If stats == 0, can't be base table.
        if (stats[i] > 0 && stats[i] > maxStats) {
          maxStats = stats[i];
          maxStatsScanIdx = i;
        }
      }
      if (maxStatsScanIdx == -1) {
        maxStatsScanIdx = 0;
      }
      int baseScanIdx = maxStatsScanIdx;
      scans[baseScanIdx].setBroadcastTable(false);
      execBlock.removeBroadcastTable(scans[baseScanIdx].getCanonicalName());
      LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join with all tables, base_table=%s, base_volume=%d",
          scans[baseScanIdx].getCanonicalName(), stats[baseScanIdx]));
      scheduleLeafTasksWithBroadcastTable(schedulerContext, subQuery, baseScanIdx, fragments);
    } else if (!execBlock.getBroadcastTables().isEmpty()) { // If some relations of this EB are broadcasted
      boolean hasNonLeafNode = false;
      List<Integer> largeScanIndexList = new ArrayList<Integer>();
      List<Integer> broadcastIndexList = new ArrayList<Integer>();
      String nonLeafScanNames = "";
      String namePrefix = "";
      long maxStats = Long.MIN_VALUE;
      int maxStatsScanIdx = -1;
      for (int i = 0; i < scans.length; i++) {
        if (scans[i].getTableDesc().getMeta().getStoreType() == StoreType.RAW) {
          // Intermediate data scan
          hasNonLeafNode = true;
          largeScanIndexList.add(i);
          nonLeafScanNames += namePrefix + scans[i].getCanonicalName();
          namePrefix = ",";
        }
        if (execBlock.isBroadcastTable(scans[i].getCanonicalName())) {
          broadcastIndexList.add(i);
        } else {
          // finding largest table.
          if (stats[i] > 0 && stats[i] > maxStats) {
            maxStats = stats[i];
            maxStatsScanIdx = i;
          }
        }
      }
      if (maxStatsScanIdx == -1) {
        maxStatsScanIdx = 0;
      }

      if (!hasNonLeafNode) {
        if (largeScanIndexList.size() > 1) {
          String largeTableNames = "";
          for (Integer eachId : largeScanIndexList) {
            largeTableNames += scans[eachId].getTableName() + ",";
          }
          throw new IOException("Broadcast join with leaf node should have only one large table, " +
              "but " + largeScanIndexList.size() + ", tables=" + largeTableNames);
        }
        int baseScanIdx = largeScanIndexList.isEmpty() ? maxStatsScanIdx : largeScanIndexList.get(0);
        LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join, base_table=%s, base_volume=%d",
            scans[baseScanIdx].getCanonicalName(), stats[baseScanIdx]));
        scheduleLeafTasksWithBroadcastTable(schedulerContext, subQuery, baseScanIdx, fragments);
      } else {
        if (largeScanIndexList.size() > 2) {
          throw new IOException("Symmetric Repartition Join should have two scan node, but " + nonLeafScanNames);
        }

        //select intermediate scan and stats
        ScanNode[] intermediateScans = new ScanNode[largeScanIndexList.size()];
        long[] intermediateScanStats = new long[largeScanIndexList.size()];
        FileFragment[] intermediateFragments = new FileFragment[largeScanIndexList.size()];
        int index = 0;
        for (Integer eachIdx : largeScanIndexList) {
          intermediateScans[index] = scans[eachIdx];
          intermediateScanStats[index] = stats[eachIdx];
          intermediateFragments[index++] = fragments[eachIdx];
        }
        FileFragment[] broadcastFragments = new FileFragment[broadcastIndexList.size()];
        index = 0;
        for (Integer eachIdx : broadcastIndexList) {
          scans[eachIdx].setBroadcastTable(true);
          broadcastFragments[index++] = fragments[eachIdx];
        }
        LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join, join_node=%s", nonLeafScanNames));
        scheduleSymmetricRepartitionJoin(masterContext, schedulerContext, subQuery,
            intermediateScans, intermediateScanStats, intermediateFragments, broadcastFragments);
      }
    } else {
      LOG.info("[Distributed Join Strategy] : Symmetric Repartition Join");
      scheduleSymmetricRepartitionJoin(masterContext, schedulerContext, subQuery, scans, stats, fragments, null);
    }
  }

  /**
   * Scheduling in tech case of Symmetric Repartition Join
   * @param masterContext
   * @param schedulerContext
   * @param subQuery
   * @param scans
   * @param stats
   * @param fragments
   * @throws IOException
   */
  private static void scheduleSymmetricRepartitionJoin(QueryMasterTask.QueryMasterTaskContext masterContext,
                                                       TaskSchedulerContext schedulerContext,
                                                       SubQuery subQuery,
                                                       ScanNode[] scans,
                                                       long[] stats,
                                                       FileFragment[] fragments,
                                                       FileFragment[] broadcastFragments) throws IOException {
    MasterPlan masterPlan = subQuery.getMasterPlan();
    ExecutionBlock execBlock = subQuery.getBlock();
    // The hash map is modeling as follows:
    // <Part Id, <EbId, List<Intermediate Data>>>
    Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> hashEntries =
        new HashMap<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>>();

    // Grouping IntermediateData by a partition key and a table name
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(subQuery.getId());

    // In the case of join with union, there is one ScanNode for union.
    Map<ExecutionBlockId, ExecutionBlockId> unionScanMap = execBlock.getUnionScanMap();
    for (ExecutionBlock childBlock : childBlocks) {
      ExecutionBlockId scanEbId = unionScanMap.get(childBlock.getId());
      if (scanEbId == null) {
        scanEbId = childBlock.getId();
      }
      SubQuery childExecSM = subQuery.getContext().getSubQuery(childBlock.getId());

      if (childExecSM.getHashShuffleIntermediateEntries() != null &&
          !childExecSM.getHashShuffleIntermediateEntries().isEmpty()) {
        for (IntermediateEntry intermEntry: childExecSM.getHashShuffleIntermediateEntries()) {
          intermEntry.setEbId(childBlock.getId());
          if (hashEntries.containsKey(intermEntry.getPartId())) {
            Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
                hashEntries.get(intermEntry.getPartId());

            if (tbNameToInterm.containsKey(scanEbId)) {
              tbNameToInterm.get(scanEbId).add(intermEntry);
            } else {
              tbNameToInterm.put(scanEbId, TUtil.newList(intermEntry));
            }
          } else {
            Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
                new HashMap<ExecutionBlockId, List<IntermediateEntry>>();
            tbNameToInterm.put(scanEbId, TUtil.newList(intermEntry));
            hashEntries.put(intermEntry.getPartId(), tbNameToInterm);
          }
        }
      } else {
        //if no intermidatedata(empty table), make empty entry
        int emptyPartitionId = 0;
        if (hashEntries.containsKey(emptyPartitionId)) {
          Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm = hashEntries.get(emptyPartitionId);
          if (tbNameToInterm.containsKey(scanEbId))
            tbNameToInterm.get(scanEbId).addAll(new ArrayList<IntermediateEntry>());
          else
            tbNameToInterm.put(scanEbId, new ArrayList<IntermediateEntry>());
        } else {
          Map<ExecutionBlockId, List<IntermediateEntry>> tbNameToInterm =
              new HashMap<ExecutionBlockId, List<IntermediateEntry>>();
          tbNameToInterm.put(scanEbId, new ArrayList<IntermediateEntry>());
          hashEntries.put(emptyPartitionId, tbNameToInterm);
        }
      }
    }

    // hashEntries can be zero if there are no input data.
    // In the case, it will cause the zero divided exception.
    // it avoids this problem.
    int[] avgSize = new int[2];
    avgSize[0] = hashEntries.size() == 0 ? 0 : (int) (stats[0] / hashEntries.size());
    avgSize[1] = hashEntries.size() == 0 ? 0 : (int) (stats[1] / hashEntries.size());
    int bothFetchSize = avgSize[0] + avgSize[1];

    // Getting the desire number of join tasks according to the volumn
    // of a larger table
    int largerIdx = stats[0] >= stats[1] ? 0 : 1;
    int desireJoinTaskVolumn = subQuery.getMasterPlan().getContext().getInt(SessionVars.JOIN_TASK_INPUT_SIZE);

    // calculate the number of tasks according to the data size
    int mb = (int) Math.ceil((double) stats[largerIdx] / 1048576);
    LOG.info("Larger intermediate data is approximately " + mb + " MB");
    // determine the number of task per 64MB
    int maxTaskNum = (int) Math.ceil((double) mb / desireJoinTaskVolumn);
    LOG.info("The calculated number of tasks is " + maxTaskNum);
    LOG.info("The number of total shuffle keys is " + hashEntries.size());
    // the number of join tasks cannot be larger than the number of
    // distinct partition ids.
    int joinTaskNum = Math.min(maxTaskNum, hashEntries.size());
    LOG.info("The determined number of join tasks is " + joinTaskNum);

    FileFragment[] rightFragments = new FileFragment[1 + (broadcastFragments == null ? 0 : broadcastFragments.length)];
    rightFragments[0] = fragments[1];
    if (broadcastFragments != null) {
      System.arraycopy(broadcastFragments, 0, rightFragments, 1, broadcastFragments.length);
    }
    SubQuery.scheduleFragment(subQuery, fragments[0], Arrays.asList(rightFragments));

    // Assign partitions to tasks in a round robin manner.
    for (Entry<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> entry
        : hashEntries.entrySet()) {
      addJoinShuffle(subQuery, entry.getKey(), entry.getValue());
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
        new HashMap<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>>();

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
        Map<String, IntermediateEntry> ebMerged = new HashMap<String, IntermediateEntry>();

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

        List<IntermediateEntry> ebIntermediateEntries = new ArrayList<IntermediateEntry>(ebMerged.values());

        Map<ExecutionBlockId, List<IntermediateEntry>> mergedPartEntries = mergedHashEntries.get(partId);
        if (mergedPartEntries == null) {
          mergedPartEntries = new HashMap<ExecutionBlockId, List<IntermediateEntry>>();
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
  public static List<FileFragment> getFragmentsFromPartitionedTable(AbstractStorageManager sm,
                                                                          ScanNode scan,
                                                                          TableDesc table) throws IOException {
    List<FileFragment> fragments = Lists.newArrayList();
    PartitionedTableScanNode partitionsScan = (PartitionedTableScanNode) scan;
    fragments.addAll(sm.getSplits(
        scan.getCanonicalName(), table.getMeta(), table.getSchema(), partitionsScan.getInputPaths()));
    partitionsScan.setInputPaths(null);
    return fragments;
  }

  private static void scheduleLeafTasksWithBroadcastTable(TaskSchedulerContext schedulerContext, SubQuery subQuery,
                                                          int baseScanId, FileFragment[] fragments) throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
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
    Collection<FileFragment> baseFragments = null;
    List<FileFragment> broadcastFragments = new ArrayList<FileFragment>();
    for (int i = 0; i < scans.length; i++) {
      ScanNode scan = scans[i];
      TableDesc desc = subQuery.getContext().getTableDescMap().get(scan.getCanonicalName());
      TableMeta meta = desc.getMeta();

      Collection<FileFragment> scanFragments;
      Path[] partitionScanPaths = null;
      if (scan.getType() == NodeType.PARTITIONS_SCAN) {
        PartitionedTableScanNode partitionScan = (PartitionedTableScanNode)scan;
        partitionScanPaths = partitionScan.getInputPaths();
        // set null to inputPaths in getFragmentsFromPartitionedTable()
        scanFragments = getFragmentsFromPartitionedTable(subQuery.getStorageManager(), scan, desc);
      } else {
        scanFragments = subQuery.getStorageManager().getSplits(scan.getCanonicalName(), meta, desc.getSchema(),
            desc.getPath());
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

    SubQuery.scheduleFragments(subQuery, baseFragments, broadcastFragments);
    schedulerContext.setEstimatedTaskNum(baseFragments.size());
  }

  private static void addJoinShuffle(SubQuery subQuery, int partitionId,
                                     Map<ExecutionBlockId, List<IntermediateEntry>> grouppedPartitions) {
    Map<String, List<FetchImpl>> fetches = new HashMap<String, List<FetchImpl>>();
    for (ExecutionBlock execBlock : subQuery.getMasterPlan().getChilds(subQuery.getId())) {
      if (grouppedPartitions.containsKey(execBlock.getId())) {
        Collection<FetchImpl> requests = mergeShuffleRequest(partitionId, HASH_SHUFFLE,
            grouppedPartitions.get(execBlock.getId()));
        fetches.put(execBlock.getId().toString(), Lists.newArrayList(requests));
      }
    }

    if (fetches.isEmpty()) {
      LOG.info(subQuery.getId() + "'s " + partitionId + " partition has empty result.");
      return;
    }
    SubQuery.scheduleFetches(subQuery, fetches);
  }

  /**
   * This method merges the partition request associated with the pullserver's address.
   * It reduces the number of TCP connections.
   *
   * @return key: pullserver's address, value: a list of requests
   */
  private static Collection<FetchImpl> mergeShuffleRequest(int partitionId,
                                                          TajoWorkerProtocol.ShuffleType type,
                                                          List<IntermediateEntry> partitions) {
    // ebId + pullhost -> FetchImmpl
    Map<String, FetchImpl> mergedPartitions = new HashMap<String, FetchImpl>();

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
                                                      MasterPlan masterPlan, SubQuery subQuery, int maxNum)
      throws IOException {
    DataChannel channel = masterPlan.getIncomingChannels(subQuery.getBlock().getId()).get(0);
    if (channel.getShuffleType() == HASH_SHUFFLE
        || channel.getShuffleType() == SCATTERED_HASH_SHUFFLE) {
      scheduleHashShuffledFetches(schedulerContext, masterPlan, subQuery, channel, maxNum);
    } else if (channel.getShuffleType() == RANGE_SHUFFLE) {
      scheduleRangeShuffledFetches(schedulerContext, masterPlan, subQuery, channel, maxNum);
    } else {
      throw new InternalException("Cannot support partition type");
    }
  }

  private static TableStats computeChildBlocksStats(QueryMasterTask.QueryMasterTaskContext context, MasterPlan masterPlan,
                                                    ExecutionBlockId parentBlockId) {
    List<TableStats> tableStatses = new ArrayList<TableStats>();
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(parentBlockId);
    for (ExecutionBlock childBlock : childBlocks) {
      SubQuery childExecSM = context.getSubQuery(childBlock.getId());
      tableStatses.add(childExecSM.getResultStats());
    }
    return StatisticsUtil.aggregateTableStat(tableStatses);
  }

  public static void scheduleRangeShuffledFetches(TaskSchedulerContext schedulerContext, MasterPlan masterPlan,
                                                  SubQuery subQuery, DataChannel channel, int maxNum)
      throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
    ScanNode scan = execBlock.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.getContext().getStorageManager().getTablePath(scan.getTableName());

    ExecutionBlock sampleChildBlock = masterPlan.getChild(subQuery.getId(), 0);
    SortNode sortNode = PlannerUtil.findTopNode(sampleChildBlock.getPlan(), NodeType.SORT);
    SortSpec [] sortSpecs = sortNode.getSortKeys();
    Schema sortSchema = new Schema(channel.getShuffleKeys());

    // calculate the number of maximum query ranges
    TableStats totalStat = computeChildBlocksStats(subQuery.getContext(), masterPlan, subQuery.getId());

    // If there is an empty table in inner join, it should return zero rows.
    if (totalStat.getNumBytes() == 0 && totalStat.getColumnStats().size() == 0 ) {
      return;
    }
    TupleRange mergedRange = TupleUtil.columnStatToRange(sortSpecs, sortSchema, totalStat.getColumnStats(), false);
    RangePartitionAlgorithm partitioner = new UniformRangePartition(mergedRange, sortSpecs);
    BigInteger card = partitioner.getTotalCardinality();

    // if the number of the range cardinality is less than the desired number of tasks,
    // we set the the number of tasks to the number of range cardinality.
    int determinedTaskNum;
    if (card.compareTo(BigInteger.valueOf(maxNum)) < 0) {
      LOG.info(subQuery.getId() + ", The range cardinality (" + card
          + ") is less then the desired number of tasks (" + maxNum + ")");
      determinedTaskNum = card.intValue();
    } else {
      determinedTaskNum = maxNum;
    }

    LOG.info(subQuery.getId() + ", Try to divide " + mergedRange + " into " + determinedTaskNum +
        " sub ranges (total units: " + determinedTaskNum + ")");
    TupleRange [] ranges = partitioner.partition(determinedTaskNum);
    if (ranges == null || ranges.length == 0) {
      LOG.warn(subQuery.getId() + " no range infos.");
    }
    TupleUtil.setMaxRangeIfNull(sortSpecs, sortSchema, totalStat.getColumnStats(), ranges);
    if (LOG.isDebugEnabled()) {
      if (ranges != null) {
        for (TupleRange eachRange : ranges) {
          LOG.debug(subQuery.getId() + " range: " + eachRange.getStart() + " ~ " + eachRange.getEnd());
        }
      }
    }

    FileFragment dummyFragment = new FileFragment(scan.getTableName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
    SubQuery.scheduleFragment(subQuery, dummyFragment);

    List<FetchImpl> fetches = new ArrayList<FetchImpl>();
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(subQuery.getId());
    for (ExecutionBlock childBlock : childBlocks) {
      SubQuery childExecSM = subQuery.getContext().getSubQuery(childBlock.getId());
      for (QueryUnit qu : childExecSM.getQueryUnits()) {
        for (IntermediateEntry p : qu.getIntermediateData()) {
          FetchImpl fetch = new FetchImpl(p.getPullHost(), RANGE_SHUFFLE, childBlock.getId(), 0);
          fetch.addPart(p.getTaskId(), p.getAttemptId());
          fetches.add(fetch);
        }
      }
    }

    boolean ascendingFirstKey = sortSpecs[0].isAscending();
    SortedMap<TupleRange, Collection<FetchImpl>> map;
    if (ascendingFirstKey) {
      map = new TreeMap<TupleRange, Collection<FetchImpl>>();
    } else {
      map = new TreeMap<TupleRange, Collection<FetchImpl>>(new TupleRange.DescendingTupleRangeComparator());
    }

    Set<FetchImpl> fetchSet;
    try {
      RowStoreUtil.RowStoreEncoder encoder = RowStoreUtil.createEncoder(sortSchema);
      for (int i = 0; i < ranges.length; i++) {
        fetchSet = new HashSet<FetchImpl>();
        for (FetchImpl fetch: fetches) {
          String rangeParam =
              TupleUtil.rangeToQuery(ranges[i], ascendingFirstKey ? i == (ranges.length - 1) : i == 0, encoder);
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

    scheduleFetchesByRoundRobin(subQuery, map, scan.getTableName(), determinedTaskNum);

    schedulerContext.setEstimatedTaskNum(determinedTaskNum);
  }

  public static void scheduleFetchesByRoundRobin(SubQuery subQuery, Map<?, Collection<FetchImpl>> partitions,
                                                   String tableName, int num) {
    int i;
    Map<String, List<FetchImpl>>[] fetchesArray = new Map[num];
    for (i = 0; i < num; i++) {
      fetchesArray[i] = new HashMap<String, List<FetchImpl>>();
    }
    i = 0;
    for (Entry<?, Collection<FetchImpl>> entry : partitions.entrySet()) {
      Collection<FetchImpl> value = entry.getValue();
      TUtil.putCollectionToNestedList(fetchesArray[i++], tableName, value);
      if (i == num) i = 0;
    }
    for (Map<String, List<FetchImpl>> eachFetches : fetchesArray) {
      SubQuery.scheduleFetches(subQuery, eachFetches);
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
                                                 SubQuery subQuery, DataChannel channel,
                                                 int maxNum) {
    ExecutionBlock execBlock = subQuery.getBlock();
    ScanNode scan = execBlock.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.getContext().getStorageManager().getTablePath(scan.getTableName());

    FileFragment frag = new FileFragment(scan.getCanonicalName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
    List<FileFragment> fragments = new ArrayList<FileFragment>();
    fragments.add(frag);
    SubQuery.scheduleFragments(subQuery, fragments);

    Map<Integer, FetchGroupMeta> finalFetches = new HashMap<Integer, FetchGroupMeta>();
    Map<ExecutionBlockId, List<IntermediateEntry>> intermediates = new HashMap<ExecutionBlockId,
        List<IntermediateEntry>>();

    for (ExecutionBlock block : masterPlan.getChilds(execBlock)) {
      List<IntermediateEntry> partitions = new ArrayList<IntermediateEntry>();
      partitions.addAll(subQuery.getContext().getSubQuery(block.getId()).getHashShuffleIntermediateEntries());

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
        Map<QueryUnit.PullHost, List<IntermediateEntry>> hashedByHost = hashByHost(interm.getValue());
        for (Entry<QueryUnit.PullHost, List<IntermediateEntry>> e : hashedByHost.entrySet()) {

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
    LogicalNode[] groupbyNodes = PlannerUtil.findAllNodes(subQuery.getBlock().getPlan(),
        new NodeType[]{NodeType.GROUP_BY, NodeType.DISTINCT_GROUP_BY});
    if (groupbyNodes != null && groupbyNodes.length > 0) {
      LogicalNode bottomNode = groupbyNodes[0];
      if (bottomNode.getType() == NodeType.GROUP_BY) {
        groupingColumns = ((GroupbyNode)bottomNode).getGroupingColumns().length;
      } else if (bottomNode.getType() == NodeType.DISTINCT_GROUP_BY) {
        DistinctGroupbyNode distinctNode = PlannerUtil.findMostBottomNode(subQuery.getBlock().getPlan(), NodeType.DISTINCT_GROUP_BY);
        if (distinctNode == null) {
          LOG.warn(subQuery.getId() + ", Can't find current DistinctGroupbyNode");
          distinctNode = (DistinctGroupbyNode)bottomNode;
        }
        groupingColumns = distinctNode.getGroupingColumns().length;

        Enforcer enforcer = execBlock.getEnforcer();
        EnforceProperty property = PhysicalPlannerImpl.getAlgorithmEnforceProperty(enforcer, distinctNode);
        if (property != null) {
          if (property.getDistinct().getIsMultipleAggregation()) {
            MultipleAggregationStage stage = property.getDistinct().getMultipleAggregationStage();
            if (stage != MultipleAggregationStage.THRID_STAGE) {
              groupingColumns = distinctNode.getOutSchema().size();
            }
          }
        }
      }
    }
    // get a proper number of tasks
    int determinedTaskNum = Math.min(maxNum, finalFetches.size());
    LOG.info(subQuery.getId() + ", ScheduleHashShuffledFetches - Max num=" + maxNum + ", finalFetchURI=" + finalFetches.size());

    if (groupingColumns == 0) {
      determinedTaskNum = 1;
      LOG.info(subQuery.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
    } else {
      TableStats totalStat = computeChildBlocksStats(subQuery.getContext(), masterPlan, subQuery.getId());
      if (totalStat.getNumRows() == 0) {
        determinedTaskNum = 1;
      }
    }

    // set the proper number of tasks to the estimated task num
    if (channel.getShuffleType() == SCATTERED_HASH_SHUFFLE) {
      scheduleScatteredHashShuffleFetches(schedulerContext, subQuery, intermediates,
          scan.getTableName());
    } else {
      schedulerContext.setEstimatedTaskNum(determinedTaskNum);
      // divide fetch uris into the the proper number of tasks according to volumes
      scheduleFetchesByEvenDistributedVolumes(subQuery, finalFetches, scan.getTableName(), determinedTaskNum);
      LOG.info(subQuery.getId() + ", DeterminedTaskNum : " + determinedTaskNum);
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
      fetchesArray[i] = new HashMap<String, List<FetchImpl>>();
      assignedVolumes[i] = 0l;
    }

    // This algorithm assignes bigger first manner by using a sorted iterator. It is a kind of greedy manner.
    // Its complexity is O(n). Since FetchGroup can be more than tens of thousands, we should consider its complexity.
    // In terms of this point, it will show reasonable performance and results. even though it is not an optimal
    // algorithm.
    Iterator<FetchGroupMeta> iterator = fetchGroupMetaList.iterator();

    int p = 0;
    while(iterator.hasNext()) {
      while (p < num && iterator.hasNext()) {
        FetchGroupMeta fetchGroupMeta = iterator.next();
        assignedVolumes[p] += fetchGroupMeta.getVolume();

        TUtil.putCollectionToNestedList(fetchesArray[p], tableName, fetchGroupMeta.fetchUrls);
        p++;
      }

      p = num - 1;
      while (p > 0 && iterator.hasNext()) {
        FetchGroupMeta fetchGroupMeta = iterator.next();
        assignedVolumes[p] += fetchGroupMeta.getVolume();
        TUtil.putCollectionToNestedList(fetchesArray[p], tableName, fetchGroupMeta.fetchUrls);

        // While the current one is smaller than next one, it adds additional fetches to current one.
        while(iterator.hasNext() && assignedVolumes[p - 1] > assignedVolumes[p]) {
          FetchGroupMeta additionalFetchGroup = iterator.next();
          assignedVolumes[p] += additionalFetchGroup.getVolume();
          TUtil.putCollectionToNestedList(fetchesArray[p], tableName, additionalFetchGroup.fetchUrls);
        }

        p--;
      }
    }

    return new Pair<Long[], Map<String, List<FetchImpl>>[]>(assignedVolumes, fetchesArray);
  }

  public static void scheduleFetchesByEvenDistributedVolumes(SubQuery subQuery, Map<Integer, FetchGroupMeta> partitions,
                                                             String tableName, int num) {
    Map<String, List<FetchImpl>>[] fetchsArray = makeEvenDistributedFetchImpl(partitions, tableName, num).getSecond();
    // Schedule FetchImpls
    for (Map<String, List<FetchImpl>> eachFetches : fetchsArray) {
      SubQuery.scheduleFetches(subQuery, eachFetches);
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
       SubQuery subQuery, Map<ExecutionBlockId, List<IntermediateEntry>> intermediates,
       String tableName) {
    long splitVolume = StorageUnit.MB *
        subQuery.getMasterPlan().getContext().getLong(SessionVars.TABLE_PARTITION_PER_SHUFFLE_SIZE);
    long pageSize = StorageUnit.MB * 
        subQuery.getContext().getConf().getIntVar(ConfVars.SHUFFLE_HASH_APPENDER_PAGE_VOLUME); // in bytes
    if (pageSize >= splitVolume) {
      throw new RuntimeException("tajo.dist-query.table-partition.task-volume-mb should be great than " +
          "tajo.shuffle.hash.appender.page.volumn-mb");
    }
    List<List<FetchImpl>> fetches = new ArrayList<List<FetchImpl>>();

    long totalIntermediateSize = 0L;
    for (Entry<ExecutionBlockId, List<IntermediateEntry>> listEntry : intermediates.entrySet()) {
      // merge by PartitionId
      Map<Integer, List<IntermediateEntry>> partitionIntermMap = new HashMap<Integer, List<IntermediateEntry>>();
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
      fetchesArray[i] = new HashMap<String, List<FetchImpl>>();
      fetchesArray[i].put(tableName, entry);

      SubQuery.scheduleFetches(subQuery, fetchesArray[i]);
      i++;
    }

    LOG.info(subQuery.getId()
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
    List<List<FetchImpl>> fetches = new ArrayList<List<FetchImpl>>();

    Iterator<IntermediateEntry> iter = entries.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    List<FetchImpl> fetchListForSingleTask = new ArrayList<FetchImpl>();
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
          fetchListForSingleTask = new ArrayList<FetchImpl>();
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

    List<URI> fetchURLs = new ArrayList<URI>();
    if(includeParts) {
      if (fetch.getType() == HASH_SHUFFLE || fetch.getType() == SCATTERED_HASH_SHUFFLE) {
        fetchURLs.add(URI.create(urlPrefix.toString()));
      } else {
        // If the get request is longer than 2000 characters,
        // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
        // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
        // The below code transforms a long request to multiple requests.
        List<String> taskIdsParams = new ArrayList<String>();
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
    Map<Integer, List<IntermediateEntry>> hashed = new HashMap<Integer, List<IntermediateEntry>>();
    for (IntermediateEntry entry : entries) {
      if (hashed.containsKey(entry.getPartId())) {
        hashed.get(entry.getPartId()).add(entry);
      } else {
        hashed.put(entry.getPartId(), TUtil.newList(entry));
      }
    }

    return hashed;
  }

  public static Map<QueryUnit.PullHost, List<IntermediateEntry>> hashByHost(List<IntermediateEntry> entries) {
    Map<QueryUnit.PullHost, List<IntermediateEntry>> hashed = new HashMap<QueryUnit.PullHost, List<IntermediateEntry>>();

    QueryUnit.PullHost host;
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

  public static SubQuery setShuffleOutputNumForTwoPhase(SubQuery subQuery, final int desiredNum, DataChannel channel) {
    ExecutionBlock execBlock = subQuery.getBlock();
    Column[] keys;
    // if the next query is join,
    // set the partition number for the current logicalUnit
    // TODO: the union handling is required when a join has unions as its child
    MasterPlan masterPlan = subQuery.getMasterPlan();
    keys = channel.getShuffleKeys();
    if (!masterPlan.isRoot(subQuery.getBlock()) ) {
      ExecutionBlock parentBlock = masterPlan.getParent(subQuery.getBlock());
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
    return subQuery;
  }
}
