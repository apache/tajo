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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TaskSchedulerContext;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.worker.FetchImpl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.HASH_SHUFFLE;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.RANGE_SHUFFLE;

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
        // TODO - to be fixed (wrong directory)
        tablePath = storageManager.getTablePath(scans[i].getTableName());
        ExecutionBlockId scanEBId = TajoIdUtils.createExecutionBlockId(scans[i].getTableName());
        stats[i] = masterContext.getSubQuery(scanEBId).getResultStats().getNumBytes();
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

    // If one of inner join tables has no input data, it should return zero rows.
    JoinNode joinNode = PlannerUtil.findMostBottomNode(execBlock.getPlan(), NodeType.JOIN);
    if (joinNode != null) {
      if ( (joinNode.getJoinType() == JoinType.INNER)) {
        for (int i = 0; i < stats.length; i++) {
          if (stats[i] == 0) {
            return;
          }
        }
      }
    }

    // If node is outer join and a preserved relation is empty, it should return zero rows.
    joinNode = PlannerUtil.findTopNode(execBlock.getPlan(), NodeType.JOIN);
    if (joinNode != null) {
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
    int baseScanIdx = -1;
    for (int i = 0; i < scans.length; i++) {
      if (!execBlock.isBroadcastTable(scans[i].getCanonicalName())) {
        isAllBroadcastTable = false;
        baseScanIdx = i;
      }
    }

    if (isAllBroadcastTable) {
      LOG.info("[Distributed Join Strategy] : Immediate " +  fragments.length + " Way Join on Single Machine");
      SubQuery.scheduleFragment(subQuery, fragments[0], Arrays.asList(Arrays.copyOfRange(fragments, 1, fragments.length)));
      schedulerContext.setEstimatedTaskNum(1);
    } else if (!execBlock.getBroadcastTables().isEmpty()) {
      LOG.info(String.format("[Distributed Join Strategy] : Broadcast Join, base_table=%s, base_volume=%d",
          scans[baseScanIdx].getCanonicalName(), stats[baseScanIdx]));
      scheduleLeafTasksWithBroadcastTable(schedulerContext, subQuery, baseScanIdx, fragments);
    } else {
      LOG.info("[Distributed Join Strategy] : Symmetric Repartition Join");
      // The hash map is modeling as follows:
      // <Part Id, <Table Name, Intermediate Data>>
      Map<Integer, Map<String, List<IntermediateEntry>>> hashEntries = new HashMap<Integer, Map<String, List<IntermediateEntry>>>();

      // Grouping IntermediateData by a partition key and a table name
      for (ScanNode scan : scans) {
        SubQuery childSubQuery = masterContext.getSubQuery(TajoIdUtils.createExecutionBlockId(scan.getCanonicalName()));
        for (QueryUnit task : childSubQuery.getQueryUnits()) {
          if (task.getIntermediateData() != null && !task.getIntermediateData().isEmpty()) {
            for (IntermediateEntry intermEntry : task.getIntermediateData()) {
              if (hashEntries.containsKey(intermEntry.getPartId())) {
                Map<String, List<IntermediateEntry>> tbNameToInterm =
                    hashEntries.get(intermEntry.getPartId());

                if (tbNameToInterm.containsKey(scan.getCanonicalName())) {
                  tbNameToInterm.get(scan.getCanonicalName()).add(intermEntry);
                } else {
                  tbNameToInterm.put(scan.getCanonicalName(), TUtil.newList(intermEntry));
                }
              } else {
                Map<String, List<IntermediateEntry>> tbNameToInterm =
                    new HashMap<String, List<IntermediateEntry>>();
                tbNameToInterm.put(scan.getCanonicalName(), TUtil.newList(intermEntry));
                hashEntries.put(intermEntry.getPartId(), tbNameToInterm);
              }
            }
          } else {
            //if no intermidatedata(empty table), make empty entry
            int emptyPartitionId = 0;
            if (hashEntries.containsKey(emptyPartitionId)) {
              Map<String, List<IntermediateEntry>> tbNameToInterm = hashEntries.get(emptyPartitionId);
              if (tbNameToInterm.containsKey(scan.getCanonicalName()))
                tbNameToInterm.get(scan.getCanonicalName())
                    .addAll(new ArrayList<IntermediateEntry>());
              else
                tbNameToInterm.put(scan.getCanonicalName(), new ArrayList<IntermediateEntry>());
            } else {
              Map<String, List<IntermediateEntry>> tbNameToInterm = new HashMap<String, List<IntermediateEntry>>();
              tbNameToInterm.put(scan.getCanonicalName(), new ArrayList<IntermediateEntry>());
              hashEntries.put(emptyPartitionId, tbNameToInterm);
            }
          }
        }
      }

      // hashEntries can be zero if there are no input data.
      // In the case, it will cause the zero divided exception.
      // it avoids this problem.
      int [] avgSize = new int[2];
      avgSize[0] = hashEntries.size() == 0 ? 0 : (int) (stats[0] / hashEntries.size());
      avgSize[1] = hashEntries.size() == 0 ? 0 : (int) (stats[1] / hashEntries.size());
      int bothFetchSize = avgSize[0] + avgSize[1];

      // Getting the desire number of join tasks according to the volumn
      // of a larger table
      int largerIdx = stats[0] >= stats[1] ? 0 : 1;
      int desireJoinTaskVolumn = subQuery.getContext().getConf().
          getIntVar(ConfVars.DIST_QUERY_JOIN_TASK_VOLUME);

      // calculate the number of tasks according to the data size
      int mb = (int) Math.ceil((double)stats[largerIdx] / 1048576);
      LOG.info("Larger intermediate data is approximately " + mb + " MB");
      // determine the number of task per 64MB
      int maxTaskNum = (int) Math.ceil((double)mb / desireJoinTaskVolumn);
      LOG.info("The calculated number of tasks is " + maxTaskNum);
      LOG.info("The number of total shuffle keys is " + hashEntries.size());
      // the number of join tasks cannot be larger than the number of
      // distinct partition ids.
      int joinTaskNum = Math.min(maxTaskNum, hashEntries.size());
      LOG.info("The determined number of join tasks is " + joinTaskNum);

      SubQuery.scheduleFragment(subQuery, fragments[0], Arrays.asList(new FileFragment[]{fragments[1]}));

      // Assign partitions to tasks in a round robin manner.
      for (Entry<Integer, Map<String, List<IntermediateEntry>>> entry
          : hashEntries.entrySet()) {
        addJoinShuffle(subQuery, entry.getKey(), entry.getValue());
      }

      schedulerContext.setTaskSize((int) Math.ceil((double) bothFetchSize / joinTaskNum));
      schedulerContext.setEstimatedTaskNum(joinTaskNum);
    }
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
    //Preconditions.checkArgument(scans.length == 2, "Must be Join Query");

    for (int i = 0; i < scans.length; i++) {
      if (i != baseScanId) {
        scans[i].setBroadcastTable(true);
      }
    }

    TableMeta meta;
    ScanNode scan = scans[baseScanId];
    TableDesc desc = subQuery.getContext().getTableDescMap().get(scan.getCanonicalName());
    meta = desc.getMeta();

    Collection<FileFragment> baseFragments;
    if (scan.getType() == NodeType.PARTITIONS_SCAN) {
      baseFragments = getFragmentsFromPartitionedTable(subQuery.getStorageManager(), scan, desc);
    } else {
      baseFragments = subQuery.getStorageManager().getSplits(scan.getCanonicalName(), meta, desc.getSchema(),
          desc.getPath());
    }

    List<FileFragment> broadcastFragments = new ArrayList<FileFragment>();
    for (int i = 0; i < fragments.length; i++) {
      if (i != baseScanId) {
        broadcastFragments.add(fragments[i]);
      }
    }
    SubQuery.scheduleFragments(subQuery, baseFragments, broadcastFragments);
    schedulerContext.setEstimatedTaskNum(baseFragments.size());
  }

  private static void addJoinShuffle(SubQuery subQuery, int partitionId,
                                     Map<String, List<IntermediateEntry>> grouppedPartitions) {
    Map<String, List<FetchImpl>> fetches = new HashMap<String, List<FetchImpl>>();
    for (ExecutionBlock execBlock : subQuery.getMasterPlan().getChilds(subQuery.getId())) {
      if (grouppedPartitions.containsKey(execBlock.getId().toString())) {
        Collection<FetchImpl> requests = mergeShuffleRequest(execBlock.getId(), partitionId, HASH_SHUFFLE,
              grouppedPartitions.get(execBlock.getId().toString()));
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
  private static Collection<FetchImpl> mergeShuffleRequest(ExecutionBlockId ebid, int partitionId,
                                                          TajoWorkerProtocol.ShuffleType type,
                                                          List<IntermediateEntry> partitions) {
    Map<QueryUnit.PullHost, FetchImpl> mergedPartitions = new HashMap<QueryUnit.PullHost, FetchImpl>();

    for (IntermediateEntry partition : partitions) {
      QueryUnit.PullHost host = partition.getPullHost();
      if (mergedPartitions.containsKey(host)) {
        FetchImpl fetch = mergedPartitions.get(partition.getPullHost());
        fetch.addPart(partition.getTaskId(), partition.getAttemptId());
      } else {
        FetchImpl fetch = new FetchImpl(host, type, ebid, partitionId);
        fetch.addPart(partition.getTaskId(), partition.getAttemptId());
        mergedPartitions.put(partition.getPullHost(), fetch);
      }
    }
    return mergedPartitions.values();
  }

  public static void scheduleFragmentsForNonLeafTasks(TaskSchedulerContext schedulerContext,
                                                      MasterPlan masterPlan, SubQuery subQuery, int maxNum)
      throws IOException {
    DataChannel channel = masterPlan.getIncomingChannels(subQuery.getBlock().getId()).get(0);
    if (channel.getShuffleType() == HASH_SHUFFLE) {
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
    TupleRange mergedRange = TupleUtil.columnStatToRange(sortSpecs, sortSchema, totalStat.getColumnStats());
    RangePartitionAlgorithm partitioner = new UniformRangePartition(mergedRange, sortSpecs);
    BigDecimal card = partitioner.getTotalCardinality();

    // if the number of the range cardinality is less than the desired number of tasks,
    // we set the the number of tasks to the number of range cardinality.
    int determinedTaskNum;
    if (card.compareTo(new BigDecimal(maxNum)) < 0) {
      LOG.info("The range cardinality (" + card
          + ") is less then the desired number of tasks (" + maxNum + ")");
      determinedTaskNum = card.intValue();
    } else {
      determinedTaskNum = maxNum;
    }

    LOG.info("Try to divide " + mergedRange + " into " + determinedTaskNum +
        " sub ranges (total units: " + determinedTaskNum + ")");
    TupleRange [] ranges = partitioner.partition(determinedTaskNum);

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
          fetch.setRangeParams(rangeParam);
          fetchSet.add(fetch);
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

  public static void scheduleHashShuffledFetches(TaskSchedulerContext schedulerContext, MasterPlan masterPlan,
                                                 SubQuery subQuery, DataChannel channel,
                                                 int maxNum) {
    ExecutionBlock execBlock = subQuery.getBlock();
    TableStats totalStat = computeChildBlocksStats(subQuery.getContext(), masterPlan, subQuery.getId());

    if (totalStat.getNumRows() == 0) {
      return;
    }

    ScanNode scan = execBlock.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.getContext().getStorageManager().getTablePath(scan.getTableName());

    FileFragment frag = new FileFragment(scan.getCanonicalName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
    List<FileFragment> fragments = new ArrayList<FileFragment>();
    fragments.add(frag);
    SubQuery.scheduleFragments(subQuery, fragments);

    Map<QueryUnit.PullHost, List<IntermediateEntry>> hashedByHost;
    Map<Integer, Collection<FetchImpl>> finalFetches = new HashMap<Integer, Collection<FetchImpl>>();

    for (ExecutionBlock block : masterPlan.getChilds(execBlock)) {
      List<IntermediateEntry> partitions = new ArrayList<IntermediateEntry>();
      for (QueryUnit tasks : subQuery.getContext().getSubQuery(block.getId()).getQueryUnits()) {
        if (tasks.getIntermediateData() != null) {
          partitions.addAll(tasks.getIntermediateData());
        }
      }
      Map<Integer, List<IntermediateEntry>> hashed = hashByKey(partitions);
      for (Entry<Integer, List<IntermediateEntry>> interm : hashed.entrySet()) {
        hashedByHost = hashByHost(interm.getValue());
        for (Entry<QueryUnit.PullHost, List<IntermediateEntry>> e : hashedByHost.entrySet()) {

          FetchImpl fetch = new FetchImpl(e.getKey(), channel.getShuffleType(),
              block.getId(), interm.getKey(), e.getValue());

          if (finalFetches.containsKey(interm.getKey())) {
            finalFetches.get(interm.getKey()).add(fetch);
          } else {
            finalFetches.put(interm.getKey(), TUtil.newList(fetch));
          }
        }
      }
    }

    GroupbyNode groupby = PlannerUtil.findMostBottomNode(subQuery.getBlock().getPlan(), NodeType.GROUP_BY);
    // get a proper number of tasks
    int determinedTaskNum = Math.min(maxNum, finalFetches.size());
    LOG.info(subQuery.getId() + ", ScheduleHashShuffledFetches - Max num=" + maxNum + ", finalFetchURI=" + finalFetches.size());
    if (groupby != null && groupby.getGroupingColumns().length == 0) {
      determinedTaskNum = 1;
      LOG.info(subQuery.getId() + ", No Grouping Column - determinedTaskNum is set to 1");
    }

    // set the proper number of tasks to the estimated task num
    schedulerContext.setEstimatedTaskNum(determinedTaskNum);
    // divide fetch uris into the the proper number of tasks in a round robin manner.
    scheduleFetchesByRoundRobin(subQuery, finalFetches, scan.getTableName(), determinedTaskNum);
    LOG.info(subQuery.getId() + ", DeterminedTaskNum : " + determinedTaskNum);
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
    }

    List<URI> fetchURLs = new ArrayList<URI>();
    if(includeParts){
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
      if (execBlock.getPlan().getType() == NodeType.GROUP_BY) {
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
