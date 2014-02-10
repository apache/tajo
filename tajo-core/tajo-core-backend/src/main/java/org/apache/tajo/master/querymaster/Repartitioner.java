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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
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
import org.apache.tajo.master.TaskSchedulerContext;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.TajoIdUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.HASH_SHUFFLE;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.RANGE_SHUFFLE;

/**
 * Repartitioner creates non-leaf tasks and shuffles intermediate data.
 * It supports two repartition methods, such as hash and range repartition.
 */
public class Repartitioner {
  private static final Log LOG = LogFactory.getLog(Repartitioner.class);

  private static int HTTP_REQUEST_MAXIMUM_LENGTH = 1900;
  private final static String UNKNOWN_HOST = "unknown";

  public static void scheduleFragmentsForJoinQuery(TaskSchedulerContext schedulerContext, SubQuery subQuery)
      throws IOException {
    MasterPlan masterPlan = subQuery.getMasterPlan();
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryMasterTask.QueryMasterTaskContext masterContext = subQuery.getContext();
    AbstractStorageManager storageManager = subQuery.getStorageManager();

    ScanNode[] scans = execBlock.getScanNodes();

    Path tablePath;
    FileFragment[] fragments = new FileFragment[2];
    long[] stats = new long[2];

    // initialize variables from the child operators
    for (int i = 0; i < 2; i++) {
      TableDesc tableDesc = masterContext.getTableDescMap().get(scans[i].getCanonicalName());
      if (tableDesc == null) { // if it is a real table stored on storage
        // TODO - to be fixed (wrong directory)
        ExecutionBlock [] childBlocks = new ExecutionBlock[2];
        childBlocks[0] = masterPlan.getChild(execBlock.getId(), 0);
        childBlocks[1] = masterPlan.getChild(execBlock.getId(), 1);

        tablePath = storageManager.getTablePath(scans[i].getTableName());
        stats[i] = masterContext.getSubQuery(childBlocks[i].getId()).getTableStat().getNumBytes();
        fragments[i] = new FileFragment(scans[i].getCanonicalName(), tablePath, 0, 0, new String[]{UNKNOWN_HOST});
      } else {
        tablePath = tableDesc.getPath();
        try {
          stats[i] = GlobalPlanner.computeDescendentVolume(scans[i]);
        } catch (PlanningException e) {
          throw new IOException(e);
        }
        fragments[i] = storageManager.getSplits(scans[i].getCanonicalName(), tableDesc.getMeta(), tableDesc.getSchema(),
            tablePath).get(0);
      }
    }

    LOG.info(String.format("Left Volume: %d, Right Volume: %d", stats[0], stats[1]));

    // Assigning either fragments or fetch urls to query units
    boolean leftSmall = execBlock.isBroadcastTable(scans[0].getCanonicalName());
    boolean rightSmall = execBlock.isBroadcastTable(scans[1].getCanonicalName());

    if (leftSmall && rightSmall) {
      LOG.info("[Distributed Join Strategy] : Immediate Two Way Join on Single Machine");
      SubQuery.scheduleFragment(subQuery, fragments[0], fragments[1]);
      schedulerContext.setEstimatedTaskNum(1);
    } else if (leftSmall ^ rightSmall) {
      int broadcastIdx = leftSmall ? 0 : 1;
      int baseScanIdx = leftSmall ? 1 : 0;
      LOG.info(String.format("[BRDCAST JOIN] base_table=%s, base_volume=%d",
          scans[baseScanIdx].getCanonicalName(), stats[baseScanIdx]));
      scheduleLeafTasksWithBroadcastTable(schedulerContext, subQuery, baseScanIdx, fragments[broadcastIdx]);
    } else {
      LOG.info("[Distributed Join Strategy] : Symmetric Repartition Join");
      // The hash map is modeling as follows:
      // <Part Id, <Table Name, Intermediate Data>>
      Map<Integer, Map<String, List<IntermediateEntry>>> hashEntries = new HashMap<Integer, Map<String, List<IntermediateEntry>>>();

      // Grouping IntermediateData by a partition key and a table name
      for (ScanNode scan : scans) {
        SubQuery childSubQuery = masterContext.getSubQuery(TajoIdUtils.createExecutionBlockId(scan.getCanonicalName()));
        for (QueryUnit task : childSubQuery.getQueryUnits()) {
          if (task.getIntermediateData() != null) {
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
          }
        }
      }

      int [] avgSize = new int[2];
      avgSize[0] = (int) (stats[0] / hashEntries.size());
      avgSize[1] = (int) (stats[1] / hashEntries.size());
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

      SubQuery.scheduleFragment(subQuery, fragments[0], fragments[1]);

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
    for (Path path : partitionsScan.getInputPaths()) {
      fragments.addAll(sm.getSplits(
          scan.getCanonicalName(), table.getMeta(), table.getSchema(), path));
    }
    partitionsScan.setInputPaths(null);
    return fragments;
  }

  private static void scheduleLeafTasksWithBroadcastTable(TaskSchedulerContext schedulerContext, SubQuery subQuery,
                                                          int baseScanId, FileFragment broadcasted) throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
    ScanNode[] scans = execBlock.getScanNodes();
    Preconditions.checkArgument(scans.length == 2, "Must be Join Query");
    TableMeta meta;
    ScanNode scan = scans[baseScanId];
    TableDesc desc = subQuery.getContext().getTableDescMap().get(scan.getCanonicalName());
    meta = desc.getMeta();

    Collection<FileFragment> fragments;
    if (scan.getType() == NodeType.PARTITIONS_SCAN) {
      fragments = getFragmentsFromPartitionedTable(subQuery.getStorageManager(), scan, desc);
    } else {
      fragments = subQuery.getStorageManager().getSplits(scan.getCanonicalName(), meta, desc.getSchema(),
          desc.getPath());
    }

    SubQuery.scheduleFragments(subQuery, fragments, broadcasted);
    schedulerContext.setEstimatedTaskNum(fragments.size());
  }

  private static void addJoinShuffle(SubQuery subQuery, int partitionId,
                                     Map<String, List<IntermediateEntry>> grouppedPartitions) {
    Map<String, List<URI>> fetches = new HashMap<String, List<URI>>();
    for (ExecutionBlock execBlock : subQuery.getMasterPlan().getChilds(subQuery.getId())) {
      Map<String, List<IntermediateEntry>> requests;
      if (grouppedPartitions.containsKey(execBlock.getId().toString())) {
          requests = mergeHashShuffleRequest(grouppedPartitions.get(execBlock.getId().toString()));
      } else {
        return;
      }
      Set<URI> fetchURIs = TUtil.newHashSet();
      for (Entry<String, List<IntermediateEntry>> requestPerNode : requests.entrySet()) {
        Collection<URI> uris = createHashFetchURL(requestPerNode.getKey(),
            execBlock.getId(),
            partitionId, HASH_SHUFFLE,
            requestPerNode.getValue());
        fetchURIs.addAll(uris);
      }
      fetches.put(execBlock.getId().toString(), Lists.newArrayList(fetchURIs));
    }
    SubQuery.scheduleFetches(subQuery, fetches);
  }

  /**
   * This method merges the partition request associated with the pullserver's address.
   * It reduces the number of TCP connections.
   *
   * @return key: pullserver's address, value: a list of requests
   */
  private static Map<String, List<IntermediateEntry>> mergeHashShuffleRequest(List<IntermediateEntry> partitions) {
    Map<String, List<IntermediateEntry>> mergedPartitions = new HashMap<String, List<IntermediateEntry>>();
    for (IntermediateEntry partition : partitions) {
      if (mergedPartitions.containsKey(partition.getPullAddress())) {
        mergedPartitions.get(partition.getPullAddress()).add(partition);
      } else {
        mergedPartitions.put(partition.getPullAddress(), TUtil.newList(partition));
      }
    }

    return mergedPartitions;
  }

  public static void scheduleFragmentsForNonLeafTasks(TaskSchedulerContext schedulerContext,
                                                      MasterPlan masterPlan, SubQuery subQuery, int maxNum)
      throws InternalException {
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
      tableStatses.add(childExecSM.getTableStat());
    }
    return StatisticsUtil.aggregateTableStat(tableStatses);
  }

  public static void scheduleRangeShuffledFetches(TaskSchedulerContext schedulerContext, MasterPlan masterPlan,
                                                  SubQuery subQuery, DataChannel channel, int maxNum)
      throws InternalException {
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

    List<String> basicFetchURIs = new ArrayList<String>();
    List<ExecutionBlock> childBlocks = masterPlan.getChilds(subQuery.getId());
    for (ExecutionBlock childBlock : childBlocks) {
      SubQuery childExecSM = subQuery.getContext().getSubQuery(childBlock.getId());
      for (QueryUnit qu : childExecSM.getQueryUnits()) {
        for (IntermediateEntry p : qu.getIntermediateData()) {
          String uri = createBasicFetchUri(p.getPullHost(), p.getPullPort(), childBlock.getId(), p.taskId, p.attemptId);
          basicFetchURIs.add(uri);
        }
      }
    }

    boolean ascendingFirstKey = sortSpecs[0].isAscending();
    SortedMap<TupleRange, Set<URI>> map;
    if (ascendingFirstKey) {
      map = new TreeMap<TupleRange, Set<URI>>();
    } else {
      map = new TreeMap<TupleRange, Set<URI>>(new TupleRange.DescendingTupleRangeComparator());
    }

    Set<URI> uris;
    try {
      for (int i = 0; i < ranges.length; i++) {
        uris = new HashSet<URI>();
        for (String uri: basicFetchURIs) {
          String rangeParam =
              TupleUtil.rangeToQuery(sortSchema, ranges[i], ascendingFirstKey ? i == (ranges.length - 1) : i == 0);
          URI finalUri = URI.create(uri + "&" + rangeParam);
          uris.add(finalUri);
        }
        map.put(ranges[i], uris);
      }

    } catch (UnsupportedEncodingException e) {
      LOG.error(e);
    }

    scheduleFetchesByRoundRobin(subQuery, map, scan.getTableName(), determinedTaskNum);

    schedulerContext.setEstimatedTaskNum(determinedTaskNum);
  }

  public static void scheduleFetchesByRoundRobin(SubQuery subQuery, Map<?, Set<URI>> partitions,
                                                   String tableName, int num) {
    int i;
    Map<String, List<URI>>[] fetchesArray = new Map[num];
    for (i = 0; i < num; i++) {
      fetchesArray[i] = new HashMap<String, List<URI>>();
    }
    i = 0;
    for (Entry<?, Set<URI>> entry : partitions.entrySet()) {
      Set<URI> value = entry.getValue();
      fetchesArray[i++].put(tableName, Lists.newArrayList(value));
      if (i == num) i = 0;
    }
    for (Map<String, List<URI>> eachFetches : fetchesArray) {
      SubQuery.scheduleFetches(subQuery, eachFetches);
    }
  }

  public static String createBasicFetchUri(String hostName, int port,
                                           ExecutionBlockId childSid,
                                           int taskId, int attemptId) {
    String scheme = "http://";
    StringBuilder sb = new StringBuilder(scheme);
    sb.append(hostName).append(":").append(port).append("/?")
        .append("qid=").append(childSid.getQueryId().toString())
        .append("&sid=").append(childSid.getId())
        .append("&").append("ta=").append(taskId).append("_").append(attemptId)
        .append("&").append("p=0")
        .append("&").append("type=r");

    return sb.toString();
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

    Map<String, List<IntermediateEntry>> hashedByHost;
    Map<Integer, List<URI>> finalFetchURI = new HashMap<Integer, List<URI>>();

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
        for (Entry<String, List<IntermediateEntry>> e : hashedByHost.entrySet()) {
          Collection<URI> uris = createHashFetchURL(e.getKey(), block.getId(),
              interm.getKey(), channel.getShuffleType(), e.getValue());

          if (finalFetchURI.containsKey(interm.getKey())) {
            finalFetchURI.get(interm.getKey()).addAll(uris);
          } else {
            finalFetchURI.put(interm.getKey(), TUtil.newList(uris));
          }
        }
      }
    }

    GroupbyNode groupby = PlannerUtil.findTopNode(subQuery.getBlock().getPlan(), NodeType.GROUP_BY);
    // the number of tasks cannot exceed the number of merged fetch uris.
    int determinedTaskNum = Math.min(maxNum, finalFetchURI.size());
    if (groupby != null && groupby.getGroupingColumns().length == 0) {
      determinedTaskNum = 1;
    }

    for (Entry<Integer, List<URI>> entry : finalFetchURI.entrySet()) {
      List<URI> value = entry.getValue();
      Map<String, List<URI>> fetches = new HashMap<String, List<URI>>();
      fetches.put(scan.getTableName(), value);
      SubQuery.scheduleFetches(subQuery, fetches);
    }

    schedulerContext.setEstimatedTaskNum(determinedTaskNum);
  }

  public static Collection<URI> createHashFetchURL(String hostAndPort, ExecutionBlockId ebid,
                                       int partitionId, ShuffleType type, List<IntermediateEntry> entries) {
    String scheme = "http://";
    StringBuilder urlPrefix = new StringBuilder(scheme);
    urlPrefix.append(hostAndPort).append("/?")
        .append("qid=").append(ebid.getQueryId().toString())
        .append("&sid=").append(ebid.getId())
        .append("&p=").append(partitionId)
        .append("&type=");
    if (type == HASH_SHUFFLE) {
      urlPrefix.append("h");
    } else if (type == RANGE_SHUFFLE) {
      urlPrefix.append("r");
    }
    urlPrefix.append("&ta=");

    // If the get request is longer than 2000 characters,
    // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
    // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
    // The below code transforms a long request to multiple requests.
    List<String> taskIdsParams = new ArrayList<String>();
    boolean first = true;
    StringBuilder taskIdListBuilder = new StringBuilder();
    for (IntermediateEntry entry: entries) {
      StringBuilder taskAttemptId = new StringBuilder();

      if (!first) { // when comma is added?
        taskAttemptId.append(",");
      } else {
        first = false;
      }

      taskAttemptId.append(entry.getTaskId()).append("_").
          append(entry.getAttemptId());
      if (taskIdListBuilder.length() + taskAttemptId.length()
          > HTTP_REQUEST_MAXIMUM_LENGTH) {
        taskIdsParams.add(taskIdListBuilder.toString());
        taskIdListBuilder = new StringBuilder(entry.getTaskId() + "_" + entry.getAttemptId());
      } else {
        taskIdListBuilder.append(taskAttemptId);
      }
    }

    // if the url params remain
    if (taskIdListBuilder.length() > 0) {
      taskIdsParams.add(taskIdListBuilder.toString());
    }

    Collection<URI> fetchURLs = new ArrayList<URI>();
    for (String param : taskIdsParams) {
      fetchURLs.add(URI.create(urlPrefix + param));
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

  public static Map<String, List<IntermediateEntry>> hashByHost(List<IntermediateEntry> entries) {
    Map<String, List<IntermediateEntry>> hashed = new HashMap<String, List<IntermediateEntry>>();

    String hostName;
    for (IntermediateEntry entry : entries) {
      hostName = entry.getPullHost() + ":" + entry.getPullPort();
      if (hashed.containsKey(hostName)) {
        hashed.get(hostName).add(entry);
      } else {
        hashed.put(hostName, TUtil.newList(entry));
      }
    }

    return hashed;
  }

  public static SubQuery setShuffleOutputNumForTwoPhase(SubQuery subQuery, final int desiredNum, DataChannel channel) {
    ExecutionBlock execBlock = subQuery.getBlock();
    Column[] keys = null;
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
        GroupbyNode groupby = (GroupbyNode) execBlock.getPlan();
        keys = groupby.getGroupingColumns();
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
