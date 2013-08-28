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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.master.ExecutionBlock;
import org.apache.tajo.master.ExecutionBlock.PartitionType;
import org.apache.tajo.master.querymaster.QueryUnit.IntermediateEntry;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

/**
 * Repartitioner creates non-leaf tasks and shuffles intermediate data.
 * It supports two repartition methods, such as hash and range repartition.
 */
public class Repartitioner {
  private static final Log LOG = LogFactory.getLog(Repartitioner.class);

  private static int HTTP_REQUEST_MAXIMUM_LENGTH = 1900;

  public static QueryUnit[] createJoinTasks(SubQuery subQuery)
      throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
    //CatalogService catalog = subQuery.getContext().getCatalog();

    ScanNode[] scans = execBlock.getScanNodes();
    Path tablePath;
    Fragment [] fragments = new Fragment[2];
    TableStat [] stats = new TableStat[2];

    // initialize variables from the child operators
    for (int i =0; i < 2; i++) {
      // TODO - temporarily tables should be stored in temporarily catalog for each query
      TableDesc tableDesc = subQuery.getContext().getTableDescMap().get(scans[i].getFromTable().getTableName());
      if (scans[i].getTableId().startsWith(ExecutionBlockId.EB_ID_PREFIX)) {
        tablePath = subQuery.getStorageManager().getTablePath(scans[i].getTableId());
        stats[i] = subQuery.getChildQuery(scans[i]).getTableStat();
      } else {
        tablePath = tableDesc.getPath();
        stats[i] = tableDesc.getMeta().getStat();
      }

      if (scans[i].isLocal()) { // it only requires a dummy fragment.
        fragments[i] = new Fragment(scans[i].getTableId(), tablePath,
            CatalogUtil.newTableMeta(scans[i].getInSchema(), StoreType.CSV), 0, 0);
      } else {
        fragments[i] = subQuery.getStorageManager().getSplits(scans[i].getTableId(),
                tableDesc.getMeta(),
            new Path(tablePath, "data")).get(0);
      }
    }

    // Assigning either fragments or fetch urls to query units
    QueryUnit [] tasks;
    if (scans[0].isBroadcast() || scans[1].isBroadcast()) {
      tasks = new QueryUnit[1];
      tasks[0] = new QueryUnit(QueryIdFactory.newQueryUnitId(subQuery.getId(), 0),
          false, subQuery.getEventHandler());
      tasks[0].setLogicalPlan(execBlock.getPlan());
      tasks[0].setFragment(scans[0].getTableId(), fragments[0]);
      tasks[0].setFragment(scans[1].getTableId(), fragments[1]);
    } else {
      // The hash map is modeling as follows:
      // <Partition Id, <Table Name, Intermediate Data>>
      Map<Integer, Map<String, List<IntermediateEntry>>> hashEntries =
          new HashMap<Integer, Map<String, List<IntermediateEntry>>>();

      // Grouping IntermediateData by a partition key and a table name
      for (ScanNode scan : scans) {
        SubQuery childSubQuery = subQuery.getChildQuery(scan);
        for (QueryUnit task : childSubQuery.getQueryUnits()) {
          if (task.getIntermediateData() != null) {
            for (IntermediateEntry intermEntry : task.getIntermediateData()) {
              if (hashEntries.containsKey(intermEntry.getPartitionId())) {
                Map<String, List<IntermediateEntry>> tbNameToInterm =
                    hashEntries.get(intermEntry.getPartitionId());

                if (tbNameToInterm.containsKey(scan.getTableId())) {
                  tbNameToInterm.get(scan.getTableId()).add(intermEntry);
                } else {
                  tbNameToInterm.put(scan.getTableId(), TUtil.newList(intermEntry));
                }
              } else {
                Map<String, List<IntermediateEntry>> tbNameToInterm =
                    new HashMap<String, List<IntermediateEntry>>();
                tbNameToInterm.put(scan.getTableId(), TUtil.newList(intermEntry));
                hashEntries.put(intermEntry.getPartitionId(), tbNameToInterm);
              }
            }
          }
        }
      }

      LOG.info("Outer Intermediate Volume: " + stats[0].getNumBytes());
      LOG.info("Inner Intermediate Volume: " + stats[1].getNumBytes());

      // Getting the desire number of join tasks according to the volumn
      // of a larger table
      int largerIdx = stats[0].getNumBytes() >= stats[1].getNumBytes() ? 0 : 1;
      int desireJoinTaskVolumn = subQuery.getContext().getConf().
          getIntVar(ConfVars.JOIN_TASK_VOLUME);

      // calculate the number of tasks according to the data size
      int mb = (int) Math.ceil((double)stats[largerIdx].getNumBytes() / 1048576);
      LOG.info("Larger intermediate data is approximately " + mb + " MB");
      // determine the number of task per 64MB
      int maxTaskNum = (int) Math.ceil((double)mb / desireJoinTaskVolumn);
      LOG.info("The calculated number of tasks is " + maxTaskNum);
      LOG.info("The number of total partition keys is " + hashEntries.size());
      // the number of join tasks cannot be larger than the number of
      // distinct partition ids.
      int joinTaskNum = Math.min(maxTaskNum, hashEntries.size());
      LOG.info("The determined number of join tasks is " + joinTaskNum);
      QueryUnit [] createdTasks = newEmptyJoinTask(subQuery, fragments, joinTaskNum);

      // Assign partitions to tasks in a round robin manner.
      int i = 0;
      for (Entry<Integer, Map<String, List<IntermediateEntry>>> entry
          : hashEntries.entrySet()) {
        addJoinPartition(createdTasks[i++], subQuery, entry.getKey(), entry.getValue());
        if (i >= joinTaskNum) {
          i = 0;
        }
      }

      List<QueryUnit> filteredTasks = new ArrayList<QueryUnit>();
      for (QueryUnit task : createdTasks) {
        // if there are at least two fetches, the join is possible.
        if (task.getFetches().size() > 1) {
          filteredTasks.add(task);
        }
      }

      tasks = filteredTasks.toArray(new QueryUnit[filteredTasks.size()]);
    }

    return tasks;
  }

  private static QueryUnit [] newEmptyJoinTask(SubQuery subQuery, Fragment [] fragments, int taskNum) {
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryUnit [] tasks = new QueryUnit[taskNum];
    for (int i = 0; i < taskNum; i++) {
      tasks[i] = new QueryUnit(
          QueryIdFactory.newQueryUnitId(subQuery.getId(), i), execBlock.isLeafBlock(),
          subQuery.getEventHandler());
      tasks[i].setLogicalPlan(execBlock.getPlan());
      for (Fragment fragment : fragments) {
        tasks[i].setFragment2(fragment);
      }
    }

    return tasks;
  }

  private static void addJoinPartition(QueryUnit task, SubQuery subQuery, int partitionId,
                                       Map<String, List<IntermediateEntry>> grouppedPartitions) {

    for (ScanNode scanNode : subQuery.getBlock().getScanNodes()) {
      Map<String, List<IntermediateEntry>> requests;
      if (grouppedPartitions.containsKey(scanNode.getTableId())) {
          requests = mergeHashPartitionRequest(grouppedPartitions.get(scanNode.getTableId()));
      } else {
        return;
      }
      Set<URI> fetchURIs = TUtil.newHashSet();
      for (Entry<String, List<IntermediateEntry>> requestPerNode : requests.entrySet()) {
        Collection<URI> uris = createHashFetchURL(requestPerNode.getKey(),
            subQuery.getChildQuery(scanNode).getId(),
            partitionId, PartitionType.HASH,
            requestPerNode.getValue());
        fetchURIs.addAll(uris);
      }
      task.addFetches(scanNode.getTableId(), fetchURIs);
    }
  }

  /**
   * This method merges the partition request associated with the pullserver's address.
   * It reduces the number of TCP connections.
   *
   * @return key: pullserver's address, value: a list of requests
   */
  private static Map<String, List<IntermediateEntry>> mergeHashPartitionRequest(
      List<IntermediateEntry> partitions) {
    Map<String, List<IntermediateEntry>> mergedPartitions =
        new HashMap<String, List<IntermediateEntry>>();
    for (IntermediateEntry partition : partitions) {
      if (mergedPartitions.containsKey(partition.getPullAddress())) {
        mergedPartitions.get(partition.getPullAddress()).add(partition);
      } else {
        mergedPartitions.put(partition.getPullAddress(), TUtil.newList(partition));
      }
    }

    return mergedPartitions;
  }

  public static QueryUnit [] createNonLeafTask(SubQuery subQuery,
                                               SubQuery childSubQuery,
                                               int maxNum)
      throws InternalException {
    ExecutionBlock childExecBlock = childSubQuery.getBlock();
    if (childExecBlock.getPartitionType() == PartitionType.HASH) {
      return createHashPartitionedTasks(subQuery, childSubQuery, maxNum);
    } else if (childExecBlock.getPartitionType() == PartitionType.RANGE) {
      return createRangePartitionedTasks(subQuery, childSubQuery, maxNum);
    } else {
      throw new InternalException("Cannot support partition type");
    }
  }

  public static QueryUnit [] createRangePartitionedTasks(SubQuery subQuery,
                                                         SubQuery childSubQuery,
                                                         int maxNum)
      throws InternalException {
    ExecutionBlock execBlock = subQuery.getBlock();
    TableStat stat = childSubQuery.getTableStat();
    if (stat.getNumRows() == 0) {
      return new QueryUnit[0];
    }

    ScanNode scan = execBlock.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.getContext().getStorageManager().getTablePath(scan.getTableId());

    StoreTableNode store = (StoreTableNode) childSubQuery.getBlock().getPlan();
    SortNode sort = (SortNode) store.getChild();
    SortSpec[] sortSpecs = sort.getSortKeys();
    Schema sortSchema = PlannerUtil.sortSpecsToSchema(sort.getSortKeys());

    // calculate the number of maximum query ranges
    TupleRange mergedRange =
        TupleUtil.columnStatToRange(sort.getOutSchema(),
            sortSchema, stat.getColumnStats());
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(sortSchema, mergedRange);
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

    Fragment dummyFragment = new Fragment(scan.getTableId(), tablePath,
        CatalogUtil.newTableMeta(scan.getInSchema(), StoreType.CSV),0, 0);

    List<String> basicFetchURIs = new ArrayList<String>();

    SubQuery child = childSubQuery.getContext().getSubQuery(
        subQuery.getBlock().getChildBlock(scan).getId());
    for (QueryUnit qu : child.getQueryUnits()) {
      for (IntermediateEntry p : qu.getIntermediateData()) {
        String uri = createBasicFetchUri(p.getPullHost(), p.getPullPort(),
            childSubQuery.getId(), p.taskId, p.attemptId);
        basicFetchURIs.add(uri);
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
          String rangeParam = TupleUtil.rangeToQuery(sortSchema, ranges[i],
              ascendingFirstKey, ascendingFirstKey ? i == (ranges.length - 1) : i == 0);
          URI finalUri = URI.create(uri + "&" + rangeParam);
          uris.add(finalUri);
        }
        map.put(ranges[i], uris);
      }

    } catch (UnsupportedEncodingException e) {
      LOG.error(e);
    }

    QueryUnit [] tasks = createEmptyNonLeafTasks(subQuery, determinedTaskNum, dummyFragment);
    assignPartitionByRoundRobin(map, scan.getTableId(), tasks);
    return tasks;
  }

  public static QueryUnit [] assignPartitionByRoundRobin(Map<?, Set<URI>> partitions,
                                               String tableName, QueryUnit [] tasks) {
    int tid = 0;
    for (Entry<?, Set<URI>> entry : partitions.entrySet()) {
      for (URI uri : entry.getValue()) {
        tasks[tid].addFetch(tableName, uri);
      }

      if (tid >= tasks.length) {
        tid = 0;
      } else {
        tid ++;
      }
    }

    return tasks;
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

  public static QueryUnit [] createHashPartitionedTasks(SubQuery subQuery,
                                                 SubQuery childSubQuery,
                                                 int maxNum) {
    ExecutionBlock execBlock = subQuery.getBlock();
    TableStat stat = childSubQuery.getTableStat();
    if (stat.getNumRows() == 0) {
      return new QueryUnit[0];
    }

    ScanNode scan = execBlock.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.getContext().getStorageManager().getTablePath(scan.getTableId());

    List<IntermediateEntry> partitions = new ArrayList<IntermediateEntry>();
    for (QueryUnit tasks : childSubQuery.getQueryUnits()) {
      if (tasks.getIntermediateData() != null) {
        partitions.addAll(tasks.getIntermediateData());
      }
    }

    Fragment frag = new Fragment(scan.getTableId(), tablePath,
        CatalogUtil.newTableMeta(scan.getInSchema(), StoreType.CSV), 0, 0);

    Map<Integer, List<IntermediateEntry>> hashed = hashByKey(partitions);
    Map<String, List<IntermediateEntry>> hashedByHost;
    Map<Integer, List<URI>> finalFetchURI = new HashMap<Integer, List<URI>>();

    for (Entry<Integer, List<IntermediateEntry>> interm : hashed.entrySet()) {
      hashedByHost = hashByHost(interm.getValue());
      for (Entry<String, List<IntermediateEntry>> e : hashedByHost.entrySet()) {
        Collection<URI> uris = createHashFetchURL(e.getKey(), childSubQuery.getId(),
            interm.getKey(),
            childSubQuery.getBlock().getPartitionType(), e.getValue());

        if (finalFetchURI.containsKey(interm.getKey())) {
          finalFetchURI.get(interm.getKey()).addAll(uris);
        } else {
          finalFetchURI.put(interm.getKey(), TUtil.newList(uris));
        }
      }
    }

    GroupbyNode groupby = (GroupbyNode) childSubQuery.getBlock().getStoreTableNode().
        getChild();
    // the number of tasks cannot exceed the number of merged fetch uris.
    int determinedTaskNum = Math.min(maxNum, finalFetchURI.size());
    if (groupby.getGroupingColumns().length == 0) {
      determinedTaskNum = 1;
    }

    QueryUnit [] tasks = createEmptyNonLeafTasks(subQuery, determinedTaskNum, frag);

    int tid = 0;
    for (Entry<Integer, List<URI>> entry : finalFetchURI.entrySet()) {
      for (URI uri : entry.getValue()) {
        tasks[tid].addFetch(scan.getTableId(), uri);
      }

      tid ++;

      if (tid == tasks.length) {
       tid = 0;
      }
    }

    return tasks;
  }

  public static Collection<URI> createHashFetchURL(String hostAndPort, ExecutionBlockId ebid,
                                       int partitionId, PartitionType type,
                                       List<IntermediateEntry> entries) {
    String scheme = "http://";
    StringBuilder urlPrefix = new StringBuilder(scheme);
    urlPrefix.append(hostAndPort).append("/?")
        .append("qid=").append(ebid.getQueryId().toString())
        .append("&sid=").append(ebid.getId())
        .append("&p=").append(partitionId)
        .append("&type=");
    if (type == PartitionType.HASH) {
      urlPrefix.append("h");
    } else if (type == PartitionType.RANGE) {
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

  public static Map<Integer, List<IntermediateEntry>> hashByKey(
      List<IntermediateEntry> entries) {
    Map<Integer, List<IntermediateEntry>> hashed = new HashMap<Integer, List<IntermediateEntry>>();
    for (IntermediateEntry entry : entries) {
      if (hashed.containsKey(entry.getPartitionId())) {
        hashed.get(entry.getPartitionId()).add(entry);
      } else {
        hashed.put(entry.getPartitionId(), TUtil.newList(entry));
      }
    }

    return hashed;
  }

  public static QueryUnit [] createEmptyNonLeafTasks(SubQuery subQuery, int num,
                                                     Fragment frag) {
    LogicalNode plan = subQuery.getBlock().getPlan();
    QueryUnit [] tasks = new QueryUnit[num];
    for (int i = 0; i < num; i++) {
      tasks[i] = new QueryUnit(QueryIdFactory.newQueryUnitId(subQuery.getId(), i),
          false, subQuery.getEventHandler());
      tasks[i].setFragment2(frag);
      tasks[i].setLogicalPlan(plan);
    }
    return tasks;
  }

  public static Map<String, List<IntermediateEntry>> hashByHost(
      List<IntermediateEntry> entries) {
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

  public static SubQuery setPartitionNumberForTwoPhase(SubQuery subQuery, final int n) {
    ExecutionBlock execBlock = subQuery.getBlock();
    Column[] keys = null;
    // if the next query is join,
    // set the partition number for the current logicalUnit
    // TODO: the union handling is required when a join has unions as its child
    ExecutionBlock parentBlock = execBlock.getParentBlock();
    if (parentBlock != null) {
      if (parentBlock.getStoreTableNode().getChild().getType() == NodeType.JOIN) {
        execBlock.getStoreTableNode().setPartitions(execBlock.getPartitionType(),
            execBlock.getStoreTableNode().getPartitionKeys(), n);
        keys = execBlock.getStoreTableNode().getPartitionKeys();
      }
    }

    StoreTableNode store = execBlock.getStoreTableNode();
    // set the partition number for group by and sort
    if (execBlock.getPartitionType() == PartitionType.HASH) {
      if (store.getChild().getType() == NodeType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode)store.getChild();
        keys = groupby.getGroupingColumns();
      }
    } else if (execBlock.getPartitionType() == PartitionType.RANGE) {
      if (store.getChild().getType() == NodeType.SORT) {
        SortNode sort = (SortNode)store.getChild();
        keys = new Column[sort.getSortKeys().length];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = sort.getSortKeys()[i].getSortKey();
        }
      }
    }
    if (keys != null) {
      if (keys.length == 0) {
        store.setPartitions(execBlock.getPartitionType(), new Column[]{}, 1);
      } else {
        store.setPartitions(execBlock.getPartitionType(), keys, n);
      }
    } else {
      store.setListPartition();
    }
    return subQuery;
  }
}
