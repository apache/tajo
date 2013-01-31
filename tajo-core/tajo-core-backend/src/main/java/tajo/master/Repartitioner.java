/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.QueryIdFactory;
import tajo.SubQueryId;
import tajo.catalog.CatalogService;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.catalog.TCatUtil;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.RangePartitionAlgorithm;
import tajo.engine.planner.UniformRangePartition;
import tajo.engine.planner.logical.GroupbyNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.engine.planner.logical.SortNode;
import tajo.engine.planner.logical.StoreTableNode;
import tajo.engine.utils.TupleUtil;
import tajo.exception.InternalException;
import tajo.master.QueryUnit.IntermediateEntry;
import tajo.master.SubQuery.PARTITION_TYPE;
import tajo.storage.Fragment;
import tajo.storage.TupleRange;
import tajo.util.TUtil;

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

  public static QueryUnit [] createJoinTasks(SubQuery subQuery)
      throws IOException {

    CatalogService catalog = subQuery.queryContext.getCatalog();

    ScanNode[] scans = subQuery.getScanNodes();
    Path tablePath;
    Fragment [] fragments = new Fragment[2];
    TableStat [] stats = new TableStat[2];

    // initialize variables from the child operators
    for (int i =0; i < 2; i++) {
      // TODO - temporarily tables should be stored in temporarily catalog for each query
      if (scans[i].getTableId().startsWith(SubQueryId.PREFIX)) {
        tablePath = subQuery.getStorageManager().getTablePath(scans[i].getTableId());
      } else {
        tablePath = catalog.getTableDesc(scans[i].getTableId()).getPath();
      }

      if (scans[i].isLocal()) { // it only requires a dummy fragment.
        fragments[i] = new Fragment(scans[i].getTableId(), tablePath,
            TCatUtil.newTableMeta(scans[i].getInSchema(), StoreType.CSV),
            0, 0, null);
      } else {
        fragments[i] = subQuery.getStorageManager().getSplits(scans[i].getTableId(),
            catalog.getTableDesc(scans[i].getTableId()).getMeta(),
            new Path(tablePath, "data")).get(0);
      }

      // Getting a table stat for each scan
      stats[i] = subQuery.getChildMaps().get(scans[i]).getStats();
    }

    // Assigning either fragments or fetch urls to query units
    QueryUnit [] tasks;
    if (scans[0].isBroadcast() || scans[1].isBroadcast()) {
      tasks = new QueryUnit[1];
      tasks[0] = new QueryUnit(QueryIdFactory.newQueryUnitId(subQuery.getId(), 0),
          false, subQuery.eventHandler);
      tasks[0].setLogicalPlan(subQuery.getLogicalPlan());
      tasks[0].setFragment(scans[0].getTableId(), fragments[0]);
      tasks[0].setFragment(scans[1].getTableId(), fragments[1]);
    } else {
      // The hash map is modeling as follows:
      // <Partition Id, <Table Name, Intermediate Data>>
      Map<Integer, Map<String, List<IntermediateEntry>>> hashEntries = new HashMap<>();

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
                    new HashMap<>();
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
      int desireJoinTaskVolumn = subQuery.queryContext.getConf().
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

      List<QueryUnit> filteredTasks = new ArrayList<>();
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
    QueryUnit [] tasks = new QueryUnit[taskNum];
    for (int i = 0; i < taskNum; i++) {
      tasks[i] = new QueryUnit(
          QueryIdFactory.newQueryUnitId(subQuery.getId(), i), subQuery.isLeafQuery(),
          subQuery.eventHandler);
      tasks[i].setLogicalPlan(subQuery.getLogicalPlan());
      for (Fragment fragment : fragments) {
        tasks[i].setFragment2(fragment);
      }
    }

    return tasks;
  }

  private static void addJoinPartition(QueryUnit task, SubQuery subQuery, int partitionId,
                                       Map<String, List<IntermediateEntry>> grouppedPartitions) {

    for (ScanNode scanNode : subQuery.getScanNodes()) {
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
            partitionId, PARTITION_TYPE.HASH,
            requestPerNode.getValue());
        fetchURIs.addAll(uris);
      }
      task.addFetches(scanNode.getTableId(), fetchURIs);
    }
  }

  private static QueryUnit newJoinTask(SubQuery subQuery, int partitionId,
      Fragment [] fragments,
      Map<String, List<IntermediateEntry>> grouppedPartitions) {

    QueryUnit task = new QueryUnit(
        QueryIdFactory.newQueryUnitId(subQuery.getId()), subQuery.isLeafQuery(),
        subQuery.eventHandler);
    task.setLogicalPlan(subQuery.getLogicalPlan());

    Map<String, Set<URI>> fetchURIsForEachRel = new HashMap<>();
    int i = 0;
    for (ScanNode scanNode : subQuery.getScanNodes()) {
      Map<String, List<IntermediateEntry>> mergedHashPartitionRequest =
          mergeHashPartitionRequest(grouppedPartitions.get(scanNode.getTableId()));
      Set<URI> fetchURIs = TUtil.newHashSet();
      for (Entry<String, List<IntermediateEntry>> requestPerNode
          : mergedHashPartitionRequest.entrySet()) {
        Collection<URI> uris = createHashFetchURL(requestPerNode.getKey(),
            subQuery.getChildQuery(scanNode).getId(),
            partitionId, PARTITION_TYPE.HASH,
            requestPerNode.getValue());
        fetchURIs.addAll(uris);
      }
      fetchURIsForEachRel.put(scanNode.getTableId(), fetchURIs);
      task.setFragment2(fragments[i++]);
    }

    task.setFetches(fetchURIsForEachRel);
    return task;
  }

  /**
   * This method merges the partition request associated with the pullserver's address.
   * It reduces the number of TCP connections.
   *
   * @return key: pullserver's address, value: a list of requests
   */
  private static Map<String, List<IntermediateEntry>> mergeHashPartitionRequest(
      List<IntermediateEntry> partitions) {
    Map<String, List<IntermediateEntry>> mergedPartitions = new HashMap<>();
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
    if (childSubQuery.getOutputType() == PARTITION_TYPE.HASH) {
      return createHashPartitionedTasks(subQuery, childSubQuery, maxNum);
    } else if (childSubQuery.getOutputType() == PARTITION_TYPE.RANGE) {
      return createRangePartitionedTasks(subQuery, childSubQuery, maxNum);
    } else {
      throw new InternalException("Cannot support partition type");
    }
  }

  public static QueryUnit [] createRangePartitionedTasks(SubQuery subQuery,
                                                         SubQuery childSubQuery,
                                                         int maxNum)
      throws InternalException {

    TableStat stat = childSubQuery.getStats();
    if (stat.getNumRows() == 0) {
      return new QueryUnit[0];
    }

    ScanNode scan = subQuery.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.sm.getTablePath(scan.getTableId());

    StoreTableNode store = (StoreTableNode) childSubQuery.getLogicalPlan();
    SortNode sort = (SortNode) store.getSubNode();
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
        TCatUtil.newTableMeta(scan.getInSchema(), StoreType.CSV),
        0, 0, null);

    List<String> basicFetchURIs = new ArrayList<>();

    for (QueryUnit qu : subQuery.getChildQuery(scan).getQueryUnits()) {
      for (IntermediateEntry p : qu.getIntermediateData()) {
        String uri = createBasicFetchUri(p.getPullHost(), p.getPullPort(),
            childSubQuery.getId(), p.taskId, p.attemptId);
        basicFetchURIs.add(uri);
      }
    }

    boolean ascendingFirstKey = sortSpecs[0].isAscending();
    SortedMap<TupleRange, Set<URI>> map;
    if (ascendingFirstKey) {
      map = new TreeMap<>();
    } else {
      map = new TreeMap<>(new TupleRange.DescendingTupleRangeComparator());
    }

    Set<URI> uris;
    try {
      for (int i = 0; i < ranges.length; i++) {
        uris = new HashSet<>();
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
                                           SubQueryId childSid,
                                           int taskId, int attemptId) {
    String scheme = "http://";
    StringBuilder sb = new StringBuilder(scheme);
    sb.append(hostName).append(":").append(port)
        .append("/?").append("sid=").append(childSid.getId())
        .append("&").append("ta=").append(taskId).append("_").append(attemptId)
        .append("&").append("p=0")
        .append("&").append("type=r");

    return sb.toString();
  }

  public static QueryUnit [] createHashPartitionedTasks(SubQuery subQuery,
                                                 SubQuery childSubQuery,
                                                 int maxNum) {

    TableStat stat = childSubQuery.getStats();
    if (stat.getNumRows() == 0) {
      return new QueryUnit[0];
    }

    ScanNode scan = subQuery.getScanNodes()[0];
    Path tablePath;
    tablePath = subQuery.sm.getTablePath(scan.getTableId());

    List<IntermediateEntry> partitions = new ArrayList<>();
    for (QueryUnit tasks : childSubQuery.getQueryUnits()) {
      if (tasks.getIntermediateData() != null) {
        partitions.addAll(tasks.getIntermediateData());
      }
    }

    Fragment frag = new Fragment(scan.getTableId(), tablePath,
        TCatUtil.newTableMeta(scan.getInSchema(), StoreType.CSV),
        0, 0, null);

    Map<Integer, List<IntermediateEntry>> hashed = hashByKey(partitions);
    Map<String, List<IntermediateEntry>> hashedByHost;
    Map<Integer, List<URI>> finalFetchURI = new HashMap<>();

    for (Entry<Integer, List<IntermediateEntry>> interm : hashed.entrySet()) {
      hashedByHost = hashByHost(interm.getValue());
      for (Entry<String, List<IntermediateEntry>> e : hashedByHost.entrySet()) {
        Collection<URI> uris = createHashFetchURL(e.getKey(), childSubQuery.getId(),
            interm.getKey(),
            childSubQuery.getStoreTableNode().getPartitionType(), e.getValue());

        if (finalFetchURI.containsKey(interm.getKey())) {
          finalFetchURI.get(interm.getKey()).addAll(uris);
        } else {
          finalFetchURI.put(interm.getKey(), TUtil.newList(uris));
        }
      }
    }

    GroupbyNode groupby = (GroupbyNode) childSubQuery.getStoreTableNode().
        getSubNode();
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

  public static Collection<URI> createHashFetchURL(String hostAndPort, SubQueryId childSid,
                                       int partitionId, PARTITION_TYPE type,
                                       List<IntermediateEntry> entries) {
    String scheme = "http://";
    StringBuilder urlPrefix = new StringBuilder(scheme);
    urlPrefix.append(hostAndPort)
        .append("/?").append("sid=").append(childSid.getId())
        .append("&").append("p=").append(partitionId)
        .append("&").append("type=");
    if (type == PARTITION_TYPE.HASH) {
      urlPrefix.append("h");
    } else if (type == PARTITION_TYPE.RANGE) {
      urlPrefix.append("r");
    }
    urlPrefix.append("&ta=");

    // If the get request is longer than 2000 characters,
    // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
    // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
    // The below code transforms a long request to multiple requests.
    List<String> taskIdsParams = new ArrayList<>();
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

    Collection<URI> fetchURLs = new ArrayList<>();
    for (String param : taskIdsParams) {
      fetchURLs.add(URI.create(urlPrefix + param));
    }

    return fetchURLs;
  }

  public static Map<Integer, List<IntermediateEntry>> hashByKey(
      List<IntermediateEntry> entries) {
    Map<Integer, List<IntermediateEntry>> hashed = new HashMap<>();
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
    QueryUnit [] tasks = new QueryUnit[num];
    for (int i = 0; i < num; i++) {
      tasks[i] = new QueryUnit(QueryIdFactory.newQueryUnitId(subQuery.getId(), i),
          false, subQuery.eventHandler);
      tasks[i].setFragment2(frag);
      tasks[i].setLogicalPlan(subQuery.getLogicalPlan());
    }
    return tasks;
  }

  public static Map<String, List<IntermediateEntry>> hashByHost(
      List<IntermediateEntry> entries) {
    Map<String, List<IntermediateEntry>> hashed = new HashMap<>();

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
}
