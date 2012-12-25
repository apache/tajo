/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.*;
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.ColumnStat;
import tajo.catalog.statistics.StatisticsUtil;
import tajo.catalog.statistics.TableStat;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.logical.*;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.event.*;
import tajo.storage.StorageManager;
import tajo.util.IndexUtil;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SubQuery implements EventHandler<SubQueryEvent> {

  private static final Log LOG = LogFactory.getLog(SubQuery.class);

  public enum PARTITION_TYPE {
    /** for hash partitioning */
    HASH,
    LIST,
    /** for map-side join */
    BROADCAST,
    /** for range partitioning */
    RANGE
  }

  private SubQueryId id;
  private LogicalNode plan = null;
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = null;
  private SubQuery next;
  private Map<ScanNode, SubQuery> childSubQueries;
  private PARTITION_TYPE outputType;
  private boolean hasJoinPlan;
  private boolean hasUnionPlan;
  private Priority priority;
  private TableStat stats;
  EventHandler eventHandler;
  final StorageManager sm;
  private final GlobalPlanner planner;
  private boolean isLeafQuery = false;
  TaskSchedulerImpl taskScheduler;
  QueryContext queryContext;

  volatile Map<QueryUnitId, QueryUnit> tasks = new ConcurrentHashMap<>();
  volatile Map<NodeId, Container> containers = new ConcurrentHashMap<>();


  private static ContainerLaunchTransition CONTAINER_LAUNCH_TRANSITION = new ContainerLaunchTransition();
  private StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent>
      stateMachine;

  private StateMachineFactory<SubQuery, SubQueryState,
      SubQueryEventType, SubQueryEvent> stateMachineFactory =
      new StateMachineFactory <SubQuery, SubQueryState,
          SubQueryEventType, SubQueryEvent> (SubQueryState.NEW)

          .addTransition(SubQueryState.NEW, EnumSet.of(SubQueryState.INIT, SubQueryState.FAILED, SubQueryState.SUCCEEDED),
              SubQueryEventType.SQ_INIT, new InitAndRequestContainer())

          .addTransition(SubQueryState.INIT, SubQueryState.CONTAINER_ALLOCATED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED, CONTAINER_LAUNCH_TRANSITION)

          .addTransition(SubQueryState.CONTAINER_ALLOCATED,
              EnumSet.of(SubQueryState.RUNNING, SubQueryState.FAILED,
                  SubQueryState.SUCCEEDED), SubQueryEventType.SQ_START, new StartTransition())
          .addTransition(SubQueryState.CONTAINER_ALLOCATED, SubQueryState.CONTAINER_ALLOCATED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED, CONTAINER_LAUNCH_TRANSITION)

          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED, CONTAINER_LAUNCH_TRANSITION)
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING, SubQueryEventType.SQ_START)
          .addTransition(SubQueryState.RUNNING, SubQueryState.RUNNING,
              SubQueryEventType.SQ_TASK_COMPLETED, new TaskCompletedTransition())
          .addTransition(SubQueryState.RUNNING, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_SUBQUERY_COMPLETED, new SubQueryCompleteTransition())
          .addTransition(SubQueryState.RUNNING, SubQueryState.FAILED,
              SubQueryEventType.SQ_FAILED, new InternalErrorTransition())

          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_START)
          .addTransition(SubQueryState.SUCCEEDED, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED)

          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              SubQueryEventType.SQ_START)
          .addTransition(SubQueryState.FAILED, SubQueryState.SUCCEEDED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED);


  private final Lock readLock;
  private final Lock writeLock;

  private int completedTaskCount = 0;
  
  public SubQuery(SubQueryId id, StorageManager sm, GlobalPlanner planner) {
    this.id = id;
    childSubQueries = new HashMap<>();
    scanlist = new ArrayList<>();
    hasJoinPlan = false;
    hasUnionPlan = false;
    this.sm = sm;
    this.planner = planner;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();


    stateMachine = stateMachineFactory.make(this);
  }

  public void setQueryContext(QueryContext context) {
    this.queryContext = context;
  }

  public void setEventHandler(EventHandler eventHandler) {
    this.eventHandler = eventHandler;
  }

  public boolean isLeafQuery() {
    return this.isLeafQuery;
  }

  public void setLeafQuery() {
    this.isLeafQuery = true;
  }

  public void addTask(QueryUnit task) {
    tasks.put(task.getId(), task);
  }
  
  public void setOutputType(PARTITION_TYPE type) {
    this.outputType = type;
  }

  public GlobalPlanner getPlanner() {
    return planner;
  }
  
  public void setLogicalPlan(LogicalNode plan) {
    hasJoinPlan = false;
    Preconditions.checkArgument(plan.getType() == ExprType.STORE
        || plan.getType() == ExprType.CREATE_INDEX);

    this.plan = plan;
    if (plan instanceof StoreTableNode) {
      store = (StoreTableNode) plan;      
    } else {
      store = (StoreTableNode) ((IndexWriteNode)plan).getSubNode();
    }
    
    LogicalNode node = plan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        s.add(s.size(), unary.getSubNode());
      } else if (node instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) node;
        if (binary.getType() == ExprType.JOIN) {
          hasJoinPlan = true;
        } else if (binary.getType() == ExprType.UNION) {
          hasUnionPlan = true;
        }
        s.add(s.size(), binary.getOuterNode());
        s.add(s.size(), binary.getInnerNode());
      } else if (node instanceof ScanNode) {
        scanlist.add((ScanNode)node);
      }
    }
  }

  public void abortSubQuery(SubQueryState finalState) {
    // TODO -
    // - committer.abortSubQuery(...)
    // - record SubQuery Finish Time
    // - CleanUp Tasks
    // - Record History

    eventHandler.handle(new SubQueryCompletedEvent(getId(), finalState));
  }

  public StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent> getStateMachine() {
    return this.stateMachine;
  }

  public boolean hasJoinPlan() {
    return this.hasJoinPlan;
  }

  public boolean hasUnionPlan() {
    return this.hasUnionPlan;
  }
  
  public void setParentQuery(SubQuery next) {
    this.next = next;
  }
  
  public void addChildQuery(ScanNode prevscan, SubQuery prev) {
    childSubQueries.put(prevscan, prev);
  }
  
  public void addChildQueries(Map<ScanNode, SubQuery> prevs) {
    this.childSubQueries.putAll(prevs);
  }
  
  public void setQueryUnits(List<QueryUnit> queryUnits) {
    for (QueryUnit task: queryUnits) {
      tasks.put(task.getId(), task);
    }
  }
  
  public void removeChildQuery(ScanNode scan) {
    scanlist.remove(scan);
    this.childSubQueries.remove(scan);
  }

  public void setPriority(int priority) {
    if (this.priority == null) {
      this.priority = new Priority(priority);
    }
  }

  public StorageManager getStorageManager() {
    return sm;
  }

  public void setStats(TableStat stat) {
    this.stats = stat;
  }
  
  public SubQuery getParentQuery() {
    return this.next;
  }
  
  public boolean hasChildQuery() {
    return !this.childSubQueries.isEmpty();
  }
  
  public Iterator<SubQuery> getChildIterator() {
    return this.childSubQueries.values().iterator();
  }
  
  public Collection<SubQuery> getChildQueries() {
    return this.childSubQueries.values();
  }
  
  public Map<ScanNode, SubQuery> getChildMaps() {
    return this.childSubQueries;
  }
  
  public SubQuery getChildQuery(ScanNode scanForChild) {
    return this.childSubQueries.get(scanForChild);
  }
  
  public String getOutputName() {
    return this.store.getTableName();
  }
  
  public PARTITION_TYPE getOutputType() {
    return this.outputType;
  }
  
  public Schema getOutputSchema() {
    return this.store.getOutSchema();
  }
  
  public StoreTableNode getStoreTableNode() {
    return this.store;
  }
  
  public ScanNode[] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }
  
  public LogicalNode getLogicalPlan() {
    return this.plan;
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public QueryUnit[] getQueryUnits() {
    // TODO - to be changed to unified getter
    return tasks.values().toArray(new QueryUnit[tasks.size()]);
  }
  
  public QueryUnit getQueryUnit(QueryUnitId qid) {
    return tasks.get(qid);
  }

  public Priority getPriority() {
    return this.priority;
  }

  public TableStat getStats() {
    return this.stats;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.id);
/*    sb.append(" plan: " + plan.toString());
    sb.append("next: " + next + " childSubQueries:");
    Iterator<SubQuery> it = getChildIterator();
    while (it.hasNext()) {
      sb.append(" " + it.next());
    }*/
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof SubQuery) {
      SubQuery other = (SubQuery)o;
      return this.id.equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.id.hashCode();
  }
  
  public int compareTo(SubQuery other) {
    return this.id.compareTo(other.id);
  }

  public SubQueryState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private void finishUnionUnit() throws IOException {
    // write meta and continue
    TableStat stat = generateUnionStat(this);
    setStats(stat);
    writeStat(this, stat);
    //unit.setState(QueryStatus.QUERY_FINISHED);
  }

  private static TableStat generateUnionStat(SubQuery unit) {
    TableStat stat = new TableStat();
    TableStat childStat;
    long avgRows = 0, numBytes = 0, numRows = 0;
    int numBlocks = 0, numPartitions = 0;
    List<ColumnStat> columnStats = Lists.newArrayList();

    for (SubQuery child : unit.getChildQueries()) {
      childStat = child.getStats();
      avgRows += childStat.getAvgRows();
      columnStats.addAll(childStat.getColumnStats());
      numBlocks += childStat.getNumBlocks();
      numBytes += childStat.getNumBytes();
      numPartitions += childStat.getNumPartitions();
      numRows += childStat.getNumRows();
    }
    stat.setColumnStats(columnStats);
    stat.setNumBlocks(numBlocks);
    stat.setNumBytes(numBytes);
    stat.setNumPartitions(numPartitions);
    stat.setNumRows(numRows);
    stat.setAvgRows(avgRows);
    return stat;
  }

  public void cleanUp() {
    if (hasUnionPlan()) {
      try {
        // write meta and continue
        TableStat stat = generateUnionStat(this);
        setStats(stat);
        writeStat(this, stat);
        //unit.setState(QueryStatus.QUERY_FINISHED);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      TableStat stat = generateStat();
      try {
        writeStat(this, stat);
      } catch (IOException e) {
      }
      setStats(stat);
    }
  }


  private static class InitAndRequestContainer implements MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      subQuery.taskScheduler = new TaskSchedulerImpl(subQuery.queryContext);
      subQuery.taskScheduler.init(subQuery.queryContext.getConf());
      subQuery.taskScheduler.start();

      try {
        // if subquery is dummy, which means it requires only a logical step
        // instead of actual query. An 'union all' is an example of
        // a dummy subquery.
        if (subQuery.hasUnionPlan()) {
          subQuery.finishUnionUnit();
          subQuery.cleanUp();
          TableMeta meta = new TableMetaImpl(subQuery.getOutputSchema(),
              StoreType.CSV, new Options(), subQuery.getStats());
          subQuery.eventHandler.handle(new SubQuerySucceeEvent(subQuery.getId(),
              meta));
          return SubQueryState.SUCCEEDED;


        } else {
          QueryUnit [] tasks;
          // TODO - should be improved
          if (subQuery.isLeafQuery() && subQuery.getScanNodes().length == 1) {
            int numTasks = subQuery.getPlanner().getTaskNum(subQuery);
            subQuery.getPlanner().setPartitionNumberForTwoPhase(subQuery, numTasks);
            tasks = subQuery.getPlanner().createLeafTasks(subQuery);
          } else if (subQuery.getScanNodes().length > 1) {
            int numTasks = subQuery.getPlanner().getTaskNum(subQuery);
            subQuery.getPlanner().setPartitionNumberForTwoPhase(subQuery, numTasks);
            tasks = Repartitioner.createJoinTasks(subQuery, numTasks);
          } else {
            int numTasks = subQuery.getPlanner().getTaskNum(subQuery);
            subQuery.getPlanner().setPartitionNumberForTwoPhase(subQuery, numTasks);
            tasks = Repartitioner.createNonLeafTask(subQuery,
                subQuery.getChildIterator().next(), numTasks);
          }
          for (QueryUnit task : tasks) {
            subQuery.addTask(task);
          }
          LOG.info("Create " + tasks.length + " Tasks");

          // if there is no tasks
          if (subQuery.tasks.size() == 0) {
            subQuery.cleanUp();
            TableMeta meta = new TableMetaImpl(subQuery.getOutputSchema(),
                StoreType.CSV, new Options(), subQuery.getStats());
            subQuery.eventHandler.handle(new SubQuerySucceeEvent(subQuery.getId(),
                meta));
            return SubQueryState.SUCCEEDED;

          } else {
            // the second condition is for some workaround code for join
            if (subQuery.isLeafQuery() && subQuery.getScanNodes().length == 1) {
              Map<String, Integer> requestMap = new HashMap<>();
              for (QueryUnit task : tasks) {
                for (String host : task.getDataLocations()) {
                  if (requestMap.containsKey(host)) {
                    requestMap.put(host, requestMap.get(host) + 1);
                  } else {
                    requestMap.put(host, 1);
                  }
                }
              }

              final Resource resource =
                  RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
                      Resource.class);
              resource.setMemory(2000);
              org.apache.hadoop.yarn.api.records.Priority priority =
                  RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
                      org.apache.hadoop.yarn.api.records.Priority.class);
              priority.setPriority(5);
              GrouppedContainerAllocatorEvent event =
                  new GrouppedContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
                      subQuery.getId(), priority, resource, requestMap, subQuery.isLeafQuery(), 0.0f);
              subQuery.eventHandler.handle(event);
            } else {
              final Resource resource =
                  RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
                      Resource.class);
              resource.setMemory(2000);
              org.apache.hadoop.yarn.api.records.Priority priority =
                  RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
                      org.apache.hadoop.yarn.api.records.Priority.class);
              priority.setPriority(3);
              ContainerAllocationEvent event =
                  new ContainerAllocationEvent(ContainerAllocatorEventType.CONTAINER_REQ,
                      subQuery.getId(), priority, resource, tasks.length, subQuery.isLeafQuery(), 0.0f);
              subQuery.eventHandler.handle(event);
            }
          }
        }
        return  SubQueryState.INIT;
      } catch (Exception e) {
        LOG.warn("SubQuery (" + subQuery.getId() + ") failed", e);
        subQuery.eventHandler.handle(
            new QueryDiagnosticsUpdateEvent(subQuery.getId().getQueryId(), e.getMessage()));
        subQuery.eventHandler.handle(
            new SubQueryCompletedEvent(subQuery.getId(), SubQueryState.FAILED));
        return SubQueryState.FAILED;
      }
    }
  }

  private static class ContainerLaunchTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery, SubQueryEvent event) {
      SubQueryContainerAllocationEvent allocationEvent =
          (SubQueryContainerAllocationEvent) event;
      for (Container container : allocationEvent.getAllocatedContainer()) {
        NodeId nodeId = container.getNodeId();
        String containerMgrAddr = nodeId.getHost() + ":" + nodeId.getPort();
        subQuery.containers.put(nodeId, container);

        subQuery.eventHandler.handle(
            new TaskRunnerLaunchEvent(
                subQuery.getId(),
                container,
                container.getResource()));
      }

      subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(),
          SubQueryEventType.SQ_START));
    }
  }

  private static class StartTransition implements
      MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {
      try {
        initOutputDir(subQuery.getStorageManager(), subQuery.getOutputName(),
            subQuery.getOutputType());

        for (QueryUnitId taskId : subQuery.tasks.keySet()) {
          subQuery.eventHandler.handle(new TaskEvent(taskId, TaskEventType.T_SCHEDULE));
        }

        return  SubQueryState.RUNNING;
      } catch (Exception e) {
        LOG.warn("SubQuery (" + subQuery.getId() + ") failed", e);
        return SubQueryState.FAILED;
      }
    }

    private void initOutputDir(StorageManager sm, String outputName,
                               PARTITION_TYPE type)
        throws IOException {
      switch (type) {
        case HASH:
          Path tablePath = sm.getTablePath(outputName);
          sm.getFileSystem().mkdirs(tablePath);
          LOG.info("Table path " + sm.getTablePath(outputName).toString()
              + " is initialized for " + outputName);
          break;
        case RANGE: // TODO - to be improved

        default:
          if (!sm.getFileSystem().exists(sm.getTablePath(outputName))) {
            sm.initTableBase(null, outputName);
            LOG.info("Table path " + sm.getTablePath(outputName).toString()
                + " is initialized for " + outputName);
          }
      }
    }
  }

  private class TaskCompletedTransition implements
      SingleArcTransition<SubQuery, SubQueryEvent> {


    @Override
    public void transition(SubQuery subQuery,
                                     SubQueryEvent event) {
      subQuery.completedTaskCount++;
//      SubQueryTaskEvent taskEvent = (SubQueryTaskEvent) event;
//      QueryUnit task = subQuery.getQueryUnit(taskEvent.getTaskId());

      LOG.info(getId() + " SubQuery Succeeded " + completedTaskCount + "/"
          + subQuery.tasks.size());
      if (subQuery.completedTaskCount == subQuery.tasks.size()) {
        subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(),
            SubQueryEventType.SQ_SUBQUERY_COMPLETED));
      }
    }
  }

  private static class SubQueryCompleteTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      // TODO - Commit subQuery & do cleanup
      // TODO - records succeeded, failed, killed completed task
      // TODO - records metrics

      for (Entry<NodeId, Container> entry : subQuery.containers.entrySet()) {
        subQuery.eventHandler.handle(new TaskRunnerStopEvent(subQuery.getId(),
            entry.getValue()));
      }
      subQuery.cleanUp();
      subQuery.taskScheduler.stop();
      TableMeta meta = new TableMetaImpl(subQuery.getOutputSchema(),
          StoreType.CSV, new Options(), subQuery.getStats());

      subQuery.eventHandler.handle(new SubQuerySucceeEvent(subQuery.getId(),
          meta));
    }
  }

  SubQueryState finished(SubQueryState state) {
    // TODO - record the state to metric
    return state;
  }

  class InternalErrorTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {

    }
  }

  private TableStat generateStat() {
    List<TableStat> stats = Lists.newArrayList();
    for (QueryUnit unit : getQueryUnits()) {
      stats.add(unit.getStats());
    }
    TableStat tableStat = StatisticsUtil.aggregateTableStat(stats);
    return tableStat;
  }

  private void writeStat(SubQuery subQuery, TableStat stat)
      throws IOException {

    if (subQuery.getLogicalPlan().getType() == ExprType.CREATE_INDEX) {
      IndexWriteNode index = (IndexWriteNode) subQuery.getLogicalPlan();
      Path indexPath = new Path(sm.getTablePath(index.getTableName()), "index");
      TableMeta meta;
      if (sm.getFileSystem().exists(new Path(indexPath, ".meta"))) {
        meta = sm.getTableMeta(indexPath);
      } else {
        meta = TCatUtil
            .newTableMeta(subQuery.getOutputSchema(), StoreType.CSV);
      }
      String indexName = IndexUtil.getIndexName(index.getTableName(),
          index.getSortSpecs());
      String json = GsonCreator.getInstance().toJson(index.getSortSpecs());
      meta.putOption(indexName, json);

      sm.writeTableMeta(indexPath, meta);

    } else {
      TableMeta meta = TCatUtil.newTableMeta(subQuery.getOutputSchema(),
          StoreType.CSV);
      meta.setStat(stat);
      sm.writeTableMeta(sm.getTablePath(subQuery.getOutputName()), meta);
    }
  }

  private void finalizePrevSubQuery(SubQuery subQuery)
      throws Exception {
    SubQuery prevSubQuery;
    for (ScanNode scan : subQuery.getScanNodes()) {
      prevSubQuery = subQuery.getChildQuery(scan);
      if (prevSubQuery.getStoreTableNode().getSubNode().getType() != ExprType.UNION) {
        for (QueryUnit unit : prevSubQuery.getQueryUnits()) {
          //sendCommand(unit.getLastAttempt(), CommandType.FINALIZE);
        }
      }
    }
  }

  @Override
  public void handle(SubQueryEvent event) {
    //if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getSubQueryId() + " of type " + event.getType());
    //}

    try {
      writeLock.lock();
      SubQueryState oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new SubQueryEvent(this.id,
            SubQueryEventType.SQ_INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
        if (oldState != getState()) {
          LOG.debug(id + " SubQuery Transitioned from " + oldState + " to "
              + getState());
        }
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
