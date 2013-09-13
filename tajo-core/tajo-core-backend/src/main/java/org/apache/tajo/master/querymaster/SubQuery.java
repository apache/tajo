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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.QueryUnitId;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStat;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.master.*;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.event.*;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.Fragment;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.conf.TajoConf.ConfVars;


/**
 * SubQuery plays a role in controlling an ExecutionBlock and is a finite state machine.
 */
public class SubQuery implements EventHandler<SubQueryEvent> {

  private static final Log LOG = LogFactory.getLog(SubQuery.class);

  private QueryContext queryContext;
  private ExecutionBlock block;
  private int priority;
  private TableMeta meta;
  private EventHandler eventHandler;
  private final AbstractStorageManager sm;
  private TaskSchedulerImpl taskScheduler;
  private QueryMasterTask.QueryMasterTaskContext context;

  private long startTime;
  private long finishTime;

  volatile Map<QueryUnitId, QueryUnit> tasks = new ConcurrentHashMap<QueryUnitId, QueryUnit>();
  volatile Map<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

  private static ContainerLaunchTransition CONTAINER_LAUNCH_TRANSITION = new ContainerLaunchTransition();
  private StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent>
      stateMachine;

  private StateMachineFactory<SubQuery, SubQueryState,
      SubQueryEventType, SubQueryEvent> stateMachineFactory =
      new StateMachineFactory <SubQuery, SubQueryState,
          SubQueryEventType, SubQueryEvent> (SubQueryState.NEW)

          .addTransition(SubQueryState.NEW,
              EnumSet.of(SubQueryState.INIT, SubQueryState.FAILED, SubQueryState.SUCCEEDED),
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
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              SubQueryEventType.SQ_CONTAINER_ALLOCATED)
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
                 SubQueryEventType.SQ_FAILED)
          .addTransition(SubQueryState.FAILED, SubQueryState.FAILED,
              SubQueryEventType.SQ_INTERNAL_ERROR);


  private final Lock readLock;
  private final Lock writeLock;

  private int completedTaskCount = 0;

  public SubQuery(QueryMasterTask.QueryMasterTaskContext context, ExecutionBlock block, AbstractStorageManager sm) {
    this.context = context;
    this.block = block;
    this.sm = sm;
    this.eventHandler = context.getEventHandler();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
  }

  public static boolean isRunningState(SubQueryState state) {
    return state == SubQueryState.INIT || state == SubQueryState.NEW ||
        state == SubQueryState.CONTAINER_ALLOCATED || state == SubQueryState.RUNNING;
  }

  public QueryMasterTask.QueryMasterTaskContext getContext() {
    return context;
  }

  public EventHandler getEventHandler() {
    return eventHandler;
  }

  public TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }

  public void setStartTime() {
    startTime = context.getClock().getTime();
  }

  @SuppressWarnings("UnusedDeclaration")
  public long getStartTime() {
    return this.startTime;
  }

  public void setFinishTime() {
    finishTime = context.getClock().getTime();
  }

  @SuppressWarnings("UnusedDeclaration")
  public long getFinishTime() {
    return this.finishTime;
  }

  public float getProgress() {
    readLock.lock();
    try {
      if (getState() == SubQueryState.NEW) {
        return 0;
      } else {
        if (completedTaskCount == 0) {
          return 0.0f;
        } else {
          return (float)completedTaskCount / (float)tasks.size();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  public ExecutionBlock getBlock() {
    return block;
  }

  public void addTask(QueryUnit task) {
    tasks.put(task.getId(), task);
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

  public void setPriority(int priority) {
    this.priority = priority;
  }


  public int getPriority() {
    return this.priority;
  }

  public AbstractStorageManager getStorageManager() {
    return sm;
  }
  
  public SubQuery getChildQuery(ScanNode scanForChild) {
    return context.getSubQuery(block.getChildBlock(scanForChild).getId());
  }
  
  public ExecutionBlockId getId() {
    return block.getId();
  }
  
  public QueryUnit[] getQueryUnits() {
    return tasks.values().toArray(new QueryUnit[tasks.size()]);
  }
  
  public QueryUnit getQueryUnit(QueryUnitId qid) {
    return tasks.get(qid);
  }

  public void setTableMeta(TableMeta meta) {
    this.meta = meta;
  }

  @SuppressWarnings("UnusedDeclaration")
  public TableMeta getTableMeta() {
    return meta;
  }

  public TableStat getTableStat() {
    return this.meta.getStat();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getId());
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof SubQuery) {
      SubQuery other = (SubQuery)o;
      return getId().equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return getId().hashCode();
  }
  
  public int compareTo(SubQuery other) {
    return getId().compareTo(other.getId());
  }

  public SubQueryState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static TableStat computeStatFromUnionBlock(SubQuery unit) {
    TableStat stat = new TableStat();
    TableStat childStat;
    long avgRows = 0, numBytes = 0, numRows = 0;
    int numBlocks = 0, numPartitions = 0;
    List<ColumnStat> columnStats = Lists.newArrayList();

    Iterator<ExecutionBlock> it = unit.getBlock().getChildBlocks().iterator();
    while (it.hasNext()) {
      ExecutionBlock block = it.next();
      SubQuery childSubQuery = unit.context.getSubQuery(block.getId());
      childStat = childSubQuery.getTableStat();
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

  public TableMeta buildTableMeta() throws IOException {
    finishTime = context.getClock().getTime();

    TableStat stat;
    if (block.hasUnion()) {
      stat = computeStatFromUnionBlock(this);
    } else {
      stat = computeStatFromTasks();
    }

    StoreTableNode storeTableNode = getBlock().getStoreTableNode();
    TableMeta meta = toTableMeta(storeTableNode);
    meta.setStat(stat);
    return meta;
  }

  private TableStat computeStatFromTasks() {
    List<TableStat> stats = Lists.newArrayList();
    for (QueryUnit unit : getQueryUnits()) {
      stats.add(unit.getStats());
    }
    TableStat tableStat = StatisticsUtil.aggregateTableStat(stats);
    return tableStat;
  }

  private TableMeta writeStat(SubQuery subQuery, TableStat stat)
      throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
    StoreTableNode storeTableNode = execBlock.getStoreTableNode();
    TableMeta meta = toTableMeta(storeTableNode);
    meta.setStat(stat);
    //sm.writeTableMeta(sm.getTablePath(execBlock.getOutputName()), meta);
    return meta;
  }

  private static TableMeta toTableMeta(StoreTableNode store) {
    if (store.hasOptions()) {
      return CatalogUtil.newTableMeta(store.getOutSchema(),
          store.getStorageType(), store.getOptions());
    } else {
      return CatalogUtil.newTableMeta(store.getOutSchema(),
          store.getStorageType());
    }
  }

  private void stopScheduler() {
    // If there are launched TaskRunners, send the 'shouldDie' message to all r
    // via received task requests.
    if (taskScheduler != null) {
      taskScheduler.stop();
    }
  }

  private void releaseContainers() {
    // If there are still live TaskRunners, try to kill the containers.
    eventHandler.handle(new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_CLEANUP ,getId(),
        containers.values()));
  }

  private void finish() {
    TableMeta meta = null;
    try {
      meta = buildTableMeta();
    } catch (IOException e) {
      e.printStackTrace();
    }

    setTableMeta(meta);
    setFinishTime();
    eventHandler.handle(new SubQuerySucceeEvent(getId(), meta));
  }

  @Override
  public void handle(SubQueryEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getSubQueryId() + " of type " + event.getType() + ", preState=" + getState());
    }

    try {
      writeLock.lock();
      SubQueryState oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new SubQueryEvent(getId(),
            SubQueryEventType.SQ_INTERNAL_ERROR));
      }

      // notify the eventhandler of state change
      if (LOG.isDebugEnabled()) {
        if (oldState != getState()) {
          LOG.debug(getId() + " SubQuery Transitioned from " + oldState + " to "
              + getState());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void handleTaskRequestEvent(TaskRequestEvent event) {
    taskScheduler.handleTaskRequestEvent(event);
  }

  private static class InitAndRequestContainer implements MultipleArcTransition<SubQuery,
      SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery, SubQueryEvent subQueryEvent) {
      subQuery.setStartTime();
      ExecutionBlock execBlock = subQuery.getBlock();
      SubQueryState state;

      try {
        // Union operator does not require actual query processing. It is performed logically.
        if (execBlock.hasUnion()) {
          subQuery.finish();
          state = SubQueryState.SUCCEEDED;
        } else {
          setRepartitionIfNecessary(subQuery);
          createTasks(subQuery);

          if (subQuery.tasks.size() == 0) { // if there is no tasks
            subQuery.finish();
            return SubQueryState.SUCCEEDED;
          } else {
            initTaskScheduler(subQuery);
            allocateContainers(subQuery);
            return SubQueryState.INIT;
          }
        }
      } catch (Exception e) {
        LOG.warn("SubQuery (" + subQuery.getId() + ") failed", e);
        subQuery.eventHandler.handle(
            new QueryDiagnosticsUpdateEvent(subQuery.getId().getQueryId(), e.getMessage()));
        subQuery.eventHandler.handle(
            new SubQueryCompletedEvent(subQuery.getId(), SubQueryState.FAILED));
        return SubQueryState.FAILED;
      }

      return state;
    }

    private void initTaskScheduler(SubQuery subQuery) {
      subQuery.taskScheduler = new TaskSchedulerImpl(subQuery.context);
      subQuery.taskScheduler.init(subQuery.context.getConf());
      subQuery.taskScheduler.start();
    }

    /**
     * If a parent block requires a repartition operation, the method sets proper repartition
     * methods and the number of partitions to a given subquery.
     */
    private static void setRepartitionIfNecessary(SubQuery subQuery) {
      if (subQuery.getBlock().hasParentBlock()) {
        int numTasks = calculatePartitionNum(subQuery);
        Repartitioner.setPartitionNumberForTwoPhase(subQuery, numTasks);
      }
    }

    /**
     * Getting the desire number of partitions according to the volume of input data.
     * This method is only used to determine the partition key number of hash join or aggregation.
     *
     * @param subQuery
     * @return
     */
    public static int calculatePartitionNum(SubQuery subQuery) {
      TajoConf conf = subQuery.context.getConf();
      ExecutionBlock parent = subQuery.getBlock().getParentBlock();

      GroupbyNode grpNode = null;
      if (parent != null) {
        grpNode = (GroupbyNode) PlannerUtil.findTopNode(
            parent.getPlan(), NodeType.GROUP_BY);
      }

      // Is this subquery the first step of join?
      if (parent != null && parent.getScanNodes().length == 2) {
        Iterator<ExecutionBlock> child = parent.getChildBlocks().iterator();

        // for inner
        ExecutionBlock outer = child.next();
        long outerVolume = getInputVolume(subQuery.context, outer);

        // for inner
        ExecutionBlock inner = child.next();
        long innerVolume = getInputVolume(subQuery.context, inner);
        LOG.info("Outer volume: " + Math.ceil((double)outerVolume / 1048576));
        LOG.info("Inner volume: " + Math.ceil((double)innerVolume / 1048576));

        long smaller = Math.min(outerVolume, innerVolume);

        int mb = (int) Math.ceil((double)smaller / 1048576);
        LOG.info("Smaller Table's volume is approximately " + mb + " MB");
        // determine the number of task
        int taskNum = (int) Math.ceil((double)mb /
            conf.getIntVar(ConfVars.JOIN_PARTITION_VOLUME));
        LOG.info("The determined number of join partitions is " + taskNum);
        return taskNum;

        // Is this subquery the first step of group-by?
      } else if (grpNode != null) {

        if (grpNode.getGroupingColumns().length == 0) {
          return 1;
        } else {
          long volume = getInputVolume(subQuery.context, subQuery.block);

          int mb = (int) Math.ceil((double)volume / 1048576);
          LOG.info("Table's volume is approximately " + mb + " MB");
          // determine the number of task
          int taskNum = (int) Math.ceil((double)mb /
              conf.getIntVar(ConfVars.AGGREGATION_PARTITION_VOLUME));
          LOG.info("The determined number of aggregation partitions is " + taskNum);
          return taskNum;
        }
      } else {
        LOG.info("============>>>>> Unexpected Case! <<<<<================");
        long volume = getInputVolume(subQuery.context, subQuery.block);

        int mb = (int) Math.ceil((double)volume / 1048576);
        LOG.info("Table's volume is approximately " + mb + " MB");
        // determine the number of task per 128MB
        int taskNum = (int) Math.ceil((double)mb / 128);
        LOG.info("The determined number of partitions is " + taskNum);
        return taskNum;
      }
    }

    private static void createTasks(SubQuery subQuery) throws IOException {
      ExecutionBlock execBlock = subQuery.getBlock();
      QueryUnit [] tasks;
      if (execBlock.isLeafBlock() && execBlock.getScanNodes().length == 1) { // Case 1: Just Scan
        tasks = createLeafTasks(subQuery);

      } else if (execBlock.getScanNodes().length > 1) { // Case 2: Join
        tasks = Repartitioner.createJoinTasks(subQuery);

      } else { // Case 3: Others (Sort or Aggregation)
        int numTasks = getNonLeafTaskNum(subQuery);
        ExecutionBlockId childId = subQuery.getBlock().getChildBlocks().iterator().next().getId();
        SubQuery child = subQuery.context.getSubQuery(childId);
        tasks = Repartitioner.createNonLeafTask(subQuery, child, numTasks);
      }

      LOG.info("Create " + tasks.length + " Tasks");

      for (QueryUnit task : tasks) {
        subQuery.addTask(task);
      }
    }

    /**
     * Getting the desire number of tasks according to the volume of input data
     *
     * @param subQuery
     * @return
     */
    public static int getNonLeafTaskNum(SubQuery subQuery) {
      // Getting intermediate data size
      long volume = getInputVolume(subQuery.context, subQuery.getBlock());

      int mb = (int) Math.ceil((double)volume / 1048576);
      LOG.info("Table's volume is approximately " + mb + " MB");
      // determine the number of task per 64MB
      int maxTaskNum = (int) Math.ceil((double)mb / 64);
      LOG.info("The determined number of non-leaf tasks is " + maxTaskNum);
      return maxTaskNum;
    }

    public static long getInputVolume(QueryMasterTask.QueryMasterTaskContext context, ExecutionBlock execBlock) {
      Map<String, TableDesc> tableMap = context.getTableDescMap();
      if (execBlock.isLeafBlock()) {
        ScanNode outerScan = execBlock.getScanNodes()[0];
        TableStat stat = tableMap.get(outerScan.getFromTable().getTableName()).getMeta().getStat();
        return stat.getNumBytes();
      } else {
        long aggregatedVolume = 0;
        for (ExecutionBlock childBlock : execBlock.getChildBlocks()) {
          SubQuery subquery = context.getSubQuery(childBlock.getId());
          aggregatedVolume += subquery.getTableStat().getNumBytes();
        }

        return aggregatedVolume;
      }
    }

    public static void allocateContainers(SubQuery subQuery) {
      ExecutionBlock execBlock = subQuery.getBlock();
      QueryUnit [] tasks = subQuery.getQueryUnits();

      int numRequest = subQuery.getContext().getResourceAllocator().calculateNumRequestContainers(
          subQuery.getContext().getQueryMasterContext().getWorkerContext(), tasks.length
      );

      final Resource resource = Records.newRecord(Resource.class);

      resource.setMemory(2000);

      LOG.info("Request Container for " + subQuery.getId() + " containers=" + numRequest);

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(subQuery.getPriority());
      ContainerAllocationEvent event =
          new ContainerAllocationEvent(ContainerAllocatorEventType.CONTAINER_REQ,
              subQuery.getId(), priority, resource, numRequest,
              execBlock.isLeafBlock(), 0.0f);
      subQuery.eventHandler.handle(event);
    }

    private static QueryUnit [] createLeafTasks(SubQuery subQuery) throws IOException {
      ExecutionBlock execBlock = subQuery.getBlock();
      ScanNode[] scans = execBlock.getScanNodes();
      Preconditions.checkArgument(scans.length == 1, "Must be Scan Query");
      TableMeta meta;
      Path inputPath;

      ScanNode scan = scans[0];
      TableDesc desc = subQuery.context.getTableDescMap().get(scan.getFromTable().getTableName());
      inputPath = desc.getPath();
      meta = desc.getMeta();

      // TODO - should be change the inner directory
      List<Fragment> fragments = subQuery.getStorageManager().getSplits(scan.getTableId(), meta, inputPath);

      QueryUnit queryUnit;
      List<QueryUnit> queryUnits = new ArrayList<QueryUnit>();

      int i = 0;
      for (Fragment fragment : fragments) {
        queryUnit = newQueryUnit(subQuery, i++, fragment);
        queryUnits.add(queryUnit);
      }

      return queryUnits.toArray(new QueryUnit[queryUnits.size()]);
    }

    private static QueryUnit newQueryUnit(SubQuery subQuery, int taskId, Fragment fragment) {
      ExecutionBlock execBlock = subQuery.getBlock();
      QueryUnit unit = new QueryUnit(
          QueryIdFactory.newQueryUnitId(subQuery.getId(), taskId), execBlock.isLeafBlock(),
          subQuery.eventHandler);
      unit.setLogicalPlan(execBlock.getPlan());
      unit.setFragment2(fragment);
      return unit;
    }
  }

  int i = 0;
  private static class ContainerLaunchTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery, SubQueryEvent event) {
      SubQueryContainerAllocationEvent allocationEvent =
          (SubQueryContainerAllocationEvent) event;
      for (Container container : allocationEvent.getAllocatedContainer()) {
        ContainerId cId = container.getId();
        if (subQuery.containers.containsKey(cId)) {
          LOG.info(">>>>>>>>>>>> Duplicate Container! <<<<<<<<<<<");
        }
        subQuery.containers.put(cId, container);
        // TODO - This is debugging message. Should be removed
        subQuery.i++;
      }
      LOG.info("SubQuery (" + subQuery.getId() + ") has " + subQuery.i + " containers!");
      subQuery.eventHandler.handle(
          new TaskRunnerGroupEvent(EventType.CONTAINER_REMOTE_LAUNCH,
              subQuery.getId(), allocationEvent.getAllocatedContainer()));

      subQuery.eventHandler.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_START));
    }
  }

  private static class StartTransition implements
      MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {
      // schedule tasks
      try {
        for (QueryUnitId taskId : subQuery.tasks.keySet()) {
          subQuery.eventHandler.handle(new TaskEvent(taskId, TaskEventType.T_SCHEDULE));
        }

        return  SubQueryState.RUNNING;
      } catch (Exception e) {
        LOG.warn("SubQuery (" + subQuery.getId() + ") failed", e);
        return SubQueryState.FAILED;
      }
    }
  }

  private static class TaskCompletedTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                                     SubQueryEvent event) {
      subQuery.completedTaskCount++;
      SubQueryTaskEvent taskEvent = (SubQueryTaskEvent) event;
      QueryUnitAttempt task = subQuery.getQueryUnit(taskEvent.getTaskId()).getSuccessfulAttempt();

      LOG.info(subQuery.getId() + " SubQuery Succeeded " + subQuery.completedTaskCount + "/"
          + subQuery.tasks.size() + " on " + task.getHost() + ":" + task.getPort());
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
      LOG.info("SubQuery finished:" + subQuery.getId());
      subQuery.stopScheduler();
      subQuery.releaseContainers();
      subQuery.finish();
    }
  }

  private static class InternalErrorTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {

    }
  }
}
