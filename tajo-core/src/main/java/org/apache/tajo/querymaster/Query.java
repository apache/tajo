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

import com.google.common.collect.Maps;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.proto.CatalogProtos.UpdateTableStatsProto;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.event.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.util.history.StageHistory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Query implements EventHandler<QueryEvent> {
  private static final Log LOG = LogFactory.getLog(Query.class);

  // Facilities for Query
  private final TajoConf systemConf;
  private final Clock clock;
  private String queryStr;
  private Map<ExecutionBlockId, Stage> stages;
  private final EventHandler eventHandler;
  private final MasterPlan plan;
  QueryMasterTask.QueryMasterTaskContext context;
  private ExecutionBlockCursor cursor;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long finishTime;
  private TableDesc resultDesc;
  private int completedStagesCount = 0;
  private int successedStagesCount = 0;
  private int killedStagesCount = 0;
  private int failedStagesCount = 0;
  private int erroredStagesCount = 0;
  private final List<String> diagnostics = new ArrayList<String>();

  // Internal Variables
  private final Lock readLock;
  private final Lock writeLock;
  private int priority = 100;

  // State Machine
  private final StateMachine<QueryState, QueryEventType, QueryEvent> stateMachine;
  private QueryState queryState;

  // Transition Handler
  private static final SingleArcTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final StageCompletedTransition STAGE_COMPLETED_TRANSITION = new StageCompletedTransition();
  private static final QueryCompletedTransition QUERY_COMPLETED_TRANSITION = new QueryCompletedTransition();

  protected static final StateMachineFactory
      <Query,QueryState,QueryEventType,QueryEvent> stateMachineFactory =
      new StateMachineFactory<Query, QueryState, QueryEventType, QueryEvent>
          (QueryState.QUERY_NEW)

          // Transitions from NEW state
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_RUNNING,
              QueryEventType.START,
              new StartTransition())
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_NEW,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_KILLED,
              QueryEventType.KILL,
              new KillNewQueryTransition())
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
              QueryEventType.STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING,
              EnumSet.of(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_FAILED, QueryState.QUERY_KILLED,
                  QueryState.QUERY_ERROR),
              QueryEventType.QUERY_COMPLETED,
              QUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_KILL_WAIT,
              QueryEventType.KILL,
              new KillAllStagesTransition())
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from QUERY_SUCCEEDED state
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // ignore-able transitions
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_SUCCEEDED,
              QueryEventType.KILL)
          .addTransition(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_KILL_WAIT,
              QueryEventType.STAGE_COMPLETED,
              STAGE_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT,
              EnumSet.of(QueryState.QUERY_SUCCEEDED, QueryState.QUERY_FAILED, QueryState.QUERY_KILLED,
                  QueryState.QUERY_ERROR),
              QueryEventType.QUERY_COMPLETED,
              QUERY_COMPLETED_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_KILL_WAIT,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_KILL_WAIT, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_KILL_WAIT, EnumSet.of(QueryState.QUERY_KILLED),
              QueryEventType.KILL,
              QUERY_COMPLETED_TRANSITION)

          // Transitions from FAILED state
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_FAILED,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_FAILED,
              QueryEventType.KILL)

          // Transitions from ERROR state
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able transitions
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              EnumSet.of(QueryEventType.KILL, QueryEventType.STAGE_COMPLETED))

          .installTopology();

  public Query(final QueryMasterTask.QueryMasterTaskContext context, final QueryId id,
               final long appSubmitTime,
               final String queryStr,
               final EventHandler eventHandler,
               final MasterPlan plan) {
    this.context = context;
    this.systemConf = context.getConf();
    this.id = id;
    this.clock = context.getClock();
    this.appSubmitTime = appSubmitTime;
    this.queryStr = queryStr;
    this.stages = Maps.newConcurrentMap();
    this.eventHandler = eventHandler;
    this.plan = plan;
    this.cursor = new ExecutionBlockCursor(plan, true);

    StringBuilder sb = new StringBuilder("\n=======================================================");
    sb.append("\nThe order of execution: \n");
    int order = 1;
    while (cursor.hasNext()) {
      ExecutionBlock currentEB = cursor.nextBlock();
      sb.append("\n").append(order).append(": ").append(currentEB.getId());
      order++;
    }
    sb.append("\n=======================================================");
    LOG.info(sb);
    cursor.reset();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
    queryState = stateMachine.getCurrentState();
  }

  public float getProgress() {
    QueryState state = getState();
    if (state == QueryState.QUERY_SUCCEEDED) {
      return 1.0f;
    } else {
      int idx = 0;
      List<Stage> tempStages = new ArrayList<Stage>();
      synchronized(stages) {
        tempStages.addAll(stages.values());
      }

      float [] subProgresses = new float[tempStages.size()];
      for (Stage stage: tempStages) {
        if (stage.getState() != StageState.NEW) {
          subProgresses[idx] = stage.getProgress();
        } else {
          subProgresses[idx] = 0.0f;
        }
        idx++;
      }

      float totalProgress = 0.0f;
      float proportion = 1.0f / (float)(getExecutionBlockCursor().size() - 1); // minus one is due to

      for (int i = 0; i < subProgresses.length; i++) {
        totalProgress += subProgresses[i] * proportion;
      }

      return totalProgress;
    }
  }

  public long getAppSubmitTime() {
    return this.appSubmitTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime() {
    startTime = clock.getTime();
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime() {
    finishTime = clock.getTime();
  }

  public QueryHistory getQueryHistory() {
    QueryHistory queryHistory = makeQueryHistory();
    queryHistory.setStageHistories(makeStageHistories());
    return queryHistory;
  }

  private List<StageHistory> makeStageHistories() {
    List<StageHistory> stageHistories = new ArrayList<StageHistory>();
    for(Stage eachStage : getStages()) {
      stageHistories.add(eachStage.getStageHistory());
    }

    return stageHistories;
  }

  private QueryHistory makeQueryHistory() {
    QueryHistory queryHistory = new QueryHistory();

    queryHistory.setQueryId(getId().toString());
    queryHistory.setQueryMaster(context.getQueryMasterContext().getWorkerContext().getWorkerName());
    queryHistory.setHttpPort(context.getQueryMasterContext().getWorkerContext().getConnectionInfo().getHttpInfoPort());
    queryHistory.setLogicalPlan(plan.toString());
    queryHistory.setLogicalPlan(plan.getLogicalPlan().toString());
    queryHistory.setDistributedPlan(plan.toString());

    List<String[]> sessionVariables = new ArrayList<String[]>();
    for(Map.Entry<String,String> entry: plan.getContext().getAllKeyValus().entrySet()) {
      if (SessionVars.exists(entry.getKey()) && SessionVars.isPublic(SessionVars.get(entry.getKey()))) {
        sessionVariables.add(new String[]{entry.getKey(), entry.getValue()});
      }
    }
    queryHistory.setSessionVariables(sessionVariables);

    return queryHistory;
  }

  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  protected void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  public TableDesc getResultDesc() {
    return resultDesc;
  }

  public void setResultDesc(TableDesc desc) {
    resultDesc = desc;
  }

  public MasterPlan getPlan() {
    return plan;
  }

  public StateMachine<QueryState, QueryEventType, QueryEvent> getStateMachine() {
    return stateMachine;
  }
  
  public void addStage(Stage stage) {
    stages.put(stage.getId(), stage);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public Stage getStage(ExecutionBlockId id) {
    return this.stages.get(id);
  }

  public Collection<Stage> getStages() {
    return this.stages.values();
  }

  public QueryState getSynchronizedState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /* non-blocking call for client API */
  public QueryState getState() {
    return queryState;
  }

  public ExecutionBlockCursor getExecutionBlockCursor() {
    return cursor;
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {

      query.setStartTime();
      Stage stage = new Stage(query.context, query.getPlan(),
          query.getExecutionBlockCursor().nextBlock());
      stage.setPriority(query.priority--);
      query.addStage(stage);
      stage.getEventHandler().handle(new StageEvent(stage.getId(), StageEventType.SQ_INIT));
      LOG.debug("Schedule unit plan: \n" + stage.getBlock().getPlan());
    }
  }

  public static class QueryCompletedTransition implements MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent queryEvent) {
      if (!(queryEvent instanceof QueryCompletedEvent)) {
        throw new IllegalArgumentException("queryEvent should be a QueryCompletedEvent type.");
      }
      QueryCompletedEvent stageEvent = (QueryCompletedEvent) queryEvent;
      QueryState finalState;

      if (stageEvent.getState() == StageState.SUCCEEDED) {
        finalState = finalizeQuery(query, stageEvent);
      } else if (stageEvent.getState() == StageState.FAILED) {
        finalState = QueryState.QUERY_FAILED;
      } else if (stageEvent.getState() == StageState.KILLED) {
        finalState = QueryState.QUERY_KILLED;
      } else {
        finalState = QueryState.QUERY_ERROR;
      }
      if (finalState != QueryState.QUERY_SUCCEEDED) {
        Stage lastStage = query.getStage(stageEvent.getExecutionBlockId());
        if (lastStage != null && lastStage.getTableMeta() != null) {
          StoreType storeType = lastStage.getTableMeta().getStoreType();
          if (storeType != null) {
            LogicalRootNode rootNode = lastStage.getMasterPlan().getLogicalPlan().getRootBlock().getRoot();
            try {
              StorageManager.getStorageManager(query.systemConf, storeType).rollbackOutputCommit(rootNode.getChild());
            } catch (IOException e) {
              LOG.warn(query.getId() + ", failed processing cleanup storage when query failed:" + e.getMessage(), e);
            }
          }
        }
      }
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
      query.setFinishTime();

      return finalState;
    }

    private QueryState finalizeQuery(Query query, QueryCompletedEvent event) {
      Stage lastStage = query.getStage(event.getExecutionBlockId());
      StoreType storeType = lastStage.getTableMeta().getStoreType();
      try {
        LogicalRootNode rootNode = lastStage.getMasterPlan().getLogicalPlan().getRootBlock().getRoot();
        CatalogService catalog = lastStage.getContext().getQueryMasterContext().getWorkerContext().getCatalog();
        TableDesc tableDesc =  PlannerUtil.getTableDesc(catalog, rootNode.getChild());

        Path finalOutputDir = StorageManager.getStorageManager(query.systemConf, storeType)
            .commitOutputData(query.context.getQueryContext(),
                lastStage.getId(), lastStage.getMasterPlan().getLogicalPlan(), lastStage.getSchema(), tableDesc);

        QueryHookExecutor hookExecutor = new QueryHookExecutor(query.context.getQueryMasterContext());
        hookExecutor.execute(query.context.getQueryContext(), query, event.getExecutionBlockId(), finalOutputDir);
      } catch (Exception e) {
        query.eventHandler.handle(new QueryDiagnosticsUpdateEvent(query.id, ExceptionUtils.getStackTrace(e)));
        return QueryState.QUERY_ERROR;
      }

      return QueryState.QUERY_SUCCEEDED;
    }

    private static interface QueryHook {
      boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir);
      void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                   ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception;
    }

    private static class QueryHookExecutor {
      private List<QueryHook> hookList = TUtil.newList();
      private QueryMaster.QueryMasterContext context;

      public QueryHookExecutor(QueryMaster.QueryMasterContext context) {
        this.context = context;
        hookList.add(new MaterializedResultHook());
        hookList.add(new CreateTableHook());
        hookList.add(new InsertTableHook());
      }

      public void execute(QueryContext queryContext, Query query,
                          ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        for (QueryHook hook : hookList) {
          if (hook.isEligible(queryContext, query, finalExecBlockId, finalOutputDir)) {
            hook.execute(context, queryContext, query, finalExecBlockId, finalOutputDir);
          }
        }
      }
    }

    private static class MaterializedResultHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        Stage lastStage = query.getStage(finalExecBlockId);
        NodeType type = lastStage.getBlock().getPlan().getType();
        return type != NodeType.CREATE_TABLE && type != NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        Stage lastStage = query.getStage(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();

        String nullChar = queryContext.get(SessionVars.NULL_CHAR);
        meta.putOption(StorageConstants.TEXT_NULL, nullChar);

        TableStats stats = lastStage.getResultStats();

        TableDesc resultTableDesc =
            new TableDesc(
                query.getId().toString(),
                lastStage.getSchema(),
                meta,
                finalOutputDir.toUri());
        resultTableDesc.setExternal(true);

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        resultTableDesc.setStats(stats);
        query.setResultDesc(resultTableDesc);
      }
    }

    private static class CreateTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        Stage lastStage = query.getStage(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.CREATE_TABLE;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception {
        CatalogService catalog = context.getWorkerContext().getCatalog();
        Stage lastStage = query.getStage(finalExecBlockId);
        TableStats stats = lastStage.getResultStats();

        CreateTableNode createTableNode = (CreateTableNode) lastStage.getBlock().getPlan();
        TableMeta meta = new TableMeta(createTableNode.getStorageType(), createTableNode.getOptions());

        TableDesc tableDescTobeCreated =
            new TableDesc(
                createTableNode.getTableName(),
                createTableNode.getTableSchema(),
                meta,
                finalOutputDir.toUri());
        tableDescTobeCreated.setExternal(createTableNode.isExternal());

        if (createTableNode.hasPartition()) {
          tableDescTobeCreated.setPartitionMethod(createTableNode.getPartitionMethod());
        }

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        tableDescTobeCreated.setStats(stats);
        query.setResultDesc(tableDescTobeCreated);

        catalog.createTable(tableDescTobeCreated);
      }
    }

    private static class InsertTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        Stage lastStage = query.getStage(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext,
                          Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir)
          throws Exception {

        CatalogService catalog = context.getWorkerContext().getCatalog();
        Stage lastStage = query.getStage(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();
        TableStats stats = lastStage.getResultStats();

        InsertNode insertNode = (InsertNode) lastStage.getBlock().getPlan();

        TableDesc finalTable;
        if (insertNode.hasTargetTable()) {
          String tableName = insertNode.getTableName();
          finalTable = catalog.getTableDesc(tableName);
        } else {
          String tableName = query.getId().toString();
          finalTable = new TableDesc(tableName, lastStage.getSchema(), meta, finalOutputDir.toUri());
        }

        long volume = getTableVolume(query.systemConf, finalOutputDir);
        stats.setNumBytes(volume);
        finalTable.setStats(stats);

        if (insertNode.hasTargetTable()) {
          UpdateTableStatsProto.Builder builder = UpdateTableStatsProto.newBuilder();
          builder.setTableName(finalTable.getName());
          builder.setStats(stats.getProto());

          catalog.updateTableStats(builder.build());
        }

        query.setResultDesc(finalTable);
      }
    }
  }

  public static long getTableVolume(TajoConf systemConf, Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(systemConf);
    ContentSummary directorySummary = fs.getContentSummary(tablePath);
    return directorySummary.getLength();
  }

  public static class StageCompletedTransition implements SingleArcTransition<Query, QueryEvent> {

    private boolean hasNext(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.peek();
      return !query.getPlan().isTerminal(nextBlock);
    }

    private void executeNextBlock(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.nextBlock();
      Stage nextStage = new Stage(query.context, query.getPlan(), nextBlock);
      nextStage.setPriority(query.priority--);
      query.addStage(nextStage);
      nextStage.getEventHandler().handle(new StageEvent(nextStage.getId(), StageEventType.SQ_INIT));

      LOG.info("Scheduling Stage:" + nextStage.getId());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Scheduling Stage's Priority: " + nextStage.getPriority());
        LOG.debug("Scheduling Stage's Plan: \n" + nextStage.getBlock().getPlan());
      }
    }

    @Override
    public void transition(Query query, QueryEvent event) {
      if (!(event instanceof StageCompletedEvent)) {
        throw new IllegalArgumentException("event should be a StageCompletedEvent type.");
      }
      try {
        query.completedStagesCount++;
        StageCompletedEvent castEvent = (StageCompletedEvent) event;

        if (castEvent.getState() == StageState.SUCCEEDED) {
          query.successedStagesCount++;
        } else if (castEvent.getState() == StageState.KILLED) {
          query.killedStagesCount++;
        } else if (castEvent.getState() == StageState.FAILED) {
          query.failedStagesCount++;
        } else if (castEvent.getState() == StageState.ERROR) {
          query.erroredStagesCount++;
        } else {
          LOG.error(String.format("Invalid Stage (%s) State %s at %s",
              castEvent.getExecutionBlockId().toString(), castEvent.getState().name(), query.getSynchronizedState().name()));
          query.eventHandler.handle(new QueryEvent(event.getQueryId(), QueryEventType.INTERNAL_ERROR));
        }

        // if a stage is succeeded and a query is running
        if (castEvent.getState() == StageState.SUCCEEDED &&  // latest stage succeeded
            query.getSynchronizedState() == QueryState.QUERY_RUNNING &&     // current state is not in KILL_WAIT, FAILED, or ERROR.
            hasNext(query)) {                                   // there remains at least one stage.
          executeNextBlock(query);
        } else { // if a query is completed due to finished, kill, failure, or error
          query.eventHandler.handle(new QueryCompletedEvent(castEvent.getExecutionBlockId(), castEvent.getState()));
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage(), t);
        query.eventHandler.handle(new QueryEvent(event.getQueryId(), QueryEventType.INTERNAL_ERROR));
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      if (!(event instanceof QueryDiagnosticsUpdateEvent)) {
        throw new IllegalArgumentException("event should be a QueryDiagnosticsUpdateEvent type.");
      }
      query.addDiagnostic(((QueryDiagnosticsUpdateEvent) event).getDiagnosticUpdate());
    }
  }

  private static class KillNewQueryTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      query.setFinishTime();
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
    }
  }

  private static class KillAllStagesTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      synchronized (query.stages) {
        for (Stage stage : query.stages.values()) {
          stage.stopFinalization();
          query.eventHandler.handle(new StageEvent(stage.getId(), StageEventType.SQ_KILL));
        }
      }
    }
  }

  private static class InternalErrorTransition implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent event) {
      query.setFinishTime();
      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
    }
  }

  @Override
  public void handle(QueryEvent event) {
    LOG.info("Processing " + event.getQueryId() + " of type " + event.getType());
    try {
      writeLock.lock();
      QueryState oldState = getSynchronizedState();
      try {
        getStateMachine().doTransition(event.getType(), event);
        queryState = getSynchronizedState();
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state"
            + ", type:" + event
            + ", oldState:" + oldState.name()
            + ", nextState:" + getSynchronizedState().name()
            , e);
        eventHandler.handle(new QueryEvent(this.id, QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getSynchronizedState()) {
        LOG.info(id + " Query Transitioned from " + oldState + " to " + getSynchronizedState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
