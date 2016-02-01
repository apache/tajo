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

import com.google.common.collect.Lists;
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
import org.apache.tajo.QueryVars;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.UpdateTableStatsProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.ExecutionQueue;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.error.Errors.SerializedException;
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.master.event.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
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
  private final String queryStr;
  private final Map<ExecutionBlockId, Stage> stages;
  private final EventHandler eventHandler;
  private final MasterPlan plan;
  QueryMasterTask.QueryMasterTaskContext context;
  private ExecutionBlockCursor cursor;
  private ExecutionQueue execution;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long finishTime;
  private TableDesc resultDesc;
  private volatile int completedStagesCount = 0;
  private volatile int succeededStagesCount = 0;
  private volatile int killedStagesCount = 0;
  private volatile int failedStagesCount = 0;
  private volatile int erroredStagesCount = 0;
  private volatile SerializedException failureReason;
  private final List<String> diagnostics = new ArrayList<>();

  // Internal Variables
  private final Lock readLock;
  private final Lock writeLock;
  private int priority = 100;

  // State Machine
  private final StateMachine<QueryState, QueryEventType, QueryEvent> stateMachine;
  private QueryState queryState;

  // Transition Handler
  private static final InternalErrorTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
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

              // Transitions from KILLED state
              // ignore-able transitions
          .addTransition(QueryState.QUERY_KILLED, QueryState.QUERY_KILLED,
              EnumSet.of(QueryEventType.START, QueryEventType.QUERY_COMPLETED,
                  QueryEventType.KILL, QueryEventType.INTERNAL_ERROR))
          .addTransition(QueryState.QUERY_KILLED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

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
    for (ExecutionBlock currentEB : cursor) {
      sb.append("\n").append(order).append(": ").append(currentEB.getId());
      order++;
    }
    sb.append("\n=======================================================");
    LOG.info(sb);

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
      List<Stage> tempStages = new ArrayList<>();
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

      for (float subProgress : subProgresses) {
        totalProgress += subProgress * proportion;
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
    List<StageHistory> stageHistories = new ArrayList<>();
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
    queryHistory.setLogicalPlan(plan.getLogicalPlan().toString());
    queryHistory.setDistributedPlan(plan.toString());

    List<String[]> sessionVariables = new ArrayList<>();
    for(Map.Entry<String,String> entry: plan.getContext().getAllKeyValus().entrySet()) {
      if (SessionVars.exists(entry.getKey()) && SessionVars.isPublic(SessionVars.get(entry.getKey()))) {
        sessionVariables.add(new String[]{entry.getKey(), entry.getValue()});
      }
    }
    queryHistory.setSessionVariables(sessionVariables);

    return queryHistory;
  }

  public List<PartitionDescProto> getPartitions() {
    Set<PartitionDescProto> partitions = new HashSet<>();
    for(Stage eachStage : getStages()) {
      partitions.addAll(eachStage.getPartitions());
    }
    return Lists.newArrayList(partitions);
  }

  public void clearPartitions() {
    for(Stage eachStage : getStages()) {
      eachStage.clearPartitions();
    }
  }

  public SerializedException getFailureReason() {
    return failureReason;
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

  public ExecutionQueue newExecutionQueue() {
    return execution = cursor.newCursor();
  }

  public ExecutionQueue getExecutionQueue() {
    return execution;
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {

      query.setStartTime();
      ExecutionQueue executionQueue = query.newExecutionQueue();
      for (ExecutionBlock executionBlock : executionQueue.first()) {
        Stage stage = new Stage(query.context, query.getPlan(), executionBlock);
        stage.setPriority(query.priority--);
        query.addStage(stage);

        stage.getEventHandler().handle(new StageEvent(stage.getId(), StageEventType.SQ_INIT));
        LOG.debug("Schedule unit plan: \n" + stage.getBlock().getPlan());
      }
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

      // When a query is failed
      if (finalState != QueryState.QUERY_SUCCEEDED) {
        Stage lastStage = query.getStage(stageEvent.getExecutionBlockId());
        handleQueryFailure(query, lastStage);
      }

      query.eventHandler.handle(new QueryMasterQueryCompletedEvent(query.getId()));
      query.setFinishTime();

      return finalState;
    }

    // handle query failures
    private void handleQueryFailure(Query query, Stage lastStage) {
      QueryContext context = query.context.getQueryContext();

      if (lastStage != null && context.hasOutputTableUri()) {
        Tablespace space = TablespaceManager.get(context.getOutputTableUri());
        try {
          LogicalRootNode rootNode = lastStage.getMasterPlan().getLogicalPlan().getRootBlock().getRoot();
          space.rollbackTable(rootNode.getChild());
        } catch (Throwable e) {
          LOG.warn(query.getId() + ", failed processing cleanup storage when query failed:" + e.getMessage(), e);
        }
      }
    }

    private QueryState finalizeQuery(Query query, QueryCompletedEvent event) {
      Stage lastStage = query.getStage(event.getExecutionBlockId());

      try {

        LogicalRootNode rootNode = lastStage.getMasterPlan().getLogicalPlan().getRootBlock().getRoot();
        CatalogService catalog = lastStage.getContext().getQueryMasterContext().getWorkerContext().getCatalog();
        TableDesc tableDesc =  PlannerUtil.getTableDesc(catalog, rootNode.getChild());

        QueryContext queryContext = query.context.getQueryContext();

        // If there is not tabledesc, it is a select query without insert or ctas.
        // In this case, we should use default tablespace.
        Tablespace space = TablespaceManager.get(queryContext.get(QueryVars.OUTPUT_TABLE_URI, ""));

        Path finalOutputDir = space.commitTable(
            query.context.getQueryContext(),
            lastStage.getId(),
            lastStage.getMasterPlan().getLogicalPlan(),
            lastStage.getSchema(),
            tableDesc);

        QueryHookExecutor hookExecutor = new QueryHookExecutor(query.context.getQueryMasterContext());
        hookExecutor.execute(query.context.getQueryContext(), query, event.getExecutionBlockId(), finalOutputDir);

        // Add dynamic partitions to catalog for partition table.
        if (queryContext.hasOutputTableUri() && queryContext.hasPartition()) {
          List<PartitionDescProto> partitions = query.getPartitions();
          if (partitions != null) {
            // Set contents length and file count to PartitionDescProto by listing final output directories.
            List<PartitionDescProto> finalPartitions = getPartitionsWithContentsSummary(query.systemConf,
              finalOutputDir, partitions);

            String databaseName, simpleTableName;
            if (CatalogUtil.isFQTableName(tableDesc.getName())) {
              String[] split = CatalogUtil.splitFQTableName(tableDesc.getName());
              databaseName = split[0];
              simpleTableName = split[1];
            } else {
              databaseName = queryContext.getCurrentDatabase();
              simpleTableName = tableDesc.getName();
            }

            // Store partitions to CatalogStore using alter table statement.
            catalog.addPartitions(databaseName, simpleTableName, finalPartitions, true);
            LOG.info("Added partitions to catalog (total=" + partitions.size() + ")");
          } else {
            LOG.info("Can't find partitions for adding.");
          }
          query.clearPartitions();
        }
      } catch (Throwable e) {
        LOG.fatal(e.getMessage(), e);
        query.failureReason = ErrorUtil.convertException(e);
        query.eventHandler.handle(new QueryDiagnosticsUpdateEvent(query.id, ExceptionUtils.getStackTrace(e)));
        return QueryState.QUERY_ERROR;
      }

      return QueryState.QUERY_SUCCEEDED;
    }

    private List<PartitionDescProto> getPartitionsWithContentsSummary(TajoConf conf, Path outputDir,
        List<PartitionDescProto> partitions) throws IOException {
      List<PartitionDescProto> finalPartitions = new ArrayList<>();

      FileSystem fileSystem = outputDir.getFileSystem(conf);
      for (PartitionDescProto partition : partitions) {
        PartitionDescProto.Builder builder = partition.toBuilder();
        Path partitionPath = new Path(outputDir, partition.getPath());
        ContentSummary contentSummary = fileSystem.getContentSummary(partitionPath);
        builder.setNumBytes(contentSummary.getLength());
        finalPartitions.add(builder.build());
      }
      return finalPartitions;
    }

    private static interface QueryHook {
      boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir);
      void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                   ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception;
    }

    private static class QueryHookExecutor {
      private List<QueryHook> hookList = new ArrayList<>();
      private QueryMaster.QueryMasterContext context;

      public QueryHookExecutor(QueryMaster.QueryMasterContext context) {
        this.context = context;
        hookList.add(new MaterializedResultHook());
        hookList.add(new CreateTableHook());
        hookList.add(new InsertTableHook());
        hookList.add(new CreateIndexHook());
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

    private static class CreateIndexHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir) {
        Stage lastStage = query.getStage(finalExecBlockId);
        return  lastStage.getBlock().getPlan().getType() == NodeType.CREATE_INDEX;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception {
        CatalogService catalog = context.getWorkerContext().getCatalog();
        Stage lastStage = query.getStage(finalExecBlockId);

        CreateIndexNode createIndexNode = (CreateIndexNode) lastStage.getBlock().getPlan();
        String databaseName, simpleIndexName, qualifiedIndexName;
        if (CatalogUtil.isFQTableName(createIndexNode.getIndexName())) {
          String [] splits = CatalogUtil.splitFQTableName(createIndexNode.getIndexName());
          databaseName = splits[0];
          simpleIndexName = splits[1];
          qualifiedIndexName = createIndexNode.getIndexName();
        } else {
          databaseName = queryContext.getCurrentDatabase();
          simpleIndexName = createIndexNode.getIndexName();
          qualifiedIndexName = CatalogUtil.buildFQName(databaseName, simpleIndexName);
        }
        ScanNode scanNode = PlannerUtil.findTopNode(createIndexNode, NodeType.SCAN);
        if (scanNode == null) {
          throw new IOException("Cannot find the table of the relation");
        }
        IndexDesc indexDesc = new IndexDesc(databaseName, CatalogUtil.extractSimpleName(scanNode.getTableName()),
            simpleIndexName, createIndexNode.getIndexPath(),
            createIndexNode.getKeySortSpecs(), createIndexNode.getIndexMethod(),
            createIndexNode.isUnique(), false, scanNode.getLogicalSchema());
        catalog.createIndex(indexDesc);
        LOG.info("Index " + qualifiedIndexName + " is created for the table " + scanNode.getTableName() + ".");
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
        meta.putProperty(StorageConstants.TEXT_NULL, nullChar);

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

    // return true for terminal
    private synchronized boolean executeNextBlock(Query query, ExecutionBlockId blockId) {
      ExecutionQueue cursor = query.getExecutionQueue();
      ExecutionBlock[] nextBlocks = cursor.next(blockId);
      if (nextBlocks == null || nextBlocks.length == 0) {
        return nextBlocks == null;
      }
      boolean terminal = true;
      for (ExecutionBlock nextBlock : nextBlocks) {
        if (query.getPlan().isTerminal(nextBlock)) {
          continue;
        }
        Stage nextStage = new Stage(query.context, query.getPlan(), nextBlock);
        nextStage.setPriority(query.priority--);
        query.addStage(nextStage);
        nextStage.getEventHandler().handle(new StageEvent(nextStage.getId(), StageEventType.SQ_INIT));

        LOG.info("Scheduling Stage:" + nextStage.getId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scheduling Stage's Priority: " + nextStage.getPriority());
          LOG.debug("Scheduling Stage's Plan: \n" + nextStage.getBlock().getPlan());
        }
        terminal = false;
      }
      return terminal;
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
          query.succeededStagesCount++;
        } else if (castEvent.getState() == StageState.KILLED) {
          query.killedStagesCount++;
        } else if (castEvent.getState() == StageState.FAILED) {
          query.failedStagesCount++;
          query.failureReason = query.getStage(castEvent.getExecutionBlockId()).getFailureReason();
        } else if (castEvent.getState() == StageState.ERROR) {
          query.erroredStagesCount++;
          query.failureReason = query.getStage(castEvent.getExecutionBlockId()).getFailureReason();
        } else {
          LOG.error(String.format("Invalid Stage (%s) State %s at %s",
              castEvent.getExecutionBlockId().toString(), castEvent.getState().name(), query.getSynchronizedState().name()));
          query.eventHandler.handle(new QueryEvent(event.getQueryId(), QueryEventType.INTERNAL_ERROR));
        }

        // if a stage is succeeded and a query is running
        if (castEvent.getState() == StageState.SUCCEEDED &&  // latest stage succeeded
            query.getSynchronizedState() == QueryState.QUERY_RUNNING &&     // current state is not in KILL_WAIT, FAILED, or ERROR.
            !executeNextBlock(query, castEvent.getExecutionBlockId())) {
          return;
        }

        //wait for stages is completed
        if (query.completedStagesCount >= query.stages.size()) {
          // if a query is completed due to finished, kill, failure, or error
          query.eventHandler.handle(new QueryCompletedEvent(castEvent.getExecutionBlockId(), castEvent.getState()));
        }
        LOG.info(String.format("Complete Stage[%s], State: %s, %d/%d. ",
            castEvent.getExecutionBlockId().toString(),
            castEvent.getState().toString(),
            query.completedStagesCount,
            query.stages.size()));
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
