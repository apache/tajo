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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.InsertNode;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.CreateTableNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.event.*;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.util.TUtil;

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
  private Map<ExecutionBlockId, SubQuery> subqueries;
  private final EventHandler eventHandler;
  private final MasterPlan plan;
  private final AbstractStorageManager sm;
  QueryMasterTask.QueryMasterTaskContext context;
  private ExecutionBlockCursor cursor;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long finishTime;
  private TableDesc resultDesc;
  private int completedSubQueryCount = 0;
  private final List<String> diagnostics = new ArrayList<String>();

  // Internal Variables
  private final Lock readLock;
  private final Lock writeLock;
  private int priority = 100;

  // State Machine
  private final StateMachine<QueryState, QueryEventType, QueryEvent> stateMachine;

  // Transition Handler
  private static final SingleArcTransition INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final DiagnosticsUpdateTransition DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();

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
          .addTransition(QueryState.QUERY_NEW, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(QueryState.QUERY_RUNNING,
              EnumSet.of(QueryState.QUERY_RUNNING, QueryState.QUERY_SUCCEEDED, QueryState.QUERY_FAILED,
                  QueryState.QUERY_ERROR),
              QueryEventType.SUBQUERY_COMPLETED,
              new SubQueryCompletedTransition())
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from FAILED state
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_FAILED,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_FAILED, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from ERROR state
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
              QueryEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

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
    subqueries = Maps.newHashMap();
    this.eventHandler = eventHandler;
    this.plan = plan;
    this.sm = context.getStorageManager();
    cursor = new ExecutionBlockCursor(plan);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  public float getProgress() {
    QueryState state = getStateMachine().getCurrentState();
    if (state == QueryState.QUERY_SUCCEEDED) {
      return 1.0f;
    } else {
      int idx = 0;
      List<SubQuery> tempSubQueries = new ArrayList<SubQuery>();
      synchronized(subqueries) {
        tempSubQueries.addAll(subqueries.values());
      }
      float [] subProgresses = new float[tempSubQueries.size()];
      boolean finished = true;
      for (SubQuery subquery: tempSubQueries) {
        if (subquery.getState() != SubQueryState.NEW) {
          subProgresses[idx] = subquery.getProgress();
          if (finished && subquery.getState() != SubQueryState.SUCCEEDED) {
            finished = false;
          }
        } else {
          subProgresses[idx] = 0.0f;
        }
        idx++;
      }

      if (finished) {
        return 1.0f;
      }

      float totalProgress = 0;
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
  
  public void addSubQuery(SubQuery subquery) {
    subqueries.put(subquery.getId(), subquery);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public SubQuery getSubQuery(ExecutionBlockId id) {
    return this.subqueries.get(id);
  }

  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }

  public QueryState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public ExecutionBlockCursor getExecutionBlockCursor() {
    return cursor;
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {
      query.setStartTime();
      SubQuery subQuery = new SubQuery(query.context, query.getPlan(),
          query.getExecutionBlockCursor().nextBlock(), query.sm);
      subQuery.setPriority(query.priority--);
      query.addSubQuery(subQuery);

      subQuery.handle(new SubQueryEvent(subQuery.getId(), SubQueryEventType.SQ_INIT));
      LOG.debug("Schedule unit plan: \n" + subQuery.getBlock().getPlan());
    }
  }

  public static class SubQueryCompletedTransition implements MultipleArcTransition<Query, QueryEvent, QueryState> {

    private boolean hasNext(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.peek();
      return !query.getPlan().isTerminal(nextBlock);
    }

    private QueryState executeNextBlock(Query query) {
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      ExecutionBlock nextBlock = cursor.nextBlock();
      SubQuery nextSubQuery = new SubQuery(query.context, query.getPlan(), nextBlock, query.sm);
      nextSubQuery.setPriority(query.priority--);
      query.addSubQuery(nextSubQuery);
      nextSubQuery.handle(new SubQueryEvent(nextSubQuery.getId(), SubQueryEventType.SQ_INIT));

      LOG.info("Scheduling SubQuery:" + nextSubQuery.getId());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Scheduling SubQuery's Priority: " + nextSubQuery.getPriority());
        LOG.debug("Scheduling SubQuery's Plan: \n" + nextSubQuery.getBlock().getPlan());
      }

      return query.checkQueryForCompleted();
    }

    private QueryState finalizeQuery(Query query, SubQueryCompletedEvent event) {
      MasterPlan masterPlan = query.getPlan();

      if (query.checkQueryForCompleted() == QueryState.QUERY_SUCCEEDED) {
        ExecutionBlock terminal = query.getPlan().getTerminalBlock();
        DataChannel finalChannel = masterPlan.getChannel(event.getExecutionBlockId(), terminal.getId());
        Path finalOutputDir = commitOutputData(query);

        QueryHookExecutor hookExecutor = new QueryHookExecutor(query.context.getQueryMasterContext());
        try {
          hookExecutor.execute(query.context.getQueryContext(), query, event.getExecutionBlockId(),
              finalOutputDir);
        } catch (Exception e) {
          query.eventHandler.handle(new QueryDiagnosticsUpdateEvent(query.id, ExceptionUtils.getStackTrace(e)));
          return QueryState.QUERY_FAILED;
        } finally {
          query.setFinishTime();
        }
        query.finished(QueryState.QUERY_SUCCEEDED);
        query.eventHandler.handle(new QueryFinishEvent(query.getId()));
      }

      return QueryState.QUERY_SUCCEEDED;
    }

    @Override
    public QueryState transition(Query query, QueryEvent event) {
      // increase the count for completed subqueries
      query.completedSubQueryCount++;

      SubQueryCompletedEvent castEvent = (SubQueryCompletedEvent) event;

      // if the subquery is succeeded
      if (castEvent.getFinalState() == SubQueryState.SUCCEEDED) {
        if (hasNext(query)) { // if there is next block
          return executeNextBlock(query);
        } else {
          return finalizeQuery(query, castEvent);
        }
      } else {
        query.setFinishTime();

        if (castEvent.getFinalState() == SubQueryState.ERROR) {
          return QueryState.QUERY_ERROR;
        } else {
          return QueryState.QUERY_FAILED;
        }
      }
    }

    /**
     * It moves a result data stored in a staging output dir into a final output dir.
     */
    public Path commitOutputData(Query query) {
      QueryContext queryContext = query.context.getQueryContext();
      Path stagingResultDir = new Path(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME);
      Path finalOutputDir;
      if (queryContext.hasOutputPath()) {
        finalOutputDir = queryContext.getOutputPath();
        try {
          FileSystem fs = stagingResultDir.getFileSystem(query.systemConf);
          fs.rename(stagingResultDir, finalOutputDir);
          LOG.info("Moved from the staging dir to the output directory '" + finalOutputDir);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        finalOutputDir = new Path(queryContext.getStagingDir(), TajoConstants.RESULT_DIR_NAME);
      }

      return finalOutputDir;
    }

    private static interface QueryHook {
      boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId, Path finalOutputDir);
      void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                   ExecutionBlockId finalExecBlockId, Path finalOutputDir) throws Exception;
    }

    private class QueryHookExecutor {
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

    private class MaterializedResultHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        NodeType type = lastStage.getBlock().getPlan().getType();
        return type != NodeType.CREATE_TABLE && type != NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                          ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();
        TableStats stats = lastStage.getTableStat();

        TableDesc resultTableDesc =
            new TableDesc(
                query.getId().toString(),
                lastStage.getSchema(),
                meta,
                finalOutputDir);

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        resultTableDesc.setStats(stats);
        query.setResultDesc(resultTableDesc);
      }
    }

    private class CreateTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.CREATE_TABLE;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                          ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir) throws Exception {
        CatalogService catalog = context.getWorkerContext().getCatalog();
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();
        TableStats stats = lastStage.getTableStat();

        CreateTableNode createTableNode = (CreateTableNode) lastStage.getBlock().getPlan();

        TableDesc tableDescTobeCreated =
            new TableDesc(
                createTableNode.getTableName(),
                createTableNode.getTableSchema(),
                meta,
                finalOutputDir);

        if (createTableNode.hasPartition()) {
          tableDescTobeCreated.setPartitionMethod(createTableNode.getPartitionMethod());
        }

        stats.setNumBytes(getTableVolume(query.systemConf, finalOutputDir));
        tableDescTobeCreated.setStats(stats);
        query.setResultDesc(tableDescTobeCreated);

        catalog.addTable(tableDescTobeCreated);
      }
    }

    private class InsertTableHook implements QueryHook {

      @Override
      public boolean isEligible(QueryContext queryContext, Query query, ExecutionBlockId finalExecBlockId,
                                Path finalOutputDir) {
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        return lastStage.getBlock().getPlan().getType() == NodeType.INSERT;
      }

      @Override
      public void execute(QueryMaster.QueryMasterContext context, QueryContext queryContext, Query query,
                          ExecutionBlockId finalExecBlockId,
                          Path finalOutputDir)
          throws Exception {

        CatalogService catalog = context.getWorkerContext().getCatalog();
        SubQuery lastStage = query.getSubQuery(finalExecBlockId);
        TableMeta meta = lastStage.getTableMeta();
        TableStats stats = lastStage.getTableStat();

        InsertNode insertNode = (InsertNode) lastStage.getBlock().getPlan();

        TableDesc finalTable;
        if (insertNode.hasTargetTable()) {
          String tableName = insertNode.getTableName();
          finalTable = catalog.getTableDesc(tableName);
        } else {
          String tableName = query.getId().toString();
          finalTable = new TableDesc(tableName, lastStage.getSchema(), meta, finalOutputDir);
        }

        long volume = getTableVolume(query.systemConf, finalOutputDir);
        stats.setNumBytes(volume);
        finalTable.setStats(stats);

        if (insertNode.hasTargetTable()) {
          catalog.deleteTable(insertNode.getTableName());
          catalog.addTable(finalTable);
        }

        query.setResultDesc(finalTable);
      }
    }

    private long getTableVolume(TajoConf systemConf, Path tablePath) throws IOException {
      FileSystem fs = tablePath.getFileSystem(systemConf);
      ContentSummary directorySummary = fs.getContentSummary(tablePath);
      return directorySummary.getLength();
    }
  }

  private static class DiagnosticsUpdateTransition implements SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      query.addDiagnostic(((QueryDiagnosticsUpdateEvent) event).getDiagnosticUpdate());
    }
  }

  private static class InternalErrorTransition implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent event) {
      query.setFinishTime();
      query.finished(QueryState.QUERY_ERROR);
    }
  }

  public QueryState finished(QueryState finalState) {
    setFinishTime();
    return finalState;
  }

  /**
   * Check if all subqueries of the query are completed
   * @return QueryState.QUERY_SUCCEEDED if all subqueries are completed.
   */
  QueryState checkQueryForCompleted() {
    if (completedSubQueryCount == subqueries.size()) {
      return QueryState.QUERY_SUCCEEDED;
    }
    return getState();
  }


  @Override
  public void handle(QueryEvent event) {
    LOG.info("Processing " + event.getQueryId() + " of type " + event.getType());
    try {
      writeLock.lock();
      QueryState oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new QueryEvent(this.id, QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(id + " Query Transitioned from " + oldState + " to "
            + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
