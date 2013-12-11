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
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.event.*;
import org.apache.tajo.storage.AbstractStorageManager;

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

  public static class SubQueryCompletedTransition implements
      MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent event) {
      // increase the count for completed subqueries
      query.completedSubQueryCount++;
      SubQueryCompletedEvent castEvent = (SubQueryCompletedEvent) event;
      ExecutionBlockCursor cursor = query.getExecutionBlockCursor();
      MasterPlan masterPlan = query.getPlan();
      // if the subquery is succeeded
      if (castEvent.getFinalState() == SubQueryState.SUCCEEDED) {
        ExecutionBlock nextBlock = cursor.nextBlock();
        if (!query.getPlan().isTerminal(nextBlock)) {
          SubQuery nextSubQuery = new SubQuery(query.context, query.getPlan(), nextBlock, query.sm);
          nextSubQuery.setPriority(query.priority--);
          query.addSubQuery(nextSubQuery);
          nextSubQuery.handle(new SubQueryEvent(nextSubQuery.getId(),
              SubQueryEventType.SQ_INIT));
          LOG.info("Scheduling SubQuery:" + nextSubQuery.getId());
          if(LOG.isDebugEnabled()) {
            LOG.debug("Scheduling SubQuery's Priority: " + nextSubQuery.getPriority());
            LOG.debug("Scheduling SubQuery's Plan: \n" + nextSubQuery.getBlock().getPlan());
          }
          return query.checkQueryForCompleted();

        } else { // Finish a query
          if (query.checkQueryForCompleted() == QueryState.QUERY_SUCCEEDED) {
            DataChannel finalChannel = masterPlan.getChannel(castEvent.getExecutionBlockId(), nextBlock.getId());
            Path finalOutputDir = commitOutputData(query);
            TableDesc finalTableDesc = buildOrUpdateResultTableDesc(query, castEvent.getExecutionBlockId(),
                finalOutputDir);

            QueryContext queryContext = query.context.getQueryContext();
            CatalogService catalog = query.context.getQueryMasterContext().getWorkerContext().getCatalog();

            if (queryContext.hasOutputTable()) { // TRUE only if a query command is 'CREATE TABLE' OR 'INSERT INTO'
              if (queryContext.isOutputOverwrite()) { // TRUE only if a query is 'INSERT OVERWRITE INTO'
                catalog.deleteTable(finalOutputDir.getName());
              }
              catalog.addTable(finalTableDesc);
            }
            query.setResultDesc(finalTableDesc);
            query.finished(QueryState.QUERY_SUCCEEDED);
            query.eventHandler.handle(new QueryFinishEvent(query.getId()));
          }

          return QueryState.QUERY_SUCCEEDED;
        }
      } else if (castEvent.getFinalState() == SubQueryState.ERROR) {
        query.setFinishTime();
        return QueryState.QUERY_ERROR;
      } else {
        // if at least one subquery is failed, the query is also failed.
        query.setFinishTime();
        return QueryState.QUERY_FAILED;
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

    /**
     * It builds a table desc and update the table desc if necessary.
     */
    public TableDesc buildOrUpdateResultTableDesc(Query query, ExecutionBlockId finalExecBlockId,
                                                  Path finalOutputDir) {
      // Determine the output table name
      SubQuery subQuery = query.getSubQuery(finalExecBlockId);
      QueryContext queryContext = query.context.getQueryContext();
      String outputTableName;
      if (queryContext.hasOutputTable()) { // CREATE TABLE or INSERT STATEMENT
        outputTableName = queryContext.getOutputTable();
      } else { // SELECT STATEMENT
        outputTableName = query.getId().toString();
      }

      TableMeta meta = subQuery.getTableMeta();
      TableStats stats = subQuery.getTableStat();
      try {
        FileSystem fs = finalOutputDir.getFileSystem(query.systemConf);
        ContentSummary directorySummary = fs.getContentSummary(finalOutputDir);
        stats.setNumBytes(directorySummary.getLength());
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
      TableDesc outputTableDesc = new TableDesc(outputTableName, subQuery.getSchema(), meta, finalOutputDir);
      outputTableDesc.setStats(stats);
      TableDesc finalTableDesc = outputTableDesc;

      // If a query has a target table, a TableDesc is updated.
      if (queryContext.hasOutputTable()) { // CREATE TABLE or INSERT STATEMENT
        if (queryContext.isOutputOverwrite()) {
          CatalogService catalog = query.context.getQueryMasterContext().getWorkerContext().getCatalog();
          Preconditions.checkNotNull(catalog, "CatalogService is NULL");
          TableDesc updatingTable = catalog.getTableDesc(outputTableDesc.getName());
          updatingTable.setStats(stats);
          finalTableDesc = updatingTable;
        }
      }
      return finalTableDesc;
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
