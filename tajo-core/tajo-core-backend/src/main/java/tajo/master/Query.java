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

package tajo.master;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import tajo.QueryConf;
import tajo.QueryId;
import tajo.SubQueryId;
import tajo.TajoProtos.QueryState;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.IndexWriteNode;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.event.*;
import tajo.storage.StorageManager;
import tajo.util.IndexUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Query implements EventHandler<QueryEvent> {
  private static final Log LOG = LogFactory.getLog(Query.class);


  // Facilities for Query
  private final QueryConf conf;
  private final Clock clock;
  private String queryStr;
  private Map<SubQueryId, SubQuery> subqueries;
  private final EventHandler eventHandler;
  private final MasterPlan plan;
  private final StorageManager sm;
  private QueryContext context;
  private ExecutionBlockCursor cursor;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long initializationTime;
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

  private static final StateMachineFactory
      <Query,QueryState,QueryEventType,QueryEvent> stateMachineFactory =
      new StateMachineFactory<Query, QueryState, QueryEventType, QueryEvent>
          (QueryState.QUERY_NEW)

      .addTransition(QueryState.QUERY_NEW,
          EnumSet.of(QueryState.QUERY_INIT, QueryState.QUERY_FAILED),
          QueryEventType.INIT, new InitTransition())

      .addTransition(QueryState.QUERY_INIT, QueryState.QUERY_RUNNING,
          QueryEventType.START, new StartTransition())

      .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_RUNNING,
          QueryEventType.INIT_COMPLETED, new InitCompleteTransition())
      .addTransition(QueryState.QUERY_RUNNING,
          EnumSet.of(QueryState.QUERY_RUNNING, QueryState.QUERY_SUCCEEDED,
              QueryState.QUERY_FAILED),
          QueryEventType.SUBQUERY_COMPLETED,
          new SubQueryCompletedTransition())
      .addTransition(QueryState.QUERY_RUNNING, QueryState.QUERY_ERROR,
          QueryEventType.INTERNAL_ERROR, new InternalErrorTransition())
       .addTransition(QueryState.QUERY_ERROR, QueryState.QUERY_ERROR,
          QueryEventType.INTERNAL_ERROR)

      .installTopology();

  public Query(final QueryContext context, final QueryId id, Clock clock,
               final long appSubmitTime,
               final String queryStr,
               final EventHandler eventHandler,
               final MasterPlan plan, final StorageManager sm) {
    this.context = context;
    this.conf = context.getConf();
    this.id = id;
    this.clock = clock;
    this.appSubmitTime = appSubmitTime;
    this.queryStr = queryStr;
    subqueries = Maps.newHashMap();
    this.eventHandler = eventHandler;
    this.plan = plan;
    this.sm = sm;
    cursor = new ExecutionBlockCursor(plan);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  public boolean isCreateTableStmt() {
    return context.isCreateTableQuery();
  }

  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  public float getProgress() {
    QueryState state = getStateMachine().getCurrentState();
    if (state == QueryState.QUERY_SUCCEEDED) {
      return 1.0f;
    } else {
      int idx = 0;
      float [] subProgresses = new float[subqueries.size()];
      boolean finished = true;
      for (SubQuery subquery: subqueries.values()) {
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
      float proportion = 1.0f / (float)subqueries.size();

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

  public long getInitializationTime() {
    return initializationTime;
  }

  public void setInitializationTime() {
    initializationTime = clock.getTime();
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
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
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

  static class InitTransition
      implements MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent queryEvent) {
      query.setStartTime();
      return QueryState.QUERY_INIT;
    }
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {
      SubQuery subQuery = new SubQuery(query.context, query.getExecutionBlockCursor().nextBlock(),
          query.sm);
      subQuery.setPriority(query.priority--);
      query.addSubQuery(subQuery);
      LOG.info("Schedule unit plan: \n" + subQuery.getBlock().getPlan());
      subQuery.handle(new SubQueryEvent(subQuery.getId(),
          SubQueryEventType.SQ_INIT));
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

      // if the subquery is succeeded
      if (castEvent.getFinalState() == SubQueryState.SUCCEEDED) {
        if (cursor.hasNext()) {
          SubQuery nextSubQuery = new SubQuery(query.context, cursor.nextBlock(), query.sm);
          nextSubQuery.setPriority(query.priority--);
          query.addSubQuery(nextSubQuery);
          nextSubQuery.handle(new SubQueryEvent(nextSubQuery.getId(),
              SubQueryEventType.SQ_INIT));
          LOG.info("Scheduling SubQuery's Priority: " + nextSubQuery.getPriority());
          LOG.info("Scheduling SubQuery's Plan: \n" + nextSubQuery.getBlock().getPlan());
          return query.checkQueryForCompleted();

        } else { // Finish a query
          if (query.checkQueryForCompleted() == QueryState.QUERY_SUCCEEDED) {
            SubQuery subQuery = query.getSubQuery(castEvent.getSubQueryId());
            TableDesc desc = new TableDescImpl(query.conf.getOutputTable(),
                subQuery.getTableMeta(), query.context.getOutputPath());
            query.setResultDesc(desc);
            try {
              query.writeStat(query.context.getOutputPath(), subQuery);
            } catch (IOException e) {
              e.printStackTrace();
            }
            query.eventHandler.handle(new QueryFinishEvent(query.getId()));

            if (query.context.isCreateTableQuery()) {
              query.context.getCatalog().addTable(desc);
            }
          }

          return query.finished(QueryState.QUERY_SUCCEEDED);
        }
      } else {
        // if at least one subquery is failed, the query is also failed.
        return QueryState.QUERY_FAILED;
      }
    }
  }

  private static class DiagnosticsUpdateTransition implements
      SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      query.addDiagnostic(((QueryDiagnosticsUpdateEvent) event)
          .getDiagnosticUpdate());
    }
  }

  private static class InitCompleteTransition implements
      SingleArcTransition<Query, QueryEvent> {
    @Override
    public void transition(Query query, QueryEvent event) {
      if (query.initializationTime == 0) {
        query.setInitializationTime();
      }
    }
  }

  private static class InternalErrorTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent event) {
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
        eventHandler.handle(new QueryEvent(this.id,
            QueryEventType.INTERNAL_ERROR));
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

  private void writeStat(Path outputPath, SubQuery subQuery)
      throws IOException {
    ExecutionBlock execBlock = subQuery.getBlock();
    if (execBlock.getPlan().getType() == ExprType.CREATE_INDEX) {
      IndexWriteNode index = (IndexWriteNode) execBlock.getPlan();
      Path indexPath = new Path(sm.getTablePath(index.getTableName()), "index");
      TableMeta meta;
      if (sm.getFileSystem().exists(new Path(indexPath, ".meta"))) {
        meta = sm.getTableMeta(indexPath);
      } else {
        meta = TCatUtil
            .newTableMeta(execBlock.getOutputSchema(), StoreType.CSV);
      }
      String indexName = IndexUtil.getIndexName(index.getTableName(),
          index.getSortSpecs());
      String json = GsonCreator.getInstance().toJson(index.getSortSpecs());
      meta.putOption(indexName, json);

      sm.writeTableMeta(indexPath, meta);

    } else {
      sm.writeTableMeta(outputPath, subQuery.getTableMeta());
    }
  }
}
