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
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.TajoProtos.QueryState;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.IndexWriteNode;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.event.*;
import tajo.storage.StorageManager;
import tajo.util.IndexUtil;

import java.io.IOException;
import java.util.*;
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
  private PriorityQueue<SubQuery> scheduleQueue;
  private QueryContext context;

  // Query Status
  private final QueryId id;
  private long appSubmitTime;
  private long startTime;
  private long initializationTime;
  private long finishTime;
  private float progress;
  private TableDesc resultDesc;
  private int completedSubQueryCount = 0;
  private final List<String> diagnostics = new ArrayList<>();

  // Internal Variables
  private final Lock readLock;
  private final Lock writeLock;

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
               final GlobalPlanner planner,
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

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.scheduleQueue = new PriorityQueue<>(1,new PriorityComparator());

    stateMachine = stateMachineFactory.make(this);
  }

  public boolean isCreateTableStmt() {
    return context.isCreateTableQuery();
  }

  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  protected StorageManager getStorageManager() {
    return this.sm;
  }

  public float getProgress() {
    return progress;
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

  class PriorityComparator implements Comparator<SubQuery> {
    public PriorityComparator() {

    }

    @Override
    public int compare(SubQuery s1, SubQuery s2) {
      return s1.getPriority().get() - s2.getPriority().get();
    }
  }

  public MasterPlan getPlan() {
    return plan;
  }

  public StateMachine<QueryState, QueryEventType, QueryEvent> getStateMachine() {
    return stateMachine;
  }
  
  public void addSubQuery(SubQuery q) {
    q.setQueryContext(context);
    q.setEventHandler(eventHandler);
    q.setClock(clock);
    subqueries.put(q.getId(), q);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public String getQueryStr() {
    return this.queryStr;
  }

  public Iterator<SubQuery> getSubQueryIterator() {
    return this.subqueries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
  
  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getSubQuery(id.getSubQueryId()).getQueryUnit(id);
  }

  public QueryState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public int getScheduleQueueSize() {
    return scheduleQueue.size();
  }

  static class InitTransition
      implements MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent queryEvent) {
      query.setStartTime();
      scheduleSubQueriesPostfix(query);
      LOG.info("Scheduled SubQueries: " + query.getScheduleQueueSize());

      return QueryState.QUERY_INIT;
    }

    private int priority = 0;

    private void scheduleSubQueriesPostfix(Query query) {
      SubQuery root = query.getPlan().getRoot();

      scheduleSubQueriesPostfix_(query, root);

      root.setPriority(priority);
      query.addSubQuery(root);
      query.schedule(root);
    }
    private void scheduleSubQueriesPostfix_(Query query, SubQuery current) {
      if (current.hasChildQuery()) {
        if (current.getChildQueries().size() == 1) {
          SubQuery subQuery = current.getChildQueries().iterator().next();
          scheduleSubQueriesPostfix_(query, subQuery);
          identifySubQuery(subQuery);

          query.addSubQuery(subQuery);
          query.schedule(subQuery);

          priority++;
        } else {
          Iterator<SubQuery> it = current.getChildQueries().iterator();
          SubQuery outer = it.next();
          SubQuery inner = it.next();

          // Switch between outer and inner
          // if an inner has a child and an outer doesn't.
          // It is for left-deep-first search.
          if (!outer.hasChildQuery() && inner.hasChildQuery()) {
            SubQuery tmp = outer;
            outer = inner;
            inner = tmp;
          }

          scheduleSubQueriesPostfix_(query, outer);
          scheduleSubQueriesPostfix_(query, inner);

          identifySubQuery(outer);
          identifySubQuery(inner);

          query.addSubQuery(outer);
          query.schedule(outer);
          query.addSubQuery(inner);
          query.schedule(inner);

          priority++;
        }
      }
    }

    private void identifySubQuery(SubQuery subQuery) {
      SubQuery parent = subQuery.getParentQuery();

      if (!subQuery.hasChildQuery()) {

        if (parent.getScanNodes().length == 2) {
          Iterator<SubQuery> childIter = subQuery.getParentQuery().getChildIterator();

          while (childIter.hasNext()) {
            SubQuery another = childIter.next();
            if (!subQuery.equals(another)) {
              if (another.hasChildQuery()) {
                subQuery.setPriority(++priority);
              }
            }
          }

          if (subQuery.getPriority() == null) {
            subQuery.setPriority(0);
          }
        } else {
          // if subQuery is leaf and not part of join.
          if (!subQuery.hasChildQuery()) {
            subQuery.setPriority(0);
          }
        }
      } else {
        subQuery.setPriority(priority);
      }
    }

    private void scheduleSubQueries(Query query, SubQuery current) {
      int priority = 0;

      if (current.hasChildQuery()) {

        int maxPriority = 0;
        Iterator<SubQuery> it = current.getChildIterator();

        while (it.hasNext()) {
          SubQuery su = it.next();
          scheduleSubQueries(query, su);

          if (su.getPriority().get() > maxPriority) {
            maxPriority = su.getPriority().get();
          }
        }

        priority = maxPriority + 1;

      } else {
        SubQuery parent = current.getParentQuery();
        priority = 0;

        if (parent.getScanNodes().length == 2) {
          Iterator<SubQuery> childIter = current.getParentQuery().getChildIterator();

          while (childIter.hasNext()) {
            SubQuery another = childIter.next();
            if (!current.equals(another)) {
              if (another.hasChildQuery()) {
                priority = another.getPriority().get() + 1;
              }
            }
          }
        }
      }

      current.setPriority(priority);
      // TODO
      query.addSubQuery(current);
      query.schedule(current);
    }
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {
      SubQuery subQuery = query.takeSubQuery();
      LOG.info("Schedule unit plan: \n" + subQuery.getLogicalPlan());
      subQuery.handle(new SubQueryEvent(subQuery.getId(),
          SubQueryEventType.SQ_INIT));
    }
  }

  public static class SubQueryCompletedTransition implements
                                                  MultipleArcTransition<Query, QueryEvent, QueryState> {

    @Override
    public QueryState transition(Query query, QueryEvent event) {

      query.completedSubQueryCount++;
      SubQueryCompletedEvent castEvent = (SubQueryCompletedEvent) event;

      if (castEvent.getFinalState() == SubQueryState.SUCCEEDED) {
        SubQuerySucceeEvent succeeEvent = (SubQuerySucceeEvent) castEvent;

        SubQuery nextSubQuery = query.takeSubQuery();

        if (nextSubQuery == null) {
          if (query.checkQueryForCompleted() == QueryState.QUERY_SUCCEEDED) {
            SubQuery subQuery = query.getSubQuery(castEvent.getSubQueryId());
            TableDesc desc = new TableDescImpl(query.conf.getOutputTable(),
                succeeEvent.getTableMeta(), query.context.getOutputPath());
            query.setResultDesc(desc);
            try {
              query.writeStat(query.context.getOutputPath(), subQuery, succeeEvent.getTableMeta().getStat());
            } catch (IOException e) {
              e.printStackTrace();
            }
            query.eventHandler.handle(new QueryFinishEvent(query.getId()));

            if (query.context.isCreateTableQuery()) {
              query.context.getCatalog().addTable(desc);
            }
            return query.finished(QueryState.QUERY_SUCCEEDED);
          }
        }

        nextSubQuery.handle(new SubQueryEvent(nextSubQuery.getId(),
            SubQueryEventType.SQ_INIT));
        LOG.info("Scheduling SubQuery's Priority: " + (100 - nextSubQuery.getPriority().get()));
        LOG.info("Scheduling SubQuery's Plan: \n" + nextSubQuery.getLogicalPlan());
        QueryState state = query.checkQueryForCompleted();
        return state;
      } else if (castEvent.getFinalState() == SubQueryState.FAILED) {
        return QueryState.QUERY_FAILED;
      } else {
        return query.checkQueryForCompleted();
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

  public void schedule(SubQuery subQuery) {
    scheduleQueue.add(subQuery);
  }

  private SubQuery takeSubQuery() {
    SubQuery unit = removeFromScheduleQueue();
    if (unit == null) {
      return null;
    }
    List<SubQuery> pended = new ArrayList<>();
    Priority priority = unit.getPriority();

    do {
      if (isReady(unit)) {
        break;
      } else {
        pended.add(unit);
      }
      unit = removeFromScheduleQueue();
      if (unit == null) {
        scheduleQueue.addAll(pended);
        return null;
      }
    } while (priority.equals(unit.getPriority()));

    if (!priority.equals(unit.getPriority())) {
      pended.add(unit);
      unit = null;
    }
    scheduleQueue.addAll(pended);
    return unit;
  }

  private boolean isReady(SubQuery subQuery) {
    if (subQuery.hasChildQuery()) {
      for (SubQuery child : subQuery.getChildQueries()) {
        if (child.getState() !=
            SubQueryState.SUCCEEDED) {
          return false;
        }
      }
      return true;
    } else {
      return true;
    }
  }

  private SubQuery removeFromScheduleQueue() {
    if (scheduleQueue.isEmpty()) {
      return null;
    } else {
      return scheduleQueue.remove();
    }
  }



  private void writeStat(Path outputPath, SubQuery subQuery, TableStat stat)
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
      sm.writeTableMeta(outputPath, meta);
    }
  }
}
