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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.scheduler.SimpleFifoScheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QueryJobManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryJobManager.class.getName());

  // TajoMaster Context
  private final TajoMaster.MasterContext masterContext;

  private AsyncDispatcher dispatcher;

  private SimpleFifoScheduler scheduler;

  private final Map<QueryId, QueryInProgress> submittedQueries = new HashMap<QueryId, QueryInProgress>();

  private final Map<QueryId, QueryInProgress> runningQueries = new HashMap<QueryId, QueryInProgress>();

  private final Map<QueryId, QueryInProgress> finishedQueries = new HashMap<QueryId, QueryInProgress>();

  public QueryJobManager(final TajoMaster.MasterContext masterContext) {
    super(QueryJobManager.class.getName());
    this.masterContext = masterContext;
  }

  @Override
  public void init(Configuration conf) {
    try {
      this.dispatcher = new AsyncDispatcher();
      addService(this.dispatcher);

      this.dispatcher.register(QueryJobEvent.Type.class, new QueryJobManagerEventHandler());

      this.scheduler = new SimpleFifoScheduler(this);
    } catch (Exception e) {
      catchException(null, e);
    }

    super.init(conf);
  }

  @Override
  public void stop() {
    synchronized(runningQueries) {
      for(QueryInProgress eachQueryInProgress: runningQueries.values()) {
        eachQueryInProgress.stop();
      }
    }
    this.scheduler.stop();
    super.stop();
  }

  @Override
  public void start() {
    this.scheduler.start();
    super.start();
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public Collection<QueryInProgress> getSubmittedQueries() {
    synchronized (submittedQueries){
      return Collections.unmodifiableCollection(submittedQueries.values());
    }
  }

  public Collection<QueryInProgress> getRunningQueries() {
    synchronized (runningQueries){
      return Collections.unmodifiableCollection(runningQueries.values());
    }
  }

  public Collection<QueryInProgress> getFinishedQueries() {
    synchronized (finishedQueries){
      return Collections.unmodifiableCollection(finishedQueries.values());
    }
  }

  public QueryInfo createNewQueryJob(Session session, QueryContext queryContext, String sql,
                                     String jsonExpr, LogicalRootNode plan)
      throws Exception {
    QueryId queryId = QueryIdFactory.newQueryId(masterContext.getResourceManager().getSeedQueryId());
    QueryInProgress queryInProgress = new QueryInProgress(masterContext, session, queryContext, queryId, sql,
        jsonExpr, plan);

    synchronized (submittedQueries) {
      queryInProgress.getQueryInfo().setQueryMaster("");
      submittedQueries.put(queryInProgress.getQueryId(), queryInProgress);
    }

    scheduler.addQuery(queryInProgress);
    return queryInProgress.getQueryInfo();
  }

  public QueryInfo startQueryJob(QueryId queryId) throws Exception {

    QueryInProgress queryInProgress;

    synchronized (submittedQueries) {
      queryInProgress = submittedQueries.remove(queryId);
    }

    synchronized (runningQueries) {
      runningQueries.put(queryInProgress.getQueryId(), queryInProgress);
    }

    addService(queryInProgress);
    queryInProgress.init(getConfig());
    queryInProgress.start();

    if (!queryInProgress.startQueryMaster()) {
      stopQuery(queryId);
    }

    return queryInProgress.getQueryInfo();
  }

  public TajoMaster.MasterContext getMasterContext() {
    return masterContext;
  }

  class QueryJobManagerEventHandler implements EventHandler<QueryJobEvent> {
    @Override
    public void handle(QueryJobEvent event) {
      QueryInProgress queryInProgress = getQueryInProgress(event.getQueryInfo().getQueryId());
      if(queryInProgress == null) {
        LOG.warn("No query info in running queries.[" + event.getQueryInfo().getQueryId() + "]");
        return;
      }
      if(queryInProgress.isStarted()){
        queryInProgress.getEventHandler().handle(event);
      } else {
        if(event.getType() == QueryJobEvent.Type.QUERY_JOB_KILL){
          scheduler.removeQuery(queryInProgress.getQueryId());
          queryInProgress.getQueryInfo().setQueryState(TajoProtos.QueryState.QUERY_KILLED);

          stopQuery(queryInProgress.getQueryId());
        }
      }
    }
  }

  public QueryInProgress getQueryInProgress(QueryId queryId) {
    QueryInProgress queryInProgress;
    synchronized (submittedQueries) {
      queryInProgress = submittedQueries.get(queryId);
    }

    if (queryInProgress == null) {
      synchronized (runningQueries) {
        queryInProgress = runningQueries.get(queryId);
      }
    }
    return queryInProgress;
  }

  public QueryInProgress getFinishedQuery(QueryId queryId) {
    synchronized(finishedQueries) {
      return finishedQueries.get(queryId);
    }
  }

  public void stopQuery(QueryId queryId) {
    LOG.info("Stop QueryInProgress:" + queryId);
    QueryInProgress queryInProgress = getQueryInProgress(queryId);
    if(queryInProgress != null) {
      queryInProgress.stop();
      synchronized(submittedQueries) {
        submittedQueries.remove(queryId);
      }

      synchronized(runningQueries) {
        runningQueries.remove(queryId);
      }

      synchronized(finishedQueries) {
        finishedQueries.put(queryId, queryInProgress);
      }
    } else {
      LOG.warn("No QueryInProgress while query stopping: " + queryId);
    }
  }

  private void catchException(QueryId queryId, Exception e) {
    LOG.error(e.getMessage(), e);
    QueryInProgress queryInProgress = runningQueries.get(queryId);
    queryInProgress.catchException(e);
  }

  public synchronized TajoMasterProtocol.TajoHeartbeatResponse.ResponseCommand queryHeartbeat(
      TajoMasterProtocol.TajoHeartbeat queryHeartbeat) {
    QueryInProgress queryInProgress = getQueryInProgress(new QueryId(queryHeartbeat.getQueryId()));
    if(queryInProgress == null) {
      return null;
    }

    QueryInfo queryInfo = makeQueryInfoFromHeartbeat(queryHeartbeat);
    getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_HEARTBEAT, queryInfo));

    return null;
  }

  private QueryInfo makeQueryInfoFromHeartbeat(TajoMasterProtocol.TajoHeartbeat queryHeartbeat) {
    QueryInfo queryInfo = new QueryInfo(new QueryId(queryHeartbeat.getQueryId()));
    if(queryHeartbeat.getTajoWorkerHost() != null) {
      queryInfo.setQueryMaster(queryHeartbeat.getTajoWorkerHost());
      queryInfo.setQueryMasterPort(queryHeartbeat.getTajoQueryMasterPort());
      queryInfo.setQueryMasterclientPort(queryHeartbeat.getTajoWorkerClientPort());
    }
    queryInfo.setLastMessage(queryHeartbeat.getStatusMessage());
    queryInfo.setQueryState(queryHeartbeat.getState());
    queryInfo.setProgress(queryHeartbeat.getQueryProgress());

    if (queryHeartbeat.hasQueryFinishTime()) {
      queryInfo.setFinishTime(queryHeartbeat.getQueryFinishTime());
    }

    return queryInfo;
  }
}
