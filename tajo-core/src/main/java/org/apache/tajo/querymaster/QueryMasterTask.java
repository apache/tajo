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
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.*;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.RpcParameterFactory;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.apache.tajo.worker.event.NodeResourceEvent;
import org.apache.tajo.worker.event.NodeStatusEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.tajo.ResourceProtos.TaskFatalErrorReport;
import static org.apache.tajo.TajoProtos.QueryState;

public class QueryMasterTask extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryMasterTask.class.getName());

  private QueryId queryId;

  private Session session;

  private QueryContext queryContext;

  private QueryMasterTaskContext queryTaskContext;

  private QueryMaster.QueryMasterContext queryMasterContext;

  private Query query;

  private String jsonExpr;

  private AsyncDispatcher dispatcher;

  private final long querySubmitTime;

  private final Map<Integer, TableDesc> tableDescMap = new HashMap<>();

  private TajoConf systemConf;

  private Properties rpcParams;

  private AtomicLong lastClientHeartbeat = new AtomicLong(-1);

  private volatile boolean isStopped;

  private Throwable initError;

  private NodeResource allocation;

  private final List<TaskFatalErrorReport> diagnostics = new ArrayList<>();

  private final ConcurrentMap<Integer, WorkerConnectionInfo> workerMap = Maps.newConcurrentMap();

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, Session session, QueryContext queryContext,
                         String jsonExpr, NodeResource allocation, AsyncDispatcher dispatcher) {

    super(QueryMasterTask.class.getName());
    this.queryMasterContext = queryMasterContext;
    this.queryId = queryId;
    this.session = session;
    this.queryContext = queryContext;
    this.jsonExpr = jsonExpr;
    this.allocation = allocation;
    this.querySubmitTime = System.currentTimeMillis();
    this.dispatcher = dispatcher;
  }

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, Session session, QueryContext queryContext,
                         String jsonExpr,
                         NodeResource allocation) {
    this(queryMasterContext, queryId, session, queryContext, jsonExpr, allocation, new AsyncDispatcher());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    rpcParams = RpcParameterFactory.get(systemConf);

    queryTaskContext = new QueryMasterTaskContext();

    addService(dispatcher);

    dispatcher.register(StageEventType.class, new StageEventDispatcher());
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
    dispatcher.register(QueryMasterQueryCompletedEvent.EventType.class, new QueryFinishEventHandler());
    dispatcher.register(TaskSchedulerEvent.EventType.class, new TaskSchedulerDispatcher());
    dispatcher.register(LocalTaskEventType.class, new LocalTaskEventHandler());

    super.serviceInit(systemConf);
  }

  public boolean isStopped() {
    return isStopped;
  }

  @Override
  public void serviceStart() throws Exception {
    startQuery();
    List<TajoProtos.WorkerConnectionInfoProto> workersProto = queryMasterContext.getQueryMaster().getAllWorker();
    for (TajoProtos.WorkerConnectionInfoProto worker : workersProto) {
      workerMap.put(worker.getId(), new WorkerConnectionInfo(worker));
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    isStopped = true;

    LOG.info("Stopping QueryMasterTask:" + queryId);

    //release QM resource
    EventHandler handler = getQueryTaskContext().getQueryMasterContext().getWorkerContext().
        getNodeResourceManager().getDispatcher().getEventHandler();

    handler.handle(new NodeResourceDeallocateEvent(allocation, NodeResourceEvent.ResourceType.QUERY_MASTER));

    //flush current node resource
    handler.handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));

    if (!queryContext.getBool(SessionVars.DEBUG_ENABLED)) {
      cleanupQuery(getQueryId());
    }

    super.serviceStop();
    LOG.info("Stopped QueryMasterTask:" + queryId);
  }

  public void handleTaskFailed(TaskFatalErrorReport report) {
    synchronized(diagnostics) {
      if (diagnostics.size() < 10) {
        diagnostics.add(report);
      }
    }

    getEventHandler().handle(new TaskFatalErrorEvent(report));
  }

  public Collection<TaskFatalErrorReport> getDiagnostics() {
    synchronized(diagnostics) {
      return Collections.unmodifiableCollection(diagnostics);
    }
  }

  private class StageEventDispatcher implements EventHandler<StageEvent> {
    public void handle(StageEvent event) {
      ExecutionBlockId id = event.getStageId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("StageEventDispatcher:" + id + "," + event.getType());
      }
      query.getStage(id).handle(event);
    }
  }

  private class TaskEventDispatcher
      implements EventHandler<TaskEvent> {
    public void handle(TaskEvent event) {
      TaskId taskId = event.getTaskId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("TaskEventDispatcher>" + taskId + "," + event.getType());
      }
      Task task = query.getStage(taskId.getExecutionBlockId()).
          getTask(taskId);
      task.handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
      implements EventHandler<TaskAttemptEvent> {
    public void handle(TaskAttemptEvent event) {
      TaskAttemptId attemptId = event.getTaskAttemptId();
      Stage stage = query.getStage(attemptId.getTaskId().getExecutionBlockId());
      Task task = stage.getTask(attemptId.getTaskId());
      TaskAttempt attempt = task.getAttempt(attemptId);
      attempt.handle(event);
    }
  }

  private class TaskSchedulerDispatcher
      implements EventHandler<TaskSchedulerEvent> {
    public void handle(TaskSchedulerEvent event) {
      Stage stage = query.getStage(event.getExecutionBlockId());
      stage.getTaskScheduler().handle(event);
    }
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param workerId worker unique Id.
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  protected void killTaskAttempt(int workerId, TaskAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc;
    ExecutionBlockId ebId = taskAttemptId.getTaskId().getExecutionBlockId();
    InetSocketAddress workerAddress = getQuery().getStage(ebId).getAssignedWorkerMap().get(workerId);

    try {
      tajoWorkerRpc = RpcClientManager.getInstance().getClient(workerAddress, TajoWorkerProtocol.class, true,
          rpcParams);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      CallFuture<PrimitiveProtos.BoolProto> callFuture = new CallFuture<>();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), callFuture);

      if(!callFuture.get().getValue()){
        getEventHandler().handle(
            new TaskFatalErrorEvent(taskAttemptId, new TajoInternalError("Can't kill task :" + taskAttemptId)));
      }
    } catch (Exception e) {
      /* Node RPC failure */
      LOG.error(e.getMessage(), e);
      getEventHandler().handle(new TaskFatalErrorEvent(taskAttemptId, e));
    }
  }

  private class LocalTaskEventHandler implements EventHandler<LocalTaskEvent> {
    @Override
    public void handle(final LocalTaskEvent event) {
      queryMasterContext.getEventExecutor().submit(new Runnable() {
        @Override
        public void run() {
          killTaskAttempt(event.getWorkerId(), event.getTaskAttemptId());
        }
      });
    }
  }

  private class QueryFinishEventHandler implements EventHandler<QueryMasterQueryCompletedEvent> {

    @Override
    public void handle(QueryMasterQueryCompletedEvent event) {

      QueryId queryId = event.getQueryId();
      LOG.info("Query completion notified from " + queryId + " final state: " + query.getSynchronizedState());
      queryMasterContext.getEventHandler().handle(new QueryStopEvent(queryId));
    }
  }

  private static boolean isTerminatedState(QueryState state) {
    return
        state == QueryState.QUERY_SUCCEEDED ||
        state == QueryState.QUERY_FAILED ||
        state == QueryState.QUERY_KILLED ||
        state == QueryState.QUERY_ERROR;
  }

  private LogicalPlan plan;

  public synchronized void startQuery() {
    Tablespace space = null;
    try {
      if (query != null) {
        LOG.warn("Query already started");
        return;
      }
      LOG.info(SessionVars.INDEX_ENABLED.keyname() + " : " + queryContext.getBool(SessionVars.INDEX_ENABLED));
      CatalogService catalog = getQueryTaskContext().getQueryMasterContext().getWorkerContext().getCatalog();
      LogicalPlanner planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
      LogicalOptimizer optimizer = new LogicalOptimizer(systemConf, catalog, TablespaceManager.getInstance());
      Expr expr = JsonHelper.fromJson(jsonExpr, Expr.class);
      jsonExpr = null; // remove the possible OOM

      plan = planner.createPlan(queryContext, expr);
      optimizer.optimize(queryContext, plan);

      // when a given uri is null, TablespaceManager.get will return the default tablespace.
      space = TablespaceManager.get(queryContext.get(QueryVars.OUTPUT_TABLE_URI, ""));
      space.rewritePlan(queryContext, plan);

      initStagingDir();

      for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
        LogicalNode[] scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.SCAN);
        if (scanNodes != null) {
          for (LogicalNode eachScanNode : scanNodes) {
            ScanNode scanNode = (ScanNode) eachScanNode;
            tableDescMap.put(scanNode.getPID(), scanNode.getTableDesc());
          }
        }

        scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.PARTITIONS_SCAN);
        if (scanNodes != null) {
          for (LogicalNode eachScanNode : scanNodes) {
            ScanNode scanNode = (ScanNode) eachScanNode;
            tableDescMap.put(scanNode.getPID(), scanNode.getTableDesc());
          }
        }

        scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.INDEX_SCAN);
        if (scanNodes != null) {
          for (LogicalNode eachScanNode : scanNodes) {
            ScanNode scanNode = (ScanNode) eachScanNode;
            tableDescMap.put(scanNode.getPID(), scanNode.getTableDesc());
          }
        }
      }
      MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
      queryMasterContext.getGlobalPlanner().build(queryContext, masterPlan);

      query = new Query(queryTaskContext, queryId, querySubmitTime,
          "", queryTaskContext.getEventHandler(), masterPlan);

      dispatcher.register(QueryEventType.class, query);
      queryTaskContext.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.START));
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      initError = t;

      if (plan != null && space != null) {
        LogicalRootNode rootNode = plan.getRootBlock().getRoot();
        try {
          space.rollbackTable(rootNode.getChild());
        } catch (Throwable e) {
          LOG.warn(query.getId() + ", failed processing cleanup storage when query failed:" + e.getMessage(), e);
        }
      }
    }
  }

  private void initStagingDir() throws IOException {
    URI stagingDir;

    try {
      Tablespace tablespace = TablespaceManager.get(queryContext.get(QueryVars.OUTPUT_TABLE_URI, ""));
      TableDesc desc = PlannerUtil.getOutputTableDesc(plan);

      FormatProperty formatProperty = tablespace.getFormatProperty(desc.getMeta());
      if (formatProperty.isStagingSupport()) {
        stagingDir = tablespace.prepareStagingSpace(systemConf, queryId.toString(), queryContext, desc.getMeta());

        // Create a staging space
        LOG.info("The staging dir '" + stagingDir + "' is created.");
        queryContext.setStagingDir(stagingDir);
      }

    } catch (IOException ioe) {
      LOG.warn("Creating staging space has been failed.", ioe);
      throw ioe;
    }
  }

  public Query getQuery() {
    return query;
  }

  protected void expireQuerySession() {
    if(!isTerminatedState(query.getState()) && !(query.getState() == QueryState.QUERY_KILL_WAIT)){
      query.handle(new QueryEvent(queryId, QueryEventType.KILL));
    }
  }

  public QueryMasterTaskContext getQueryTaskContext() {
    return queryTaskContext;
  }

  public EventHandler getEventHandler() {
    return queryTaskContext.getEventHandler();
  }

  public void touchSessionTime() {
    this.lastClientHeartbeat.set(System.currentTimeMillis());
  }

  public long getLastClientHeartbeat() {
    return this.lastClientHeartbeat.get();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public boolean isInitError() {
    return initError != null;
  }

  public QueryState getState() {
    if(query == null) {
      if (isInitError()) {
        return QueryState.QUERY_ERROR;
      } else {
        return QueryState.QUERY_NOT_ASSIGNED;
      }
    } else {
      return query.getState();
    }
  }

  public Throwable getInitError() {
    return initError;
  }

  public String getErrorMessage() {
    if (isInitError()) {
      return StringUtils.stringifyException(initError);
    } else {
      return null;
    }
  }

  public long getQuerySubmitTime() {
    return this.querySubmitTime;
  }

  private void cleanupQuery(final QueryId queryId) {
    if (getQuery() != null) {
      Set<InetSocketAddress> workers = Sets.newHashSet();
      for (Stage stage : getQuery().getStages()) {
        workers.addAll(stage.getAssignedWorkerMap().values());
      }

      LOG.info("Cleanup resources of all workers. Query: " + queryId + ", workers: " + workers.size());
      for (final InetSocketAddress worker : workers) {
        queryMasterContext.getEventExecutor().submit(new Runnable() {
          @Override
          public void run() {
            try {
              AsyncRpcClient rpc = RpcClientManager.getInstance().getClient(worker, TajoWorkerProtocol.class, true,
                  rpcParams);
              TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerProtocolService = rpc.getStub();
              tajoWorkerProtocolService.stopQuery(null, queryId.getProto(), NullCallback.get());
            } catch (Throwable e) {
              LOG.error(e.getMessage(), e);
            }
          }
        });
      }
    }
  }

  public class QueryMasterTaskContext {
    EventHandler eventHandler;
    public QueryMaster.QueryMasterContext getQueryMasterContext() {
      return queryMasterContext;
    }

    public Session getSession() {
      return session;
    }

    public QueryContext getQueryContext() {
      return queryContext;
    }

    public TajoConf getConf() {
      return systemConf;
    }

    public Clock getClock() {
      return queryMasterContext.getClock();
    }

    public Query getQuery() {
      return query;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public Path getStagingDir() {
      return queryContext.getStagingDir();
    }

    public synchronized EventHandler getEventHandler() {
      if(eventHandler == null) {
        eventHandler = dispatcher.getEventHandler();
      }
      return eventHandler;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Stage getStage(ExecutionBlockId id) {
      return query.getStage(id);
    }

    public TableDesc getTableDesc(ScanNode scanNode) {
      return tableDescMap.get(scanNode.getPID());
    }

    public float getProgress() {
      if(query == null) {
        return 0.0f;
      }
      return query.getProgress();
    }

    /**
     * A key is worker id, and a value is a worker connection information.
     */
    public ConcurrentMap<Integer, WorkerConnectionInfo> getWorkerMap() {
      return workerMap;
    }
  }
}