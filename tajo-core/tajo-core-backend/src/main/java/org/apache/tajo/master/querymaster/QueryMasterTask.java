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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.HiveConverter;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.master.TajoAsyncDispatcher;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.util.metrics.TajoMetrics;
import org.apache.tajo.util.metrics.reporter.MetricsConsoleReporter;
import org.apache.tajo.worker.AbstractResourceAllocator;
import org.apache.tajo.worker.TajoResourceAllocator;
import org.apache.tajo.worker.YarnResourceAllocator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueryMasterTask extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryMasterTask.class.getName());

  // query submission directory is private!
  final public static FsPermission STAGING_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------

  private QueryId queryId;

  private QueryContext queryContext;

  private QueryMasterTaskContext queryTaskContext;

  private QueryMaster.QueryMasterContext queryMasterContext;

  private Query query;

  private MasterPlan masterPlan;

  private String sql;

  private String logicalPlanJson;

  private TajoAsyncDispatcher dispatcher;

  private final long querySubmitTime;

  private Map<String, TableDesc> tableDescMap = new HashMap<String, TableDesc>();

  private TajoConf systemConf;

  private AtomicLong lastClientHeartbeat = new AtomicLong(-1);

  private AbstractResourceAllocator resourceAllocator;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private TajoMetrics queryMetrics;

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, QueryContext queryContext, String sql, String logicalPlanJson) {
    super(QueryMasterTask.class.getName());
    this.queryMasterContext = queryMasterContext;
    this.queryId = queryId;
    this.queryContext = queryContext;
    this.sql = sql;
    this.logicalPlanJson = logicalPlanJson;
    this.querySubmitTime = System.currentTimeMillis();
  }

  @Override
  public void init(Configuration conf) {
    systemConf = (TajoConf)conf;

    try {
      queryTaskContext = new QueryMasterTaskContext();
      String resourceManagerClassName = systemConf.getVar(TajoConf.ConfVars.RESOURCE_MANAGER_CLASS);

      if(resourceManagerClassName.indexOf(TajoWorkerResourceManager.class.getName()) >= 0) {
        resourceAllocator = new TajoResourceAllocator(queryTaskContext);
      } else {
        resourceAllocator = new YarnResourceAllocator(queryTaskContext);
      }
      addService(resourceAllocator);

      dispatcher = new TajoAsyncDispatcher(queryId.toString());
      addService(dispatcher);

      dispatcher.register(SubQueryEventType.class, new SubQueryEventDispatcher());
      dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
      dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
      dispatcher.register(QueryFinishEvent.EventType.class, new QueryFinishEventHandler());
      dispatcher.register(TaskSchedulerEvent.EventType.class, new TaskSchedulerDispatcher());

      initStagingDir();

      queryMetrics = new TajoMetrics(queryId.toString());

      super.init(systemConf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public boolean isStopped() {
    return stopped.get();
  }

  @Override
  public void start() {
    startQuery();
    super.start();
  }

  @Override
  public void stop() {

    if(stopped.getAndSet(true)) {
      return;
    }

    LOG.info("Stopping QueryMasterTask:" + queryId);

    CallFuture future = new CallFuture();

    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryMasterContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(queryMasterContext.getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.stopQueryMaster(null, queryId.getProto(), future);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }

    try {
      future.get(3, TimeUnit.SECONDS);
    } catch (Throwable t) {
      LOG.warn(t);
    }

    super.stop();

    //TODO change report to tajo master
    queryMetrics.report(new MetricsConsoleReporter());

    LOG.info("Stopped QueryMasterTask:" + queryId);
  }

  public void handleTaskRequestEvent(TaskRequestEvent event) {
    ExecutionBlockId id = event.getExecutionBlockId();
    query.getSubQuery(id).handleTaskRequestEvent(event);
  }

  private class SubQueryEventDispatcher implements EventHandler<SubQueryEvent> {
    public void handle(SubQueryEvent event) {
      ExecutionBlockId id = event.getSubQueryId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("SubQueryEventDispatcher:" + id + "," + event.getType());
      }
      //Query query = queryMasterTasks.get(id.getQueryId()).getQuery();
      query.getSubQuery(id).handle(event);
    }
  }

  private class TaskEventDispatcher
      implements EventHandler<TaskEvent> {
    public void handle(TaskEvent event) {
      QueryUnitId taskId = event.getTaskId();
      if(LOG.isDebugEnabled()) {
        LOG.debug("TaskEventDispatcher>" + taskId + "," + event.getType());
      }
      //Query query = queryMasterTasks.get(taskId.getExecutionBlockId().getQueryId()).getQuery();
      QueryUnit task = query.getSubQuery(taskId.getExecutionBlockId()).
          getQueryUnit(taskId);
      task.handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
      implements EventHandler<TaskAttemptEvent> {
    public void handle(TaskAttemptEvent event) {
      QueryUnitAttemptId attemptId = event.getTaskAttemptId();
      //Query query = queryMasterTasks.get(attemptId.getQueryUnitId().getExecutionBlockId().getQueryId()).getQuery();
      SubQuery subQuery = query.getSubQuery(attemptId.getQueryUnitId().getExecutionBlockId());
      QueryUnit task = subQuery.getQueryUnit(attemptId.getQueryUnitId());
      QueryUnitAttempt attempt = task.getAttempt(attemptId);
      attempt.handle(event);
    }
  }

  private class TaskSchedulerDispatcher
      implements EventHandler<TaskSchedulerEvent> {
    public void handle(TaskSchedulerEvent event) {
      //Query query = queryMasterTasks.get(event.getExecutionBlockId().getQueryId()).getQuery();
      SubQuery subQuery = query.getSubQuery(event.getExecutionBlockId());
      subQuery.getTaskScheduler().handle(event);
    }
  }

  private static class QueryFinishEventHandler implements EventHandler<QueryFinishEvent> {
    @Override
    public void handle(QueryFinishEvent event) {
      QueryId queryId = event.getQueryId();
      LOG.info("Query end notification started for QueryId : " + queryId);
      //QueryMaster must be lived until client fetching all query result data.
    }
  }

  public synchronized void startQuery() {
    if(query != null) {
      LOG.warn("Query already started");
      return;
    }

    CatalogService catalog = getQueryTaskContext().getQueryMasterContext().getWorkerContext().getCatalog();
    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(systemConf);
    Expr expr;
    if (queryContext.isHiveQueryMode()) {
      HiveConverter hiveConverter = new HiveConverter();
      expr = hiveConverter.parse(sql);
    } else {
      SQLAnalyzer analyzer = new SQLAnalyzer();
      expr = analyzer.parse(sql);
    }
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(expr);
      optimizer.optimize(plan);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    GlobalEngine.DistributedQueryHookManager hookManager = new GlobalEngine.DistributedQueryHookManager();
    hookManager.addHook(new GlobalEngine.InsertHook());
    hookManager.doHooks(queryContext, plan);

    try {

      for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
        LogicalNode[] scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.SCAN);
        if(scanNodes != null) {
          for(LogicalNode eachScanNode: scanNodes) {
            ScanNode scanNode = (ScanNode)eachScanNode;
            tableDescMap.put(scanNode.getCanonicalName(), scanNode.getTableDesc());
          }
        }

        scanNodes = PlannerUtil.findAllNodes(block.getRoot(), NodeType.PARTITIONS_SCAN);
        if(scanNodes != null) {
          for(LogicalNode eachScanNode: scanNodes) {
            ScanNode scanNode = (ScanNode)eachScanNode;
            tableDescMap.put(scanNode.getCanonicalName(), scanNode.getTableDesc());
          }
        }
      }

      MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
      queryMasterContext.getGlobalPlanner().build(masterPlan);
      //this.masterPlan = queryMasterContext.getGlobalOptimizer().optimize(masterPlan);

      query = new Query(queryTaskContext, queryId, querySubmitTime,
          "", queryTaskContext.getEventHandler(), masterPlan);

      dispatcher.register(QueryEventType.class, query);

      queryTaskContext.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.START));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      //TODO how set query failed(???)
      //send FAIL query status
      //this.statusMessage = StringUtils.stringifyException(e);
    }
  }

  /**
   * It initializes the final output and staging directory and sets
   * them to variables.
   */
  private void initStagingDir() throws IOException {

    String realUser;
    String currentUser;
    UserGroupInformation ugi;
    ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    FileSystem defaultFS = TajoConf.getWarehouseDir(systemConf).getFileSystem(systemConf);

    Path stagingDir = null;
    Path outputDir = null;
    try {
      ////////////////////////////////////////////
      // Create Output Directory
      ////////////////////////////////////////////

      stagingDir = new Path(TajoConf.getStagingDir(systemConf), queryId.toString());

      if (defaultFS.exists(stagingDir)) {
        throw new IOException("The staging directory '" + stagingDir + "' already exists");
      }
      defaultFS.mkdirs(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
      FileStatus fsStatus = defaultFS.getFileStatus(stagingDir);
      String owner = fsStatus.getOwner();

      if (!owner.isEmpty() && !(owner.equals(currentUser) || owner.equals(realUser))) {
        throw new IOException("The ownership on the user's query " +
            "directory " + stagingDir + " is not as expected. " +
            "It is owned by " + owner + ". The directory must " +
            "be owned by the submitter " + currentUser + " or " +
            "by " + realUser);
      }

      if (!fsStatus.getPermission().equals(STAGING_DIR_PERMISSION)) {
        LOG.info("Permissions on staging directory " + stagingDir + " are " +
            "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
            "to correct value " + STAGING_DIR_PERMISSION);
        defaultFS.setPermission(stagingDir, new FsPermission(STAGING_DIR_PERMISSION));
      }

      // Create a subdirectories
      defaultFS.mkdirs(new Path(stagingDir, TajoConstants.RESULT_DIR_NAME));
      LOG.info("The staging dir '" + outputDir + "' is created.");
      queryContext.setStagingDir(stagingDir);

      /////////////////////////////////////////////////
      // Check and Create Output Directory If Necessary
      /////////////////////////////////////////////////
      if (queryContext.hasOutputPath()) {
        outputDir = queryContext.getOutputPath();
        if (queryContext.isOutputOverwrite()) {
          if (defaultFS.exists(outputDir.getParent())) {
            if (defaultFS.exists(outputDir)) {
              defaultFS.delete(outputDir, true);
              LOG.info("The output directory '" + outputDir + "' is cleaned.");
            }
          } else {
            defaultFS.mkdirs(outputDir.getParent());
            LOG.info("The output directory's parent '" + outputDir.getParent() + "' is created.");
          }
        } else {
          if (defaultFS.exists(outputDir)) {
            throw new IOException("The output directory '" + outputDir + " already exists.");
          }
        }
      }
    } catch (IOException ioe) {
      if (stagingDir != null && defaultFS.exists(stagingDir)) {
        defaultFS.delete(stagingDir, true);
        LOG.info("The staging directory '" + stagingDir + "' is deleted");
      }

      if (outputDir != null && defaultFS.exists(outputDir)) {
        defaultFS.delete(outputDir, true);
        LOG.info("The output directory '" + outputDir + "' is deleted");
      }

      throw ioe;
    }
  }

  public Query getQuery() {
    return query;
  }

  public void expiredSessionTimeout() {
    stop();
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

  public TajoProtos.QueryState getState() {
    if(query == null) {
      return TajoProtos.QueryState.QUERY_NOT_ASSIGNED;
    } else {
      return query.getState();
    }
  }

  public class QueryMasterTaskContext {
    EventHandler eventHandler;
    public QueryMaster.QueryMasterContext getQueryMasterContext() {
      return queryMasterContext;
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

    public AbstractStorageManager getStorageManager() {
      return queryMasterContext.getStorageManager();
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

    public TajoAsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public SubQuery getSubQuery(ExecutionBlockId id) {
      return query.getSubQuery(id);
    }

    public Map<String, TableDesc> getTableDescMap() {
      return tableDescMap;
    }

    public float getProgress() {
      if(query == null) {
        return 0.0f;
      }
      return query.getProgress();
    }

    public AbstractResourceAllocator getResourceAllocator() {
      return resourceAllocator;
    }

    public TajoMetrics getQueryMetrics() {
      return queryMetrics;
    }
  }

}