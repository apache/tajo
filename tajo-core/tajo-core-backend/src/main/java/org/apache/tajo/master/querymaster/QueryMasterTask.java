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
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.tajo.*;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.master.TajoAsyncDispatcher;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.worker.AbstractResourceAllocator;
import org.apache.tajo.worker.TajoResourceAllocator;
import org.apache.tajo.worker.YarnResourceAllocator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueryMasterTask extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryMasterTask.class.getName());

  // query submission directory is private!
  final public static FsPermission USER_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------

  private QueryId queryId;

  private QueryContext queryContext;

  private QueryMaster.QueryMasterContext queryMasterContext;

  private Query query;

  private MasterPlan masterPlan;

  private String logicalPlanJson;

  private TajoAsyncDispatcher dispatcher;

  private final long querySubmitTime;

  private boolean isCreateTableStmt;

  private Path outputPath;

  private Map<String, TableDesc> tableDescMap = new HashMap<String, TableDesc>();

  private QueryConf queryConf;

  private AtomicLong lastClientHeartbeat = new AtomicLong(-1);

  private AbstractResourceAllocator resourceAllocator;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  public QueryMasterTask(QueryMaster.QueryMasterContext queryMasterContext,
                         QueryId queryId, String logicalPlanJson) {
    super(QueryMasterTask.class.getName());
    this.queryMasterContext = queryMasterContext;
    this.queryId = queryId;
    this.logicalPlanJson = logicalPlanJson;
    this.querySubmitTime = System.currentTimeMillis();
  }

  @Override
  public void init(Configuration conf) {
    queryConf = new QueryConf(conf);
    queryConf.addResource(new Path(QueryConf.QUERY_MASTER_FILENAME));
    try {
      queryContext = new QueryContext();
      String resourceManagerClassName = conf.get("tajo.resource.manager",
          TajoWorkerResourceManager.class.getCanonicalName());

      if(resourceManagerClassName.indexOf(TajoWorkerResourceManager.class.getName()) >= 0) {
        resourceAllocator = new TajoResourceAllocator(queryContext);
      } else {
        resourceAllocator = new YarnResourceAllocator(queryContext);
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

      super.init(queryConf);
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
    if(stopped.get()) {
      return;
    }
    stopped.set(true);

    LOG.info("Stopping QueryMasterTask:" + queryId);

    queryMasterContext.getWorkerContext().getTajoMasterRpcClient()
        .stopQueryMaster(null, queryId.getProto(), NullCallback.get());

    super.stop();

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

  private class QueryFinishEventHandler implements EventHandler<QueryFinishEvent> {
    @Override
    public void handle(QueryFinishEvent event) {
      QueryId queryId = event.getQueryId();
      LOG.info("Query end notification started for QueryId : " + queryId);
      //QueryMaster must be lived until client fetching all query result data.
    }
  }

  public synchronized void startQuery() {
    LOG.info("Plan JSON:" + logicalPlanJson);
    if(query != null) {
      LOG.warn("Query already started");
      return;
    }

    try {
      LogicalRootNode logicalNodeRoot = (LogicalRootNode) CoreGsonHelper.fromJson(logicalPlanJson, LogicalNode.class);
      LogicalNode[] scanNodes = PlannerUtil.findAllNodes(logicalNodeRoot, NodeType.SCAN);
      if(scanNodes != null) {
        for(LogicalNode eachScanNode: scanNodes) {
          ScanNode scanNode = (ScanNode)eachScanNode;
          tableDescMap.put(scanNode.getFromTable().getTableName(), scanNode.getFromTable().getTableDesc());
        }
      }
      MasterPlan globalPlan = queryMasterContext.getGlobalPlanner().build(queryId, logicalNodeRoot);
      this.masterPlan = queryMasterContext.getGlobalOptimizer().optimize(globalPlan);

      query = new Query(queryContext, queryId, querySubmitTime,
          "", queryContext.getEventHandler(), masterPlan);

      dispatcher.register(QueryEventType.class, query);

      queryContext.getEventHandler().handle(new QueryEvent(queryId,
          QueryEventType.INIT));
      queryContext.getEventHandler().handle(new QueryEvent(queryId,
          QueryEventType.START));
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

    String givenOutputTableName = queryConf.getOutputTable();
    Path stagingDir;

    // If final output directory is not given by an user,
    // we use the query id as a output directory.
    if (givenOutputTableName == null || givenOutputTableName.isEmpty()) {
      this.isCreateTableStmt = false;
      FileSystem defaultFS = FileSystem.get(queryConf);

      Path homeDirectory = defaultFS.getHomeDirectory();
      if (!defaultFS.exists(homeDirectory)) {
        defaultFS.mkdirs(homeDirectory, new FsPermission(USER_DIR_PERMISSION));
      }

      Path userQueryDir = new Path(homeDirectory, TajoConstants.USER_QUERYDIR_PREFIX);

      if (defaultFS.exists(userQueryDir)) {
        FileStatus fsStatus = defaultFS.getFileStatus(userQueryDir);
        String owner = fsStatus.getOwner();

        if (!(owner.equals(currentUser) || owner.equals(realUser))) {
          throw new IOException("The ownership on the user's query " +
              "directory " + userQueryDir + " is not as expected. " +
              "It is owned by " + owner + ". The directory must " +
              "be owned by the submitter " + currentUser + " or " +
              "by " + realUser);
        }

        if (!fsStatus.getPermission().equals(USER_DIR_PERMISSION)) {
          LOG.info("Permissions on staging directory " + userQueryDir + " are " +
              "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
              "to correct value " + USER_DIR_PERMISSION);
          defaultFS.setPermission(userQueryDir, new FsPermission(USER_DIR_PERMISSION));
        }
      } else {
        defaultFS.mkdirs(userQueryDir,
            new FsPermission(USER_DIR_PERMISSION));
      }

      stagingDir = StorageUtil.concatPath(userQueryDir, queryId.toString());

      if (defaultFS.exists(stagingDir)) {
        throw new IOException("The staging directory " + stagingDir
            + "already exists. The directory must be unique to each query");
      } else {
        defaultFS.mkdirs(stagingDir, new FsPermission(USER_DIR_PERMISSION));
      }

      // Set the query id to the output table name
      queryConf.setOutputTable(queryId.toString());

    } else {
      this.isCreateTableStmt = true;
      Path warehouseDir = new Path(queryConf.getVar(TajoConf.ConfVars.ROOT_DIR),
          TajoConstants.WAREHOUSE_DIR);
      stagingDir = new Path(warehouseDir, queryConf.getOutputTable());

      FileSystem fs = warehouseDir.getFileSystem(queryConf);
      if (fs.exists(stagingDir)) {
        throw new IOException("The staging directory " + stagingDir
            + " already exists. The directory must be unique to each query");
      } else {
        // TODO - should have appropriate permission
        fs.mkdirs(stagingDir, new FsPermission(USER_DIR_PERMISSION));
      }
    }

    queryConf.setOutputPath(stagingDir);
    outputPath = stagingDir;
    LOG.info("Initialized Query Staging Dir: " + outputPath);
  }

  public Query getQuery() {
    return query;
  }

  public void expiredSessionTimeout() {
    stop();
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public EventHandler getEventHandler() {
    return queryContext.getEventHandler();
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

  public class QueryContext {
    EventHandler eventHandler;
    public QueryMaster.QueryMasterContext getQueryMasterContext() {
      return queryMasterContext;
    }

    public QueryConf getConf() {
      return queryConf;
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

    public StorageManager getStorageManager() {
      return queryMasterContext.getStorageManager();
    }

    public Path getOutputPath() {
      return outputPath;
    }

    public boolean isCreateTableQuery() {
      return isCreateTableStmt;
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
      return (AbstractResourceAllocator)resourceAllocator;
    }
  }

}