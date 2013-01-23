/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import tajo.*;
import tajo.catalog.CatalogService;
import tajo.conf.TajoConf;
import tajo.engine.planner.global.MasterPlan;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.TaskRunnerLauncherImpl.Container;
import tajo.master.event.*;
import tajo.master.rm.RMContainerAllocator;
import tajo.util.TajoIdUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());

  // Master Context
  private final MasterContext masterContext;

  // AppMaster Common
  private final Clock clock;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;

  // For Query
  private final QueryId queryId;
  private QueryContext queryContext;
  private Query query;
  private MasterPlan masterPlan;

  private AsyncDispatcher dispatcher;
  private YarnRPC rpc;
  private RMContainerAllocator rmAllocator;
  private TaskRunnerListener taskRunnerListener;
  private TaskRunnerLauncher taskRunnerLauncher;

  // Services of Tajo
  private CatalogService catalog;

  private boolean isCreateTableStmt;
  private Path outputPath;

  public QueryMaster(final MasterContext masterContext,
                     final ApplicationAttemptId appAttemptID,
                     final Clock clock, long appSubmitTime,
                     MasterPlan masterPlan) {
    super(QueryMaster.class.getName());
    this.masterContext = masterContext;

    this.appAttemptID = appAttemptID;
    this.clock = clock;
    this.appSubmitTime = appSubmitTime;

    this.queryId = TajoIdUtils.createQueryId(appAttemptID);
    this.masterPlan = masterPlan;
    LOG.info("Created Query Master for " + appAttemptID);
  }

  public void init(Configuration _conf) {
    QueryConf conf = new QueryConf(_conf);

    try {

      queryContext = new QueryContext(conf);

      dispatcher = masterContext.getDispatcher();
      // TODO - This comment should be eliminated when QueryMaster is separated.
      dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      // TODO - This comment should be improved when QueryMaster is separated.
      rpc = masterContext.getYarnRPC();

      catalog = masterContext.getCatalog();

      taskRunnerListener = new TaskRunnerListener(queryContext);
      addIfService(taskRunnerListener);

      rmAllocator = new RMContainerAllocator(queryContext);
      addIfService(rmAllocator);
      dispatcher.register(ContainerAllocatorEventType.class, rmAllocator);

      query = new Query(queryContext, queryId, clock, appSubmitTime,
          "", dispatcher.getEventHandler(), null, masterPlan,
          masterContext.getStorageManager());
      initStagingDir();

      // QueryEventDispatcher is already registered in TajoMaster
      dispatcher.register(QueryEventType.class, query);
      dispatcher.register(SubQueryEventType.class, new SubQueryEventDispatcher());
      dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
      dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
      dispatcher.register(QueryFinishEvent.EventType.class, new QueryFinishEventHandler());
      dispatcher.register(TaskSchedulerEvent.EventType.class, new TaskSchedulerDispatcher());

      taskRunnerLauncher = new TaskRunnerLauncherImpl(queryContext);
      addIfService(taskRunnerLauncher);
      dispatcher.register(TaskRunnerEvent.EventType.class, taskRunnerLauncher);


    } catch (Throwable t) {
      LOG.error(ExceptionUtils.getStackTrace(t));
      throw new RuntimeException(t);
    }

    super.init(conf);
  }

  public void start() {
    super.start();
    startQuery();
  }

  public void stop() {
    super.stop();
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  public void startQuery() {
    dispatcher.getEventHandler().handle(new QueryEvent(queryId,
        QueryEventType.INIT));
    dispatcher.getEventHandler().handle(new QueryEvent(queryId,
        QueryEventType.START));
  }

  @Override
  public void handle(Event event) {
    dispatcher.getEventHandler().handle(event);
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  private class SubQueryEventDispatcher implements EventHandler<SubQueryEvent> {
    public void handle(SubQueryEvent event) {
      SubQueryId id = event.getSubQueryId();
      query.getSubQuery(id).handle(event);
    }
  }

  private class TaskEventDispatcher
      implements EventHandler<TaskEvent> {
    public void handle(TaskEvent event) {
      QueryUnitId taskId = event.getTaskId();
      QueryUnit task = query.getSubQuery(taskId.getSubQueryId()).
          getQueryUnit(taskId);
      task.handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
      implements EventHandler<TaskAttemptEvent> {
    public void handle(TaskAttemptEvent event) {
      QueryUnitAttemptId attemptId = event.getTaskAttemptId();
      SubQuery subQuery = query.getSubQuery(attemptId.getSubQueryId());
      QueryUnit task = subQuery.getQueryUnit(attemptId.getQueryUnitId());
      QueryUnitAttempt attempt = task.getAttempt(attemptId);
      attempt.handle(event);
    }
  }

  private class TaskSchedulerDispatcher
      implements EventHandler<TaskSchedulerEvent> {
    public void handle(TaskSchedulerEvent event) {
      SubQuery subQuery = query.getSubQuery(event.getSubQueryId());
      subQuery.taskScheduler.handle(event);
    }
  }

  public QueryContext getContext() {
    return this.queryContext;
  }

  public class QueryContext {
    private QueryConf conf;
    int clusterNode;
    public Map<ContainerId, Container> containers = new ConcurrentHashMap<>();
    int minCapability;
    int maxCapability;

    public QueryContext(QueryConf conf) {
      this.conf = conf;
    }

    public QueryConf getConf() {
      return conf;
    };

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Query getQuery(){
      return query;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public ApplicationId getApplicationId() {
      return appAttemptID.getApplicationId();
    }

    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public YarnRPC getYarnRPC() {
      return rpc;
    }

    public InetSocketAddress getRpcAddress() {
      return masterContext.getClientService().getBindAddress();
    }

    public InetSocketAddress getTaskListener() {
      return taskRunnerListener.getBindAddress();
    }

    public void addContainer(ContainerId cId, Container container) {
      containers.put(cId, container);
    }

    public void removeContainer(ContainerId cId) {
      containers.remove(cId);
    }

    public boolean containsContainer(ContainerId cId) {
      return containers.containsKey(cId);
    }

    public Container getContainer(ContainerId cId) {
      return containers.get(cId);
    }

    public int getNumClusterNode() {
      return clusterNode;
    }

    public void setNumClusterNode(int num) {
      clusterNode = num;
    }

    public CatalogService getCatalog() {
      return catalog;
    }

    public Path getOutputPath() {
      return outputPath;
    }

    public void setMaxContainerCapability(int capability) {
      this.maxCapability = capability;
    }

    public int getMaxContainerCapability() {
      return this.maxCapability;
    }

    public void setMinContainerCapability(int capability) {
      this.minCapability = capability;
    }

    public int getMinContainerCapability() {
      return this.minCapability;
    }

    public boolean isCreateTableQuery() {
      return isCreateTableStmt;
    }
  }

  private class QueryFinishEventHandler implements EventHandler<QueryFinishEvent> {
    @Override
    public void handle(QueryFinishEvent event) {
      LOG.info("Query end notification started for QueryId : " + query.getId());

      try {
        // Stop all services
        // This will also send the final report to the ResourceManager
        LOG.info("Calling stop for all the services");
        stop();

      } catch (Throwable t) {
        LOG.warn("Graceful stop failed ", t);
      }

      //Bring the process down by force.
      //Not needed after HADOOP-7140
      LOG.info("Exiting QueryMaster..GoodBye!");
      // TODO - to be enabled if query master is separated.
      //System.exit(0);
    }
  }

  // query submission directory is private!
  final public static FsPermission USER_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------
  final public static FsPermission QUERYCONF_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  /**
   * It initializes the final output and staging directory and sets
   * them to variables.
   *
   * @return
   * @throws java.io.IOException
   */
  private void initStagingDir() throws IOException {
    QueryConf conf = getContext().getConf();

    String realUser;
    String currentUser;
    UserGroupInformation ugi;
    ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();

    String givenOutputTableName = conf.getOutputTable();
    Path stagingDir;

    // If final output directory is not given by an user,
    // we use the query id as a output directory.
    if (givenOutputTableName.equals("")) {
      this.isCreateTableStmt = false;
      FileSystem defaultFS = FileSystem.get(conf);
      Path queryBaseDir = defaultFS.makeQualified(
          new Path(conf.getVar(TajoConf.ConfVars.QUERY_TMP_DIR)));
      Path userQueryDir = defaultFS.makeQualified(
          new Path(queryBaseDir, currentUser));

      // If an user directory is not created yet
      FileSystem fs = userQueryDir.getFileSystem(conf);

      if (fs.exists(userQueryDir)) {
        FileStatus fsStatus = fs.getFileStatus(userQueryDir);
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
          fs.setPermission(userQueryDir, USER_DIR_PERMISSION);
        }
      } else {
        fs.mkdirs(userQueryDir,
            new FsPermission(USER_DIR_PERMISSION));
      }

      stagingDir = new Path(userQueryDir, queryId.toString());
      if (fs.exists(stagingDir)) {
        throw new IOException("The staging directory " + stagingDir
            + "already exists. The directory must be unique to each query");
      } else {
        fs.mkdirs(stagingDir, new FsPermission(USER_DIR_PERMISSION));
      }

      // Set the query id to the output table name
      conf.setOutputTable(queryId.toString());

    } else {
      this.isCreateTableStmt = true;
      Path warehouseDir = new Path(conf.getVar(TajoConf.ConfVars.WAREHOUSE_PATH));
      stagingDir = new Path(warehouseDir, conf.getOutputTable());

      FileSystem fs = warehouseDir.getFileSystem(conf);
      if (fs.exists(stagingDir)) {
        throw new IOException("The staging directory " + stagingDir
            + " already exists. The directory must be unique to each query");
      } else {
        // TODO - should have appropriate permission
        fs.mkdirs(stagingDir, new FsPermission(USER_DIR_PERMISSION));
      }
    }

    conf.setOutputPath(stagingDir);
    outputPath = stagingDir;
    LOG.info("Initialized Query Staging Dir: " + outputPath);
  }
}
