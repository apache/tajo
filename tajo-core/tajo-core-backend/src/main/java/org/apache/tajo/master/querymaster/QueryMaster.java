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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.tajo.*;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.global.GlobalOptimizer;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.ExprType;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.ipc.QueryMasterManagerProtocol;
import org.apache.tajo.ipc.QueryMasterProtocol;
import org.apache.tajo.master.*;
import org.apache.tajo.master.event.*;
import org.apache.tajo.master.rm.RMContainerAllocator;
import org.apache.tajo.rpc.ProtoAsyncRpcServer;
import org.apache.tajo.rpc.ProtoBlockingRpcClient;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TajoIdUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// TODO - when exception, send error status to QueryMasterManager
public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());
  private static PrimitiveProtos.BoolProto TRUE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  private static PrimitiveProtos.BoolProto FALSE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();
  private static int QUERY_SESSION_TIMEOUT = 60 * 1000;  //60 sec

  // AppMaster Common
  private final long appSubmitTime;
  private Clock clock;

  // For Query
  private final QueryId queryId;
  private QueryContext queryContext;
  private Query query;
  private TajoProtos.QueryState state = TajoProtos.QueryState.QUERY_NOT_ASSIGNED;
  private String statusMessage;
  private MasterPlan masterPlan;

  private AsyncDispatcher dispatcher;
  private RMContainerAllocator rmAllocator;

  //service handler for QueryMasterManager, Worker
  private QueryMasterService queryMasterService;
  private QueryMasterClientService queryMasterClientService;

  private TaskRunnerLauncher taskRunnerLauncher;
  private GlobalPlanner globalPlanner;
  private GlobalOptimizer globalOptimizer;

  private boolean isCreateTableStmt;
  private StorageManager storageManager;
  private Path outputPath;
  private QueryConf queryConf;
  private ApplicationAttemptId appAttemptId;
  private ApplicationId appId;
  private ProtoBlockingRpcClient queryMasterManagerClient;
  private QueryMasterManagerProtocol.QueryMasterManagerProtocolService.BlockingInterface queryMasterManagerService;

  private Map<String, TableDesc> tableDescMap = new HashMap<String, TableDesc>();

  private String queryMasterManagerAddress;

  private YarnRPC yarnRPC;

  private YarnClient yarnClient;

  private ClientSessionTimeoutCheckThread clientSessionTimeoutCheckThread;

  public QueryMaster(final QueryId queryId, final long appSubmitTime, String queryMasterManagerAddress) {
    super(QueryMaster.class.getName());

    this.queryId = queryId;
    this.appSubmitTime = appSubmitTime;
    this.appId = queryId.getApplicationId();
    this.queryMasterManagerAddress = queryMasterManagerAddress;

    LOG.info("Created Query Master for " + queryId);
  }

  public void init(Configuration conf) {
    try {
      queryConf = new QueryConf(conf);
      QUERY_SESSION_TIMEOUT = 60 * 1000;//queryConf.getIntVar(TajoConf.ConfVars.QUERY_SESSION_TIMEOUT);
      queryContext = new QueryContext(queryConf);
      yarnRPC = YarnRPC.create(queryContext.getConf());
      connectYarnClient();

      LOG.info("Init QueryMasterManagerClient connection to:" + queryMasterManagerAddress);
      InetSocketAddress addr = NetUtils.createSocketAddr(queryMasterManagerAddress);
      queryMasterManagerClient = new ProtoBlockingRpcClient(QueryMasterManagerProtocol.class, addr);
      queryMasterManagerService = queryMasterManagerClient.getStub();

      clock = new SystemClock();

      this.dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      this.storageManager = new StorageManager(queryConf);

      globalPlanner = new GlobalPlanner(queryConf, storageManager, dispatcher.getEventHandler());
      globalOptimizer = new GlobalOptimizer();

      queryMasterService = new QueryMasterService();
      addIfService(queryMasterService);

      queryMasterClientService = new QueryMasterClientService(queryContext);
      addIfService(queryMasterClientService);

      initStagingDir();

      dispatcher.register(SubQueryEventType.class, new SubQueryEventDispatcher());
      dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
      dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
      dispatcher.register(QueryFinishEvent.EventType.class, new QueryFinishEventHandler());
      dispatcher.register(TaskSchedulerEvent.EventType.class, new TaskSchedulerDispatcher());

      clientSessionTimeoutCheckThread = new ClientSessionTimeoutCheckThread();

      clientSessionTimeoutCheckThread.start();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new RuntimeException(t);
    }
    super.init(conf);
  }

  class ClientSessionTimeoutCheckThread extends Thread {
    public void run() {
      LOG.info("ClientSessionTimeoutCheckThread started");
      while(true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
        try {
          long lastHeartbeat = queryContext.getLastClientHeartbeat();
          long time = System.currentTimeMillis() - lastHeartbeat;
          if(lastHeartbeat > 0 && time > QUERY_SESSION_TIMEOUT) {
            LOG.warn("Query " + queryId + " stopped cause query sesstion timeout: " + time + " ms");
            QueryMaster.this.stop();
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  class QueryHeartbeatThread extends Thread {
    public QueryHeartbeatThread() {
      super("QueryHeartbeatThread");
    }

    @Override
    public void run() {
      LOG.info("Start QueryMaster heartbeat thread");
      while(queryMasterManagerClient.isConnected()) {
        QueryMasterManagerProtocol.QueryHeartbeat queryHeartbeat =
            QueryMasterManagerProtocol.QueryHeartbeat.newBuilder()
                .setQueryMasterHost(queryMasterService.bindAddr.getHostName())
                .setQueryMasterPort(queryMasterService.bindAddr.getPort())
                .setQueryMasterClientPort(queryMasterClientService.getBindAddr().getPort())
                .setState(state)
                .setQueryId(queryId.getProto())
                .build();

        try {
          QueryMasterManagerProtocol.QueryHeartbeatResponse response =
              queryMasterManagerService.queryHeartbeat(null, queryHeartbeat);
          if(response.getResponseCommand() != null) {
            if("executeQuery".equals(response.getResponseCommand().getCommand())) {
              appAttemptId = TajoIdUtils.toApplicationAttemptId(response.getResponseCommand().getParams(0));
              startQuery(response.getResponseCommand().getParams(1),
                  response.getResponseCommand().getParams(2));
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOG.info("QueryMaster heartbeat thread stopped");
    }
  }

  // TODO blocking/nonblocking ???
  class QueryMasterService extends AbstractService implements QueryMasterProtocol.QueryMasterProtocolService.Interface {
    private ProtoAsyncRpcServer rpcServer;
    private InetSocketAddress bindAddr;
    private String addr;
    private QueryHeartbeatThread queryHeartbeatThread;

    public QueryMasterService() {
      super(QueryMasterService.class.getName());

      // Setup RPC server
      try {
        InetSocketAddress initIsa =
                new InetSocketAddress(InetAddress.getLocalHost(), 0);
        if (initIsa.getAddress() == null) {
          throw new IllegalArgumentException("Failed resolve of " + initIsa);
        }

        this.rpcServer = new ProtoAsyncRpcServer(QueryMasterProtocol.class, this, initIsa);
        this.rpcServer.start();

        this.bindAddr = NetUtils.getConnectAddress(rpcServer.getListenAddress());
        this.addr = NetUtils.normalizeInetSocketAddress(this.bindAddr);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
      queryConf.setVar(TajoConf.ConfVars.TASKRUNNER_LISTENER_ADDRESS, addr);
      LOG.info("QueryMasterService startup");
    }

    @Override
    public void init(Configuration conf) {
      super.init(conf);
    }

    @Override
    public void start() {
      try {
        queryHeartbeatThread = new QueryHeartbeatThread();
        queryHeartbeatThread.start();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        // TODO - set query status failed and stop QueryMaster
      }
      super.start();
    }

    @Override
    public void stop() {
      if(rpcServer != null) {
        rpcServer.shutdown();
      }
      if(queryHeartbeatThread != null) {
        queryHeartbeatThread.interrupt();
      }
      if(yarnClient != null) {
        yarnClient.stop();
      }
      if(clientSessionTimeoutCheckThread != null) {
        clientSessionTimeoutCheckThread.interrupt();
      }
      super.stop();
      LOG.info("QueryMasterService stopped");
    }

    @Override
    public void getTask(RpcController controller, YarnProtos.ContainerIdProto request,
                        RpcCallback<QueryMasterProtocol.QueryUnitRequestProto> done) {
      queryContext.getEventHandler().handle(new TaskRequestEvent(new ContainerIdPBImpl(request), done));
    }

    @Override
    public void statusUpdate(RpcController controller, QueryMasterProtocol.TaskStatusProto request,
                             RpcCallback<PrimitiveProtos.BoolProto> done) {
      QueryUnitAttemptId attemptId = new QueryUnitAttemptId(request.getId());
      queryContext.getEventHandler().handle(new TaskAttemptStatusUpdateEvent(attemptId, request));
      done.run(TRUE_PROTO);
    }

    @Override
    public void ping(RpcController controller,
                     TajoIdProtos.QueryUnitAttemptIdProto attemptIdProto,
                     RpcCallback<PrimitiveProtos.BoolProto> done) {
      // TODO - to be completed
      QueryUnitAttemptId attemptId = new QueryUnitAttemptId(attemptIdProto);
      done.run(TRUE_PROTO);
    }

    @Override
    public void fatalError(RpcController controller, QueryMasterProtocol.TaskFatalErrorReport report,
                           RpcCallback<PrimitiveProtos.BoolProto> done) {
      queryContext.getEventHandler().handle(new TaskFatalErrorEvent(report));
      done.run(TRUE_PROTO);
    }

    @Override
    public void done(RpcController controller, QueryMasterProtocol.TaskCompletionReport report,
                     RpcCallback<PrimitiveProtos.BoolProto> done) {
      queryContext.getEventHandler().handle(new TaskCompletionEvent(report));
      done.run(TRUE_PROTO);
    }

    @Override
    public void executeQuery(RpcController controller, PrimitiveProtos.StringProto request,
                             RpcCallback<PrimitiveProtos.BoolProto> done) {
    }
  }

  public void start() {
    super.start();
  }

  public void stop() {
    LOG.info("unregisterApplicationMaster");
    if(rmAllocator != null) {
      try {
        FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;
        if (query != null) {
          TajoProtos.QueryState state = query.getState();
          if (state == TajoProtos.QueryState.QUERY_SUCCEEDED) {
            status = FinalApplicationStatus.SUCCEEDED;
          } else if (state == TajoProtos.QueryState.QUERY_FAILED || state == TajoProtos.QueryState.QUERY_ERROR) {
            status = FinalApplicationStatus.FAILED;
          } else if (state == TajoProtos.QueryState.QUERY_ERROR) {
            status = FinalApplicationStatus.FAILED;
          }
        }
        this.rmAllocator.unregisterApplicationMaster(status, "tajo query finished", null);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    // TODO - release opened resource
    if(this.queryMasterManagerClient != null) {
      reportQueryStatus();

      queryMasterManagerClient.close();
    }

    try {
      FileSystem.closeAll();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    super.stop();

    synchronized(queryId) {
      queryId.notifyAll();
    }
  }

  private void reportQueryStatus() {
    //send query status heartbeat
    QueryMasterManagerProtocol.QueryHeartbeat.Builder queryHeartbeatBuilder =
        QueryMasterManagerProtocol.QueryHeartbeat.newBuilder()
        .setQueryMasterHost(queryMasterService.bindAddr.getHostName())
        .setQueryMasterPort(queryMasterService.bindAddr.getPort())
        .setQueryMasterClientPort(queryMasterClientService.getBindAddr().getPort())
        .setState(state)
        .setQueryId(queryId.getProto());

    if(statusMessage != null) {
      queryHeartbeatBuilder.setStatusMessage(statusMessage);
    }
    try {
      queryMasterManagerService.queryHeartbeat(null, queryHeartbeatBuilder.build());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void connectYarnClient() {
    this.yarnClient = new YarnClientImpl();
    this.yarnClient.init(queryConf);
    this.yarnClient.start();
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  public synchronized void startQuery(String queryStr, String planJSON) {
    LOG.info("Query Start:" + queryStr);
    LOG.info("Plan JSON:" + planJSON);
    if(query != null) {
      LOG.warn("Query already started");
      return;
    }

    try {
      LogicalRootNode logicalNodeRoot = (LogicalRootNode) CoreGsonHelper.fromJson(planJSON, LogicalNode.class);
      LogicalNode[] scanNodes = PlannerUtil.findAllNodes(logicalNodeRoot, ExprType.SCAN);
      if(scanNodes != null) {
        for(LogicalNode eachScanNode: scanNodes) {
          ScanNode scanNode = (ScanNode)eachScanNode;
          tableDescMap.put(scanNode.getFromTable().getTableName(), scanNode.getFromTable().getTableDesc());
        }
      }
      MasterPlan globalPlan = globalPlanner.build(queryId, logicalNodeRoot);
      this.masterPlan = globalOptimizer.optimize(globalPlan);

      taskRunnerLauncher = new TaskRunnerLauncherImpl(queryContext);
      addIfService(taskRunnerLauncher);
      dispatcher.register(TaskRunnerGroupEvent.EventType.class, taskRunnerLauncher);

      ((TaskRunnerLauncherImpl)taskRunnerLauncher).init(queryConf);
      ((TaskRunnerLauncherImpl)taskRunnerLauncher).start();

      rmAllocator = new RMContainerAllocator(queryContext);
      addIfService(rmAllocator);
      dispatcher.register(ContainerAllocatorEventType.class, rmAllocator);

      rmAllocator.init(queryConf);
      rmAllocator.start();

      //TODO - synchronized with executeQuery logic
      query = new Query(queryContext, queryId, clock, appSubmitTime,
              "", dispatcher.getEventHandler(), masterPlan, storageManager);
      dispatcher.register(QueryEventType.class, query);

      dispatcher.getEventHandler().handle(new QueryEvent(queryId,
          QueryEventType.INIT));
      dispatcher.getEventHandler().handle(new QueryEvent(queryId,
          QueryEventType.START));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      //send FAIL query status
      this.statusMessage = StringUtils.stringifyException(e);
      this.state = TajoProtos.QueryState.QUERY_FAILED;
    }
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
      subQuery.getTaskScheduler().handle(event);
    }
  }

  public QueryContext getContext() {
    return this.queryContext;
  }

  public class QueryContext {
    private QueryConf conf;
    public Map<ContainerId, ContainerProxy> containers = new ConcurrentHashMap<ContainerId, ContainerProxy>();
    int minCapability;
    int maxCapability;
    int numCluster;
    AtomicLong lastClientHeartbeat = new AtomicLong(-1);

    public QueryContext(QueryConf conf) {
      this.conf = conf;
    }

    public QueryConf getConf() {
      return conf;
    }

    public InetSocketAddress getQueryMasterServiceAddress() {
      return queryMasterService.bindAddr;
    }

    public QueryMasterClientService getQueryMasterClientService() {
      return queryMasterClientService;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Clock getClock() {
      return clock;
    }

    public Query getQuery() {
      return query;
    }

    public SubQuery getSubQuery(SubQueryId subQueryId) {
      return query.getSubQuery(subQueryId);
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public ApplicationId getApplicationId() {
      return appId;
    }

    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptId;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public void addContainer(ContainerId cId, ContainerProxy container) {
      containers.put(cId, container);
    }

    public void removeContainer(ContainerId cId) {
      containers.remove(cId);
    }

    public boolean containsContainer(ContainerId cId) {
      return containers.containsKey(cId);
    }

    public ContainerProxy getContainer(ContainerId cId) {
      return containers.get(cId);
    }

    public Map<ContainerId, ContainerProxy> getContainers() {
      return containers;
    }

    public int getNumClusterNode() {
      return numCluster;
    }

    public void setNumClusterNodes(int num) {
      numCluster = num;
    }

//    public CatalogService getCatalog() {
//      return catalog;
//    }

    public Map<String, TableDesc> getTableDescMap() {
      return tableDescMap;
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

    public float getProgress() {
      if(query != null) {
        return query.getProgress();
      } else {
        return 0;
      }
    }

    public long getStartTime() {
      if(query != null) {
        return query.getStartTime();
      } else {
        return -1;
      }
    }

    public long getFinishTime() {
      if(query != null) {
        return query.getFinishTime();
      } else {
        return -1;
      }
    }

    public StorageManager getStorageManager() {
      return storageManager;
    }

    public QueryMaster getQueryMaster() {
      return QueryMaster.this;
    }

    public YarnRPC getYarnRPC() {
      return yarnRPC;
    }

    public void setState(TajoProtos.QueryState state) {
      QueryMaster.this.state = state;
    }

    public TajoProtos.QueryState getState() {
      return state;
    }

    public void touchSessionTime() {
      this.lastClientHeartbeat.set(System.currentTimeMillis());
    }

    public long getLastClientHeartbeat() {
      return this.lastClientHeartbeat.get();
    }
  }

  private class QueryFinishEventHandler implements EventHandler<QueryFinishEvent> {
    @Override
    public void handle(QueryFinishEvent event) {
      LOG.info("Query end notification started for QueryId : " + query.getId() + "," + query.getState());

      //QueryMaster must be lived until client fetching all query result data.
      try {
        // Stop all services
        // This will also send the final report to the ResourceManager
        //LOG.info("Calling stop for all the services");
//        stop();
      } catch (Throwable t) {
        LOG.warn("Graceful stop failed ", t);
      }

      //Bring the process down by force.
      //Not needed after HADOOP-7140
      //LOG.info("Exiting QueryMaster..GoodBye!");
    }
  }

  // query submission directory is private!
  final public static FsPermission USER_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx--------

  /**
   * It initializes the final output and staging directory and sets
   * them to variables.
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
      conf.setOutputTable(queryId.toString());

    } else {
      this.isCreateTableStmt = true;
      Path warehouseDir = new Path(conf.getVar(TajoConf.ConfVars.ROOT_DIR),
          TajoConstants.WAREHOUSE_DIR);
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
