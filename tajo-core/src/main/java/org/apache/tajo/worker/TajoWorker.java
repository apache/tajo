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

package org.apache.tajo.worker;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogClient;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.function.hiveudf.HiveFunctionLoader;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.metrics.Node;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.pullserver.PullServerUtil;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.querymaster.QueryMaster;
import org.apache.tajo.querymaster.QueryMasterManagerService;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationFailedException;
import org.apache.tajo.rule.SelfDiagnosisRuleEngine;
import org.apache.tajo.rule.SelfDiagnosisRuleSession;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.OldStorageManager;
import org.apache.tajo.util.*;
import org.apache.tajo.util.history.HistoryReader;
import org.apache.tajo.util.history.HistoryWriter;
import org.apache.tajo.util.metrics.TajoSystemMetrics;
import org.apache.tajo.webapp.StaticHttpServer;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class TajoWorker extends CompositeService {
  public static final PrimitiveProtos.BoolProto TRUE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  public static final PrimitiveProtos.BoolProto FALSE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();
  public static final PrimitiveProtos.NullProto NULL_PROTO = PrimitiveProtos.NullProto.newBuilder().build();
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(TajoWorker.class);

  private TajoConf systemConf;
  private StaticHttpServer webServer;
  private QueryMasterManagerService queryMasterManagerService;
  private CatalogClient catalogClient;
  private WorkerContext workerContext;
  private TaskManager taskManager;
  private TaskExecutor taskExecutor;
  private TajoPullServerService pullService;
  private ServiceTracker serviceTracker;
  private NodeResourceManager nodeResourceManager;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private WorkerConnectionInfo connectionInfo;
  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private String[] cmdArgs;
  private DeletionService deletionService;
  private TajoSystemMetrics workerSystemMetrics;
  private HashShuffleAppenderManager hashShuffleAppenderManager;
  private LocalDirAllocator lDirAllocator;
  private JvmPauseMonitor pauseMonitor;

  private HistoryWriter taskHistoryWriter;
  private HistoryReader historyReader;

  public TajoWorker() throws Exception {
    super(TajoWorker.class.getName());
  }

  public void startWorker(TajoConf systemConf, String[] args) {
    this.systemConf = systemConf;
    this.cmdArgs = args;
    init(systemConf);
    start();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    AsyncDispatcher dispatcher;
    TajoWorkerClientService tajoWorkerClientService;
    TajoWorkerManagerService tajoWorkerManagerService;

    ShutdownHookManager.get().addShutdownHook(new ShutdownHook(), SHUTDOWN_HOOK_PRIORITY);

    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    RackResolver.init(systemConf);

    serviceTracker = ServiceTrackerFactory.get(systemConf);

    this.workerContext = new TajoWorkerContext();
    this.lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

    dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    tajoWorkerManagerService = new TajoWorkerManagerService(workerContext);
    addIfService(tajoWorkerManagerService);

    // querymaster worker
    tajoWorkerClientService = new TajoWorkerClientService(workerContext);
    addIfService(tajoWorkerClientService);

    queryMasterManagerService = new QueryMasterManagerService(workerContext);
    addIfService(queryMasterManagerService);

    if (!PullServerUtil.useExternalPullServerService(systemConf)) {
      pullService = new TajoPullServerService();
      addIfService(pullService);
    }

    this.taskManager = new TaskManager(dispatcher, workerContext, pullService);
    addService(taskManager);

    this.taskExecutor = new TaskExecutor(workerContext);
    addService(taskExecutor);

    AsyncDispatcher rmDispatcher = new AsyncDispatcher();
    addService(rmDispatcher);
    this.nodeResourceManager = new NodeResourceManager(rmDispatcher, workerContext);
    addService(nodeResourceManager);

    addService(new NodeStatusUpdater(workerContext));

    int httpPort = 0;
    if (!systemConf.getBoolVar(ConfVars.$TEST_MODE)) {
      httpPort = initWebServer();
    }

    super.serviceInit(conf);

    int pullServerPort = systemConf.getIntVar(ConfVars.PULLSERVER_PORT);
    if(pullService != null){
      pullServerPort = pullService.getPort();
    } else if (TajoPullServerService.isStandalone()) {
      pullServerPort = getStandAlonePullServerPort();
    }

    this.connectionInfo = new WorkerConnectionInfo(
        tajoWorkerManagerService.getBindAddr().getHostName(),
        tajoWorkerManagerService.getBindAddr().getPort(),
        pullServerPort,
        tajoWorkerClientService.getBindAddr().getPort(),
        queryMasterManagerService.getBindAddr().getPort(),
        httpPort);

    LOG.info("Tajo Worker is initialized." + " connection :" + connectionInfo.toString());

    try {
      hashShuffleAppenderManager = new HashShuffleAppenderManager(systemConf);
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
      System.exit(-1);
    }

    taskHistoryWriter = new HistoryWriter(workerContext.getWorkerName(), false);
    addIfService(taskHistoryWriter);
    taskHistoryWriter.init(conf);

    historyReader = new HistoryReader(workerContext.getWorkerName(), this.systemConf);

    FunctionLoader.loadUserDefinedFunctions(systemConf);
    HiveFunctionLoader.loadHiveUDFs(systemConf);

    PythonScriptEngine.initPythonScriptEngineFiles();
    
    diagnoseTajoWorker();
  }

  private void initWorkerMetrics() {
    workerSystemMetrics = new TajoSystemMetrics(systemConf, Node.class, workerContext.getWorkerName());
    workerSystemMetrics.start();

    workerSystemMetrics.register(Node.QueryMaster.RUNNING_QM,
        () -> queryMasterManagerService != null ?
            queryMasterManagerService.getQueryMaster().getQueryMasterTasks().size() : 0);

    workerSystemMetrics.register(Node.Tasks.RUNNING_TASKS,
        () -> taskExecutor != null ? taskExecutor.getRunningTasks() : 0);
  }

  private int initWebServer() {
    InetSocketAddress address = systemConf.getSocketAddrVar(ConfVars.WORKER_INFO_ADDRESS);
    try {
      webServer = StaticHttpServer.getInstance(this, "worker", address.getHostName(), address.getPort(),
          true, null, systemConf, null);
      webServer.start();

      systemConf.setVar(TajoConf.ConfVars.WORKER_INFO_ADDRESS, NetUtils.getHostPortString(
          NetUtils.getConnectAddress(new InetSocketAddress(address.getHostName(), webServer.getPort()))));

      LOG.info("Worker info server started:" + webServer.getPort());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
    return webServer.getPort();
  }

  private void initCleanupService() throws IOException {
    String[] localDirs = systemConf.getVar(ConfVars.WORKER_TEMPORAL_DIR).trim().split("\\s*,\\s*");
    deletionService = new DeletionService(localDirs.length, 0);
    if (systemConf.getBoolVar(ConfVars.WORKER_TEMPORAL_DIR_CLEANUP)) {
      getWorkerContext().cleanupTemporalDirectories();
    }
  }
  
  private void diagnoseTajoWorker() throws EvaluationFailedException {
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    EvaluationContext context = new EvaluationContext();
    
    context.addParameter(TajoConf.class.getName(), systemConf);
    
    ruleSession.withCategoryNames("base", "worker").fireRules(context);
  }

  private void startJvmPauseMonitor(){
    pauseMonitor = new JvmPauseMonitor(systemConf);
    pauseMonitor.start();
  }

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public void serviceStart() throws Exception {
    startJvmPauseMonitor();
    connectToCatalog();

    if (!systemConf.getBoolVar(ConfVars.$TEST_MODE)) {
      initCleanupService();
    }

    initWorkerMetrics();
    super.serviceStart();
    LOG.info("Tajo Worker is started");
  }

  @Override
  public void serviceStop() throws Exception {
    if(stopped.getAndSet(true)) {
      return;
    }

    if (hashShuffleAppenderManager != null) {
      hashShuffleAppenderManager.shutdown();
    }

    if(webServer != null) {
      try {
        webServer.stop();
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
      }
    }

    if (catalogClient != null) {
      catalogClient.close();
    }

    if(webServer != null && webServer.isAlive()) {
      try {
        webServer.stop();
      } catch (Throwable e) {
      }
    }

    try {
      OldStorageManager.shutdown();
    } catch (IOException ie) {
      LOG.error(ie.getMessage(), ie);
    }

    if(workerSystemMetrics != null) {
      workerSystemMetrics.stop();
    }

    if(deletionService != null) deletionService.stop();

    if(pauseMonitor != null) pauseMonitor.stop();
    super.serviceStop();
    LOG.info("TajoWorker main thread exiting");
  }

  public interface WorkerContext {
    QueryMaster getQueryMaster();

    TajoConf getConf();

    ServiceTracker getServiceTracker();

    QueryMasterManagerService getQueryMasterManagerService();

    TaskManager getTaskManager();

    TaskExecutor getTaskExecuor();

    NodeResourceManager getNodeResourceManager();

    CatalogService getCatalog();

    WorkerConnectionInfo getConnectionInfo();

    String getWorkerName();

    LocalDirAllocator getLocalDirAllocator();

    TajoSystemMetrics getMetrics();

    HashShuffleAppenderManager getHashShuffleAppenderManager();

    HistoryWriter getTaskHistoryWriter();

    HistoryReader getHistoryReader();

    void cleanup(String strPath);

    void cleanupTemporalDirectories();
  }

  class TajoWorkerContext implements WorkerContext {
    public QueryMaster getQueryMaster() {
      if (queryMasterManagerService == null) {
        return null;
      }
      return queryMasterManagerService.getQueryMaster();
    }

    public TajoConf getConf() {
      return systemConf;
    }

    public ServiceTracker getServiceTracker() {
      return serviceTracker;
    }

    public QueryMasterManagerService getQueryMasterManagerService() {
      return queryMasterManagerService;
    }

    @Override
    public TaskManager getTaskManager(){
      return taskManager;
    }

    @Override
    public TaskExecutor getTaskExecuor() {
      return taskExecutor;
    }

    @Override
    public NodeResourceManager getNodeResourceManager() {
      return nodeResourceManager;
    }

    public CatalogService getCatalog() {
      return catalogClient;
    }

    public WorkerConnectionInfo getConnectionInfo() {
      return connectionInfo;
    }

    public String getWorkerName() {
      return connectionInfo.getHostAndPeerRpcPort();
    }

    public LocalDirAllocator getLocalDirAllocator(){
      return lDirAllocator;
    }

    public void cleanup(String strPath) {
      if (deletionService == null) return;

      LocalDirAllocator lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

      try {
        Iterable<Path> iter = lDirAllocator.getAllLocalPathsToRead(strPath, systemConf);
        FileSystem localFS = FileSystem.getLocal(systemConf);
        for (Path path : iter) {
          deletionService.delete(localFS.makeQualified(path));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    public void cleanupTemporalDirectories() {
      if (deletionService == null) return;

      LocalDirAllocator lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

      try {
        Iterable<Path> iter = lDirAllocator.getAllLocalPathsToRead(".", systemConf);
        FileSystem localFS = FileSystem.getLocal(systemConf);
        for (Path path : iter) {
          PathData[] items = PathData.expandAsGlob(localFS.makeQualified(new Path(path, "*")).toString(), systemConf);

          ArrayList<Path> paths = new ArrayList<>();
          for (PathData pd : items) {
            paths.add(pd.path);
          }
          if (paths.size() == 0) continue;

          deletionService.delete(null, paths.toArray(new Path[paths.size()]));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    public TajoSystemMetrics getMetrics() {
      return workerSystemMetrics;
    }

    public HashShuffleAppenderManager getHashShuffleAppenderManager() {
      return hashShuffleAppenderManager;
    }

    public HistoryWriter getTaskHistoryWriter() {
      return taskHistoryWriter;
    }

    public HistoryReader getHistoryReader() {
      return historyReader;
    }
  }

  private int getStandAlonePullServerPort() {
    long startTime = System.currentTimeMillis();
    int pullServerPort;

    //wait for pull server bring up
    while (true) {
      pullServerPort = TajoPullServerService.readPullServerPort();
      if (pullServerPort > 0) {
        break;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
      if (System.currentTimeMillis() - startTime > 30 * 1000) {
        LOG.fatal("TajoWorker stopped cause can't get PullServer port.");
        System.exit(-1);
      }
    }
    return pullServerPort;
  }

  @VisibleForTesting
  public void stopWorkerForce() {
    stop();
  }

  private void connectToCatalog() {
    try {
      catalogClient = new CatalogClient(systemConf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      if(!stopped.get()) {
        LOG.info("============================================");
        LOG.info("TajoWorker received SIGINT Signal");
        LOG.info("============================================");
        stop();
        RpcClientManager.shutdown();
      }
    }
  }

  String getThreadTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public void dumpThread(Writer writer) {
    PrintWriter stream = new PrintWriter(writer);
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: Tajo Worker");
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + getThreadTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state + ",  Blocked count: " + info.getBlockedCount() +
          ",  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime() + ",  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName() +
            ", Blocked by " + getThreadTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
      stream.println("");
    }
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new TajoUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(TajoWorker.class, args, LOG);

    TajoConf tajoConf = new TajoConf();
    tajoConf.addResource(new Path(TajoConstants.SYSTEM_CONF_FILENAME));

    try {
      TajoWorker tajoWorker = new TajoWorker();
      tajoWorker.startWorker(tajoConf, args);
    } catch (Throwable t) {
      LOG.fatal("Error starting TajoWorker", t);
      System.exit(-1);
    }
  }
}
