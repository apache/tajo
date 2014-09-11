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

import com.codahale.metrics.Gauge;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogClient;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.ha.TajoMasterInfo;
import org.apache.tajo.master.querymaster.QueryMaster;
import org.apache.tajo.master.querymaster.QueryMasterManagerService;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.*;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.util.metrics.TajoSystemMetrics;
import org.apache.tajo.webapp.StaticHttpServer;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class TajoWorker extends CompositeService {
  public static final PrimitiveProtos.BoolProto TRUE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  public static final PrimitiveProtos.BoolProto FALSE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();

  public static final String WORKER_MODE_YARN_TASKRUNNER = "tr";
  public static final String WORKER_MODE_YARN_QUERYMASTER = "qm";
  public static final String WORKER_MODE_STANDBY = "standby";
  public static final String WORKER_MODE_QUERY_MASTER = "standby-qm";
  public static final String WORKER_MODE_TASKRUNNER = "standby-tr";

  private static final Log LOG = LogFactory.getLog(TajoWorker.class);

  private TajoConf systemConf;

  private StaticHttpServer webServer;

  private TajoWorkerClientService tajoWorkerClientService;

  private QueryMasterManagerService queryMasterManagerService;

  private TajoWorkerManagerService tajoWorkerManagerService;

  private TajoMasterInfo tajoMasterInfo;

  private CatalogClient catalogClient;

  private WorkerContext workerContext;

  private TaskRunnerManager taskRunnerManager;

  private TajoPullServerService pullService;

  private int pullServerPort;

  private boolean yarnContainerMode;

  private boolean queryMasterMode;

  private boolean taskRunnerMode;

  private WorkerHeartbeatService workerHeartbeatThread;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private AtomicInteger numClusterNodes = new AtomicInteger();

  private TajoMasterProtocol.ClusterResourceSummary clusterResource;

  private int httpPort;

  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  private RpcConnectionPool connPool;

  private String[] cmdArgs;

  private DeletionService deletionService;

  private TajoSystemMetrics workerSystemMetrics;

  private HashShuffleAppenderManager hashShuffleAppenderManager;

  public TajoWorker() throws Exception {
    super(TajoWorker.class.getName());
  }

  public void startWorker(TajoConf systemConf, String[] args) {
    this.systemConf = systemConf;
    this.cmdArgs = args;
    setWorkerMode(args);
    init(systemConf);
    start();
  }

  private void setWorkerMode(String[] args) {
    if(args.length < 1) {
      queryMasterMode = systemConf.getBoolean("tajo.worker.mode.querymaster", true);
      taskRunnerMode = systemConf.getBoolean("tajo.worker.mode.taskrunner", true);
    } else {
      if(WORKER_MODE_STANDBY.equals(args[0])) {
        queryMasterMode = true;
        taskRunnerMode = true;
      } else if(WORKER_MODE_YARN_TASKRUNNER.equals(args[0])) {
        yarnContainerMode = true;
        queryMasterMode = true;
      } else if(WORKER_MODE_YARN_QUERYMASTER.equals(args[0])) {
        yarnContainerMode = true;
        taskRunnerMode = true;
      } else if(WORKER_MODE_QUERY_MASTER.equals(args[0])) {
        yarnContainerMode = false;
        queryMasterMode = true;
      } else {
        yarnContainerMode = false;
        taskRunnerMode = true;
      }
    }
    if(!queryMasterMode && !taskRunnerMode) {
      LOG.fatal("Worker daemon exit cause no worker mode(querymaster/taskrunner) property");
      System.exit(0);
    }
  }
  
  @Override
  public void init(Configuration conf) {
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

    this.systemConf = (TajoConf)conf;
    RackResolver.init(systemConf);

    this.connPool = RpcConnectionPool.getPool(systemConf);
    this.workerContext = new WorkerContext();

    String resourceManagerClassName = systemConf.getVar(ConfVars.RESOURCE_MANAGER_CLASS);

    boolean randomPort = true;
    if(resourceManagerClassName.indexOf(TajoWorkerResourceManager.class.getName()) >= 0) {
      randomPort = false;
    }
    int clientPort = systemConf.getSocketAddrVar(ConfVars.WORKER_CLIENT_RPC_ADDRESS).getPort();
    int peerRpcPort = systemConf.getSocketAddrVar(ConfVars.WORKER_PEER_RPC_ADDRESS).getPort();
    int qmManagerPort = systemConf.getSocketAddrVar(ConfVars.WORKER_QM_RPC_ADDRESS).getPort();

    if(randomPort) {
      clientPort = 0;
      peerRpcPort = 0;
      qmManagerPort = 0;
      systemConf.setIntVar(ConfVars.PULLSERVER_PORT, 0);
    }

    // querymaster worker
    tajoWorkerClientService = new TajoWorkerClientService(workerContext, clientPort);
    addService(tajoWorkerClientService);

    queryMasterManagerService = new QueryMasterManagerService(workerContext, qmManagerPort);
    addService(queryMasterManagerService);

    // taskrunner worker
    taskRunnerManager = new TaskRunnerManager(workerContext);
    addService(taskRunnerManager);

    tajoWorkerManagerService = new TajoWorkerManagerService(workerContext, peerRpcPort);
    addService(tajoWorkerManagerService);

    if(!yarnContainerMode) {
      if(taskRunnerMode && !TajoPullServerService.isStandalone()) {
        pullService = new TajoPullServerService();
        addService(pullService);
      }

      if (!systemConf.get(CommonTestingUtil.TAJO_TEST_KEY, "FALSE").equalsIgnoreCase("TRUE")) {
        try {
          httpPort = systemConf.getSocketAddrVar(ConfVars.WORKER_INFO_ADDRESS).getPort();
          if(queryMasterMode && !taskRunnerMode) {
            //If QueryMaster and TaskRunner run on single host, http port conflicts
            httpPort = systemConf.getSocketAddrVar(ConfVars.WORKER_QM_INFO_ADDRESS).getPort();
          }
          webServer = StaticHttpServer.getInstance(this ,"worker", null, httpPort ,
              true, null, systemConf, null);
          webServer.start();
          httpPort = webServer.getPort();
          LOG.info("Worker info server started:" + httpPort);

          deletionService = new DeletionService(getMountPath().size(), 0);
          if(systemConf.getBoolVar(ConfVars.WORKER_TEMPORAL_DIR_CLEANUP)){
            getWorkerContext().cleanupTemporalDirectories();
          }
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    LOG.info("Tajo Worker started: queryMaster=" + queryMasterMode + " taskRunner=" + taskRunnerMode +
        ", qmRpcPort=" + qmManagerPort +
        ",yarnContainer=" + yarnContainerMode + ", clientPort=" + clientPort +
        ", peerRpcPort=" + peerRpcPort + ":" + qmManagerPort + ",httpPort" + httpPort);

    super.init(conf);

    tajoMasterInfo = new TajoMasterInfo();
    if(yarnContainerMode && queryMasterMode) {
      tajoMasterInfo.setTajoMasterAddress(NetUtils.createSocketAddr(cmdArgs[2]));
      connectToCatalog();

      QueryId queryId = TajoIdUtils.parseQueryId(cmdArgs[1]);
      queryMasterManagerService.getQueryMaster().reportQueryStatusToQueryMaster(
          queryId, TajoProtos.QueryState.QUERY_MASTER_LAUNCHED);
    } else if(yarnContainerMode && taskRunnerMode) { //TaskRunner mode
      taskRunnerManager.startTask(cmdArgs);
    } else {
      if (systemConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
        tajoMasterInfo.setTajoMasterAddress(HAServiceUtil.getMasterUmbilicalAddress(systemConf));
        tajoMasterInfo.setWorkerResourceTrackerAddr(HAServiceUtil.getResourceTrackerAddress(systemConf));
      } else {
        tajoMasterInfo.setTajoMasterAddress(NetUtils.createSocketAddr(systemConf.getVar(ConfVars
            .TAJO_MASTER_UMBILICAL_RPC_ADDRESS)));
        tajoMasterInfo.setWorkerResourceTrackerAddr(NetUtils.createSocketAddr(systemConf.getVar(ConfVars
            .RESOURCE_TRACKER_RPC_ADDRESS)));
      }
      connectToCatalog();
    }

    workerHeartbeatThread = new WorkerHeartbeatService(workerContext);
    workerHeartbeatThread.init(conf);
    addIfService(workerHeartbeatThread);

    try {
      hashShuffleAppenderManager = new HashShuffleAppenderManager(systemConf);
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
      System.exit(-1);
    }
  }

  private void initWorkerMetrics() {
    workerSystemMetrics = new TajoSystemMetrics(systemConf, "worker", workerContext.getWorkerName());
    workerSystemMetrics.start();

    workerSystemMetrics.register("querymaster", "runningQueries", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        if(queryMasterManagerService != null) {
          return queryMasterManagerService.getQueryMaster().getQueryMasterTasks().size();
        } else {
          return 0;
        }
      }
    });

    workerSystemMetrics.register("task", "runningTasks", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        if(taskRunnerManager != null) {
          return taskRunnerManager.getNumTasks();
        } else {
          return 0;
        }
      }
    });
  }

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public void start() {
    super.start();
    initWorkerMetrics();
  }

  @Override
  public void stop() {
    if(stopped.getAndSet(true)) {
      return;
    }

    if(webServer != null) {
      try {
        webServer.stop();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    if (catalogClient != null) {
      catalogClient.close();
    }

    if(connPool != null) {
      connPool.shutdown();
      RpcChannelFactory.shutdown();
    }

    if(webServer != null && webServer.isAlive()) {
      try {
        webServer.stop();
      } catch (Exception e) {
      }
    }

    if(workerSystemMetrics != null) {
      workerSystemMetrics.stop();
    }

    if(deletionService != null) deletionService.stop();
    super.stop();
    LOG.info("TajoWorker main thread exiting");
  }

  public class WorkerContext {
    private ConcurrentHashMap<ExecutionBlockId, ExecutionBlockSharedResource> sharedResourceMap =
        new ConcurrentHashMap<ExecutionBlockId, ExecutionBlockSharedResource>();

    public QueryMaster getQueryMaster() {
      if (queryMasterManagerService == null) {
        return null;
      }
      return queryMasterManagerService.getQueryMaster();
    }

    public TajoConf getConf() {
      return systemConf;
    }

    public TajoWorkerManagerService getTajoWorkerManagerService() {
      return tajoWorkerManagerService;
    }

    public QueryMasterManagerService getQueryMasterManagerService() {
      return queryMasterManagerService;
    }

    public TajoWorkerClientService getTajoWorkerClientService() {
      return tajoWorkerClientService;
    }

    public TaskRunnerManager getTaskRunnerManager() {
      return taskRunnerManager;
    }

    public CatalogService getCatalog() {
      return catalogClient;
    }

    public int getHttpPort() {
      return httpPort;
    }

    public String getWorkerName() {
      if (queryMasterMode) {
        return getQueryMasterManagerService().getHostAndPort();
      } else {
        return getTajoWorkerManagerService().getHostAndPort();
      }
    }

    public void stopWorker(boolean force) {
      stop();
      if (force) {
        System.exit(0);
      }
    }

    public void initSharedResource(QueryContext queryContext, ExecutionBlockId blockId, String planJson)
        throws InterruptedException {

      if (!sharedResourceMap.containsKey(blockId)) {
        ExecutionBlockSharedResource resource = new ExecutionBlockSharedResource();
        if (sharedResourceMap.putIfAbsent(blockId, resource) == null) {
          resource.initialize(queryContext, planJson);
        }
      }
    }

    public ExecutionBlockSharedResource getSharedResource(ExecutionBlockId blockId) {
      return sharedResourceMap.get(blockId);
    }

    public void releaseSharedResource(ExecutionBlockId blockId) {
      sharedResourceMap.remove(blockId).release();
    }

    protected void cleanup(String strPath) {
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

    protected void cleanupTemporalDirectories() {
      if (deletionService == null) return;

      LocalDirAllocator lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);

      try {
        Iterable<Path> iter = lDirAllocator.getAllLocalPathsToRead(".", systemConf);
        FileSystem localFS = FileSystem.getLocal(systemConf);
        for (Path path : iter) {
          PathData[] items = PathData.expandAsGlob(localFS.makeQualified(new Path(path, "*")).toString(), systemConf);

          ArrayList<Path> paths = new ArrayList<Path>();
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

    public boolean isYarnContainerMode() {
      return yarnContainerMode;
    }

    public void setNumClusterNodes(int numClusterNodes) {
      TajoWorker.this.numClusterNodes.set(numClusterNodes);
    }

    public int getNumClusterNodes() {
      return TajoWorker.this.numClusterNodes.get();
    }

    public void setClusterResource(TajoMasterProtocol.ClusterResourceSummary clusterResource) {
      synchronized (numClusterNodes) {
        TajoWorker.this.clusterResource = clusterResource;
      }
    }

    public TajoMasterProtocol.ClusterResourceSummary getClusterResource() {
      synchronized (numClusterNodes) {
        return TajoWorker.this.clusterResource;
      }
    }

    public InetSocketAddress getTajoMasterAddress() {
      return tajoMasterInfo.getTajoMasterAddress();
    }

    public void setTajoMasterAddress(InetSocketAddress tajoMasterAddress) {
      tajoMasterInfo.setTajoMasterAddress(tajoMasterAddress);
    }

    public InetSocketAddress getResourceTrackerAddress() {
      return tajoMasterInfo.getWorkerResourceTrackerAddr();
    }

    public void setWorkerResourceTrackerAddr(InetSocketAddress workerResourceTrackerAddr) {
      tajoMasterInfo.setWorkerResourceTrackerAddr(workerResourceTrackerAddr);
    }

    public int getPeerRpcPort() {
      return getTajoWorkerManagerService() == null ? 0 : getTajoWorkerManagerService().getBindAddr().getPort();
    }

    public boolean isQueryMasterMode() {
      return queryMasterMode;
    }

    public boolean isTaskRunnerMode() {
      return taskRunnerMode;
    }

    public TajoSystemMetrics getWorkerSystemMetrics() {
      return workerSystemMetrics;
    }

    public HashShuffleAppenderManager getHashShuffleAppenderManager() {
      return hashShuffleAppenderManager;
    }

    public int getPullServerPort() {
      if (pullService != null) {
        long startTime = System.currentTimeMillis();
        while (true) {
          int pullServerPort = pullService.getPort();
          if (pullServerPort > 0) {
            return pullServerPort;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
          if (System.currentTimeMillis() - startTime > 30 * 1000) {
            LOG.fatal("TajoWorker stopped cause can't get PullServer port.");
            System.exit(-1);
          }
        }
      } else {
        if (pullServerPort != 0) {
          return pullServerPort;
        } else {
          loadPullServerPort();
          return pullServerPort;
        }
      }
    }
  }

  private void loadPullServerPort() {
    // get pull server port
    long startTime = System.currentTimeMillis();
    while (true) {
      pullServerPort = TajoPullServerService.readPullServerPort();
      if (pullServerPort > 0) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      if (System.currentTimeMillis() - startTime > 30 * 1000) {
        LOG.fatal("TajoWorker stopped cause can't get PullServer port.");
        System.exit(-1);
      }
    }
  }

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

  public static List<File> getMountPath() throws IOException {
    BufferedReader mountOutput = null;
    Process mountProcess = null;
    try {
      mountProcess = Runtime.getRuntime ().exec("mount");
      mountOutput = new BufferedReader(new InputStreamReader(mountProcess.getInputStream()));
      List<File> mountPaths = new ArrayList<File>();
      while (true) {
        String line = mountOutput.readLine();
        if (line == null) {
          break;
        }

        int indexStart = line.indexOf(" on /");
        int indexEnd = line.indexOf(" ", indexStart + 4);

        mountPaths.add(new File(line.substring (indexStart + 4, indexEnd)));
      }
      return mountPaths;
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if(mountOutput != null) {
        mountOutput.close();
      }
      if (mountProcess != null) {
        org.apache.commons.io.IOUtils.closeQuietly(mountProcess.getInputStream());
        org.apache.commons.io.IOUtils.closeQuietly(mountProcess.getOutputStream());
        org.apache.commons.io.IOUtils.closeQuietly(mountProcess.getErrorStream());
      }
    }
  }

  public static void main(String[] args) throws Exception {
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
