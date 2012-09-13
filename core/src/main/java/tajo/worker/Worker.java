/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.QueryUnitAttemptId;
import tajo.common.Sleeper;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.*;
import tajo.engine.cluster.MasterAddressTracker;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.AsyncWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.storage.StorageUtil;
import tajo.webapp.HttpServer;
import tajo.worker.dataserver.HttpDataServer;
import tajo.worker.dataserver.retriever.AdvancedDataRetriever;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Worker extends Thread implements AsyncWorkerProtocol {
  private static final Log LOG = LogFactory.getLog(Worker.class);

  private final TajoConf conf;

  // Server States
  private NettyRpcServer rpcServer;
  private InetSocketAddress isa;

  private volatile boolean stopped = false;
  private volatile boolean isOnline = false;

  private String serverName;

  // Cluster Management
  private ZkClient zkClient;
  private MasterAddressTracker masterAddrTracker;
  private MasterWorkerProtocol master;

  // Query Processing
  private FileSystem localFS;
  private FileSystem defaultFS;
  private final File workDir;

  private TQueryEngine queryEngine;
  private QueryLauncher queryLauncher;
  private final int coreNum = Runtime.getRuntime().availableProcessors();
  private final ExecutorService fetchLauncher = 
      Executors.newFixedThreadPool(coreNum);  
  private final Map<QueryUnitAttemptId, Task> tasks = Maps.newConcurrentMap();
  private HttpDataServer dataServer;
  private AdvancedDataRetriever retriever;
  private String dataServerURL;
  private final LocalDirAllocator lDirAllocator;
  
  //Web server
  private HttpServer webServer;

  private Sleeper sleeper;

  private WorkerContext workerContext;

  public Worker(final TajoConf conf) {
    this.conf = conf;
    lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TMP_DIR.varname);
    LOG.info(conf.getVar(ConfVars.WORKER_TMP_DIR));
    this.workDir = new File(conf.getVar(ConfVars.WORKER_TMP_DIR));
    sleeper = new Sleeper();
  }
  
  private void prepareServing() throws IOException {
    defaultFS = FileSystem.get(URI.create(
        conf.getVar(ConfVars.ENGINE_BASE_DIR)),conf);

    localFS = FileSystem.getLocal(conf);
    Path workDirPath = new Path(workDir.toURI());
    if (!localFS.exists(workDirPath)) {
      localFS.mkdirs(workDirPath);
      LOG.info("local temporal dir is created: " + localFS.exists(workDirPath));
      LOG.info("local temporal dir (" + workDir + ") is created");
    }

    String hostname = DNS.getDefaultHost(
        conf.get("nta.master.dns.interface", "default"),
        conf.get("nta.master.dns.nameserver", "default"));
    int port = this.conf.getIntVar(ConfVars.LEAFSERVER_PORT);

    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + this.isa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this,
        AsyncWorkerProtocol.class, initialIsa);
    this.rpcServer.start();
    
    // Set our address.
    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();
    
    this.zkClient = new ZkClient(this.conf);
    this.queryLauncher = new QueryLauncher();
    this.queryLauncher.start();
    this.queryEngine = new TQueryEngine(conf);
    
    this.retriever = new AdvancedDataRetriever();
    this.dataServer = new HttpDataServer(NetUtils.createSocketAddr(hostname, 0), 
        retriever);
    this.dataServer.start();
    
    InetSocketAddress dataServerAddr = this.dataServer.getBindAddress(); 
    this.dataServerURL = "http://" + dataServerAddr.getAddress().getHostAddress() + ":" 
        + dataServerAddr.getPort();
    LOG.info("dataserver listens on " + dataServerURL);

    this.workerContext = new WorkerContext();
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
  }

  private void participateCluster() throws IOException, InterruptedException,
      KeeperException {
    this.masterAddrTracker = new MasterAddressTracker(zkClient);
    this.masterAddrTracker.start();

    byte[] master;
    do {    
      master = masterAddrTracker.blockUntilAvailable(1000);
      LOG.info("Waiting for the Tajo master.....");
    } while (master == null);

    LOG.info("Got the master address (" + new String(master) + ")");
    // if the znode already exists, it will be updated for notification.
    ZkUtil.upsertEphemeralNode(zkClient,
        ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName));
    LOG.info("Created the znode " + NConstants.ZNODE_LEAFSERVERS + "/" 
        + serverName);
    
    InetSocketAddress addr = NetUtils.createSocketAddr(new String(master));
    this.master = (MasterWorkerProtocol) NettyRpc.getProtoParamBlockingRpcProxy(
        MasterWorkerProtocol.class, addr);
  }

  class WorkerContext {
    public TajoConf getConf() {
      return conf;
    }

    public AdvancedDataRetriever getRetriever() {
      return retriever;
    }

    public MasterWorkerProtocol getMaster() {
      return master;
    }

    public FileSystem getLocalFS() {
      return localFS;
    }

    public FileSystem getDefaultFS() {
      return defaultFS;
    }

    public LocalDirAllocator getLocalDirAllocator() {
      return lDirAllocator;
    }

    public TQueryEngine getTQueryEngine() {
      return queryEngine;
    }

    public Map<QueryUnitAttemptId, Task> getTasks() {
      return tasks;
    }

    public Task getTask(QueryUnitAttemptId taskId) {
      return tasks.get(taskId);
    }

    public ExecutorService getFetchLauncher() {
      return fetchLauncher;
    }

    public String getDataServerURL() {
      return dataServerURL;
    }
  }

  public void run() {
    LOG.info("Tajo Worker startup");

    try {
      try {
        prepareServing();
        participateCluster();
        
        webServer = new HttpServer("admin", this.isa.getHostName() ,8080 , 
            true, null, conf, null);
        webServer.setAttribute("tajo.master.addr",
            conf.getVar(ConfVars.MASTER_ADDRESS));
        webServer.start();
      } catch (Exception e) {
        abort(e.getMessage(), e);
      }

      if (!this.stopped) {
        this.isOnline = true;
        while (!this.stopped) {
          sleeper.sleep(3000);
          long time = System.currentTimeMillis();

          boolean res = sendHeartbeat(time);
          LOG.info("sent heart beat!! (" + res + ")");
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {     
      for (Task t : tasks.values()) {
        if (t.getStatus() != QueryStatus.QUERY_FINISHED) {
          t.kill();
        }
      }

      // remove the znode
      ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName);

      try {
        webServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }

      rpcServer.shutdown();
      queryLauncher.shutdown();      
      masterAddrTracker.stop();
      zkClient.close();
    }

    LOG.info("Worker (" + serverName + ") main thread exiting");
  }
  
  private boolean sendHeartbeat(long time) throws IOException {
    StatusReportProto.Builder report = StatusReportProto.newBuilder();
    report.setTimestamp(time);
    report.setServerName(serverName);
    
    // to send
    List<TaskStatusProto> list
      = new ArrayList<TaskStatusProto>();
    TaskStatusProto taskStatus;
    // to be removed
    List<QueryUnitAttemptId> tobeRemoved = Lists.newArrayList();
    
    // builds one status for each in-progress query
    QueryStatus qs;

    for (Task task : tasks.values()) {
      qs = task.getStatus();
      if (qs == QueryStatus.QUERY_ABORTED
          || qs == QueryStatus.QUERY_KILLED
          || qs == QueryStatus.QUERY_FINISHED) {
        // TODO - in-progress queries should be kept until this leafserver 
        // ensures that this report is delivered.
        tobeRemoved.add(task.getId());
      }

      taskStatus = task.getReport();
      list.add(taskStatus);
    }

    report.addAllStatus(list);

    return master.statusUpdate(report.build()).getValue();
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      shutdown("Shutdown Hook");
    }
  }

  public String getServerName() {
    return this.serverName;
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  public boolean isOnline() {
    return this.isOnline;
  }

  public void shutdown(final String msg) {
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
    synchronized (this) {
      notifyAll();
    }
  }

  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("ABORTING worker " + serverName + ": " + reason, cause);
    } else {
      LOG.fatal("ABORTING worker " + serverName + ": " + reason);
    }
    // TODO - abortRequest : to be implemented
    shutdown(reason);
  }
  
  public static Path getQueryUnitDir(QueryUnitAttemptId quid) {
    Path workDir = 
        StorageUtil.concatPath(
            quid.getSubQueryId().toString(),
            String.valueOf(quid.getQueryUnitId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }

  //////////////////////////////////////////////////////////////////////////////
  // AsyncWorkerProtocol
  //////////////////////////////////////////////////////////////////////////////

  static BoolProto TRUE_PROTO = BoolProto.newBuilder().setValue(true).build();

  @Override
  public BoolProto requestQueryUnit(QueryUnitRequestProto proto)
      throws Exception {
    QueryUnitRequest request = new QueryUnitRequestImpl(proto);
    Task task = new Task(workerContext, request);
    synchronized(tasks) {
      if (tasks.containsKey(task.getId())) {
        throw new IllegalStateException("Query unit (" + task.getId() + ") is already is submitted");
      }    
      tasks.put(task.getId(), task);
    }        
    if (task.hasFetchPhase()) {
      task.fetch(); // The fetch is performed in an asynchronous way.
    }
    task.init();
    queryLauncher.schedule(task);

    return TRUE_PROTO;
  }

  @Override
  public ServerStatusProto getServerStatus(NullProto request) {
    // serverStatus builder
    ServerStatusProto.Builder serverStatus = ServerStatusProto.newBuilder();
    // TODO: compute the available number of task slots
    serverStatus.setTaskNum(tasks.size());

    // system(CPU, memory) status builder
    ServerStatusProto.System.Builder systemStatus = ServerStatusProto.System
        .newBuilder();

    systemStatus.setAvailableProcessors(Runtime.getRuntime()
        .availableProcessors());
    systemStatus.setFreeMemory(Runtime.getRuntime().freeMemory());
    systemStatus.setMaxMemory(Runtime.getRuntime().maxMemory());
    systemStatus.setTotalMemory(Runtime.getRuntime().totalMemory());

    serverStatus.setSystem(systemStatus);

    // disk status builder
    File[] roots = File.listRoots();
    for (File root : roots) {
      ServerStatusProto.Disk.Builder diskStatus = ServerStatusProto.Disk
          .newBuilder();

      diskStatus.setAbsolutePath(root.getAbsolutePath());
      diskStatus.setTotalSpace(root.getTotalSpace());
      diskStatus.setFreeSpace(root.getFreeSpace());
      diskStatus.setUsableSpace(root.getUsableSpace());

      serverStatus.addDisk(diskStatus);
    }
    return serverStatus.build();
  }

  @VisibleForTesting
  Task getTask(QueryUnitAttemptId id) {
    return this.tasks.get(id);
  }
  
  private class QueryLauncher extends Thread {
    private final BlockingQueue<Task> blockingQueue
      = new ArrayBlockingQueue<Task>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    private boolean stopped = false;    
    
    public void schedule(Task task) throws InterruptedException {
      this.blockingQueue.put(task);
      task.setStatus(QueryStatus.QUERY_PENDING);
    }
    
    public void shutdown() {
      stopped = true;
    }
    
    @Override
    public void run() {
      try {
        LOG.info("Started the query launcher (maximum concurrent tasks: " 
            + coreNum + ")");
        while (!Thread.interrupted() && !stopped) {
          // wait for add
          Task task = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          
          // the futures keeps submitted tasks for force kill when
          // the leafserver shutdowns.
          if (task != null) {
            executor.submit(task);
          }          
        }
      } catch (Throwable t) {
        LOG.error(t);
      } finally {
        executor.shutdown();
      }
    }
  }

  @Override
  public CommandResponseProto requestCommand(CommandRequestProto request) {
    QueryUnitAttemptId uid;
    for (Command cmd : request.getCommandList()) {
      uid = new QueryUnitAttemptId(cmd.getId());
      Task task = tasks.get(uid);
      if (task == null) {
        LOG.warn("Unknown task: " + uid);
        return null;
      }
      QueryStatus status = task.getStatus();
      switch (cmd.getType()) {
      case FINALIZE:
        if (status == QueryStatus.QUERY_FINISHED
        || status == QueryStatus.QUERY_DATASERVER
        || status == QueryStatus.QUERY_ABORTED
        || status == QueryStatus.QUERY_KILLED) {
          task.cleanUp();
          LOG.info("Query unit ( " + uid + ") is finalized");
        } else {
          task.kill();
          LOG.info("Query unit ( " + uid + ") is stopped");
        }
        break;
      case STOP:
        task.kill();
        tasks.remove(task.getId());
        LOG.info("Query unit ( " + uid + ") is stopped");
        break;
      default:
        break;
      }
    }
    return null;
  }

  public static void main(String[] args) throws IOException {
    TajoConf conf = new TajoConf();
    Worker worker = new Worker(conf);
    worker.start();
  }
}
