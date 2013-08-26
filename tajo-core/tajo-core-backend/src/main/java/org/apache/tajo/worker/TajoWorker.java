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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.querymaster.QueryMaster;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.ProtoAsyncRpcClient;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.webapp.StaticHttpServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoWorker extends CompositeService {
  public static PrimitiveProtos.BoolProto TRUE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  public static PrimitiveProtos.BoolProto FALSE_PROTO = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();

  private static final Log LOG = LogFactory.getLog(TajoWorker.class);

  private TajoConf tajoConf;

  private StaticHttpServer webServer;

  private TajoWorkerClientService tajoWorkerClientService;

  private TajoWorkerManagerService tajoWorkerManagerService;

  //to TajoMaster
  private ProtoAsyncRpcClient tajoMasterRpc;

  private TajoMasterProtocol.TajoMasterProtocolService tajoMasterRpcClient;

  private WorkerContext workerContext;

  private TaskRunnerManager taskRunnerManager;

  private TajoPullServerService pullService;

  private String daemonMode;

  private WorkerHeartbeatThread workerHeartbeatThread;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  public TajoWorker(String daemonMode) throws Exception {
    super(TajoWorker.class.getName());
    this.daemonMode = daemonMode;
  }

  @Override
  public void init(Configuration conf) {
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

    this.tajoConf = (TajoConf)conf;
    RackResolver.init(tajoConf);

    workerContext = new WorkerContext();

    String resourceManagerClassName = conf.get("tajo.resource.manager",
        TajoWorkerResourceManager.class.getCanonicalName());

    boolean randomPort = true;
    if(resourceManagerClassName.indexOf(TajoWorkerResourceManager.class.getName()) >= 0) {
      randomPort = false;
    }
    int infoPort = tajoConf.getInt("tajo.worker.info.port", 8090);
    int clientPort = tajoConf.getInt("tajo.worker.client.rpc.port", 8091);
    int managerPort = tajoConf.getInt("tajo.worker.manager.rpc.port", 8092);

    if(randomPort) {
      clientPort = 0;
      managerPort = 0;
      tajoConf.setInt(TajoConf.ConfVars.PULLSERVER_PORT.varname, 0);
      //infoPort = 0;
    }
    try {
      //TODO WebServer port configurable
      webServer = StaticHttpServer.getInstance(this, "admin", null, infoPort,
          true, null, tajoConf, null);
      webServer.start();
    } catch (Exception e) {
      LOG.error("Can' start info http server:" + e.getMessage(), e);
    }

    if(!"qm".equals(daemonMode)) {
      taskRunnerManager = new TaskRunnerManager(workerContext);
      addService(taskRunnerManager);
    }

    if(workerContext.isStandbyMode()) {
      pullService = new TajoPullServerService();
      addService(pullService);
    }

    if(!"tr".equals(daemonMode)) {
      tajoWorkerClientService = new TajoWorkerClientService(workerContext, clientPort);
      addService(tajoWorkerClientService);

      tajoWorkerManagerService = new TajoWorkerManagerService(workerContext, managerPort);
      addService(tajoWorkerManagerService);
      LOG.info("====> Tajo worker started: mode=" + daemonMode + ", clientPort=" + clientPort + ", managerPort=" + managerPort);
    } else {
      LOG.info("====> Tajo worker started: mode=" + daemonMode);
    }

    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    if(webServer != null) {
      try {
        webServer.stop();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
    if(workerHeartbeatThread != null) {
      workerHeartbeatThread.interrupt();
    }

//    try {
//      FileSystem.closeAll();
//    } catch (IOException e) {
//      LOG.error(e.getMessage(), e);
//    }
    if(tajoMasterRpc != null) {
      tajoMasterRpc.close();
    }
//    for(Service eachService: getServices()) {
//      System.out.println("Service:" + eachService);
//    }
    super.stop();
    LOG.info("TajoWorker main thread exiting");
  }

  public class WorkerContext {
    public QueryMaster getQueryMaster() {
      return tajoWorkerManagerService.getQueryMaster();
    }

    public TajoWorkerManagerService getTajoWorkerManagerService() {
      return tajoWorkerManagerService;
    }

    public TajoWorkerClientService getTajoWorkerClientService() {
      return tajoWorkerClientService;
    }

    public TajoMasterProtocol.TajoMasterProtocolService getTajoMasterRpcClient() {
      return tajoMasterRpcClient;
    }

    public TaskRunnerManager getTaskRunnerManager() {
      return taskRunnerManager;
    }

    public TajoPullServerService getPullService() {
      return pullService;
    }

    public void stopWorker(boolean force) {
      stop();
      if(force) {
        System.exit(0);
      }
    }

    public boolean isStandbyMode() {
      return !"qm".equals(daemonMode) && !"tr".equals(daemonMode);
    }
  }

  public void stopWorkerForce() {
    stop();
  }

  private void setWorkerMode(String[] params) {
    if("qm".equals(daemonMode)) {
      //QueryMaster mode
      String tajoMasterAddress = params[2];

      LOG.info("Init TajoMaster connection to:" + tajoMasterAddress);
      InetSocketAddress addr = NetUtils.createSocketAddr(tajoMasterAddress);
      try {
        tajoMasterRpc = new ProtoAsyncRpcClient(TajoMasterProtocol.class, addr);
        tajoMasterRpcClient = tajoMasterRpc.getStub();
      } catch (Exception e) {
        LOG.error("Can't connect to TajoMaster[" + addr + "], " + e.getMessage(), e);
      }

      QueryId queryId = TajoIdUtils.parseQueryId(params[1]);
      tajoWorkerManagerService.getQueryMaster().reportQueryStatusToQueryMaster(
          queryId, TajoProtos.QueryState.QUERY_MASTER_LAUNCHED);
    } else if("tr".equals(daemonMode)) {
      //TaskRunner mode
      taskRunnerManager.startTask(params);
    } else {
      //Standby mode
      String tajoMasterAddress = tajoConf.get("tajo.master.manager.addr");
      LOG.info("Init TajoMaster connection to:" + tajoMasterAddress);
      InetSocketAddress addr = NetUtils.createSocketAddr(tajoMasterAddress);
      try {
        tajoMasterRpc = new ProtoAsyncRpcClient(TajoMasterProtocol.class, addr);
        tajoMasterRpcClient = tajoMasterRpc.getStub();
      } catch (Exception e) {
        LOG.error("Can't connect to TajoMaster[" + addr + "], " + e.getMessage(), e);
      }
      workerHeartbeatThread = new WorkerHeartbeatThread();
      workerHeartbeatThread.start();
    }
  }

  class WorkerHeartbeatThread extends Thread {
    TajoMasterProtocol.ServerStatusProto.System systemInfo;
    List<TajoMasterProtocol.ServerStatusProto.Disk> diskInfos =
        new ArrayList<TajoMasterProtocol.ServerStatusProto.Disk>();
    int workerDiskSlots;
    List<File> mountPaths;

    public WorkerHeartbeatThread() {
      int workerMemoryMBSlots;
      int workerCpuCoreSlots;

      boolean useSystemInfo = tajoConf.getBoolean("tajo.worker.slots.use.os.info", true);

      try {
        mountPaths = getMountPath();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }

      if(useSystemInfo) {
        float memoryRatio = tajoConf.getFloat("tajo.worker.slots.os.memory.ratio", 0.8f);
        workerMemoryMBSlots = getTotalMemoryMB();
        workerMemoryMBSlots = (int)((float)(workerMemoryMBSlots) * memoryRatio);
        workerCpuCoreSlots = Runtime.getRuntime().availableProcessors();
        if(mountPaths == null) {
          workerDiskSlots = 2;
        } else {
          workerDiskSlots = mountPaths.size();
        }
      } else {
        workerMemoryMBSlots = tajoConf.getInt("tajo.worker.slots.memoryMB", 2048);
        workerDiskSlots = tajoConf.getInt("tajo.worker.slots.disk", 2);
        workerCpuCoreSlots = tajoConf.getInt("tajo.worker.slots.cpu.core", 4);
      }

      workerDiskSlots = workerDiskSlots * tajoConf.getInt("tajo.worker.slots.disk.concurrency", 4);

      systemInfo = TajoMasterProtocol.ServerStatusProto.System.newBuilder()
          .setAvailableProcessors(workerCpuCoreSlots)
          .setFreeMemoryMB(0)
          .setMaxMemoryMB(0)
          .setTotalMemoryMB(workerMemoryMBSlots)
          .build();
    }

    public void run() {
      LOG.info("Worker Resource Heartbeat Thread start.");
      int sendDiskInfoCount = 0;
      while(true) {
        if(sendDiskInfoCount == 0 && mountPaths != null) {
          for(File eachFile: mountPaths) {
            diskInfos.clear();
            diskInfos.add(TajoMasterProtocol.ServerStatusProto.Disk.newBuilder()
                .setAbsolutePath(eachFile.getAbsolutePath())
                .setTotalSpace(eachFile.getTotalSpace())
                .setFreeSpace(eachFile.getFreeSpace())
                .setUsableSpace(eachFile.getUsableSpace())
                .build());
          }
        }
        TajoMasterProtocol.ServerStatusProto serverStatus = TajoMasterProtocol.ServerStatusProto.newBuilder()
            .addAllDisk(diskInfos)
            .setRunningTaskNum(0)   //TODO
            .setSystem(systemInfo)
            .setDiskSlots(workerDiskSlots)
            .build();

        TajoMasterProtocol.TajoHeartbeat heartbeatProto = TajoMasterProtocol.TajoHeartbeat.newBuilder()
            .setTajoWorkerHost(workerContext.getTajoWorkerManagerService().getBindAddr().getHostName())
            .setTajoWorkerPort(workerContext.getTajoWorkerManagerService().getBindAddr().getPort())
            .setTajoWorkerClientPort(workerContext.getTajoWorkerClientService().getBindAddr().getPort())
            .setServerStatus(serverStatus)
            .build();

        workerContext.getTajoMasterRpcClient().heartbeat(null, heartbeatProto, NullCallback.get());
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        sendDiskInfoCount++;

        if(sendDiskInfoCount > 10) {
          sendDiskInfoCount = 0;
        }
      }

      LOG.info("Worker Resource Heartbeat Thread stopped.");
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

  public void startWorker(TajoConf tajoConf, String[] args) {
    init(tajoConf);
    start();
    setWorkerMode(args);
  }

  public static List<File> getMountPath() throws Exception {
    BufferedReader mountOutput = null;
    try {
      Process mountProcess = Runtime.getRuntime ().exec("mount");
      mountOutput = new BufferedReader(new InputStreamReader(mountProcess.getInputStream()));
      List<File> mountPaths = new ArrayList<File>();
      while (true) {
        String line = mountOutput.readLine();
        if (line == null) {
          break;
        }

        System.out.println(line);

        int indexStart = line.indexOf(" on /");
        int indexEnd = line.indexOf(" ", indexStart + 4);

        mountPaths.add(new File(line.substring (indexStart + 4, indexEnd)));
      }
      return mountPaths;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      if(mountOutput != null) {
        mountOutput.close();
      }
    }
  }

  public static int getTotalMemoryMB() {
    com.sun.management.OperatingSystemMXBean bean =
        (com.sun.management.OperatingSystemMXBean)
            java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long max = bean.getTotalPhysicalMemorySize();
    return ((int)(max/1024));
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(TajoWorker.class, args, LOG);

    if(args.length < 1) {
      LOG.error("Wrong startup params");
      System.exit(-1);
    }

    String workerMode = args[0];

    try {
      TajoWorker tajoWorker = new TajoWorker(workerMode);
      tajoWorker.startWorker(new TajoConf(new YarnConfiguration()), args);
    } catch (Throwable t) {
      LOG.fatal("Error starting TajoWorker", t);
      System.exit(-1);
    }
  }
}
