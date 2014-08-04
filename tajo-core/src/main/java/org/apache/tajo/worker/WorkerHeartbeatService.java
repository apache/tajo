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

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.v2.DiskDeviceInfo;
import org.apache.tajo.storage.v2.DiskMountInfo;
import org.apache.tajo.storage.v2.DiskUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.NodeHeartbeat;

/**
 * It periodically sends heartbeat to {@link org.apache.tajo.master.rm.TajoResourceTracker} via asynchronous rpc.
 */
public class WorkerHeartbeatService extends AbstractService {
  /** class logger */
  private final static Log LOG = LogFactory.getLog(WorkerHeartbeatService.class);

  private final TajoWorker.WorkerContext context;
  private TajoConf systemConf;
  private RpcConnectionPool connectionPool;
  private WorkerHeartbeatThread thread;
  private static final float HDFS_DATANODE_STORAGE_SIZE;

  static {
    HDFS_DATANODE_STORAGE_SIZE = DiskUtil.getDataNodeStorageSize();
  }

  public WorkerHeartbeatService(TajoWorker.WorkerContext context) {
    super(WorkerHeartbeatService.class.getSimpleName());
    this.context = context;
  }

  @Override
  public void serviceInit(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf, "Configuration must be a TajoConf instance.");
    this.systemConf = (TajoConf) conf;

    connectionPool = RpcConnectionPool.getPool(systemConf);
    thread = new WorkerHeartbeatThread();
    thread.start();
    super.init(conf);
  }

  @Override
  public void serviceStop() {
    thread.stopped.set(true);
    synchronized (thread) {
      thread.notifyAll();
    }
    super.stop();
  }

  class WorkerHeartbeatThread extends Thread {
    private volatile AtomicBoolean stopped = new AtomicBoolean(false);
    TajoMasterProtocol.ServerStatusProto.System systemInfo;
    List<TajoMasterProtocol.ServerStatusProto.Disk> diskInfos =
        new ArrayList<TajoMasterProtocol.ServerStatusProto.Disk>();
    float workerDiskSlots;
    int workerMemoryMB;
    List<DiskDeviceInfo> diskDeviceInfos;

    public WorkerHeartbeatThread() {
      int workerCpuCoreNum;

      boolean dedicatedResource = systemConf.getBoolVar(TajoConf.ConfVars.WORKER_RESOURCE_DEDICATED);
      int workerCpuCoreSlots = Runtime.getRuntime().availableProcessors();

      try {
        diskDeviceInfos = DiskUtil.getDiskDeviceInfos();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }

      if(dedicatedResource) {
        float dedicatedMemoryRatio = systemConf.getFloatVar(TajoConf.ConfVars.WORKER_RESOURCE_DEDICATED_MEMORY_RATIO);
        int totalMemory = getTotalMemoryMB();
        workerMemoryMB = (int) ((float) (totalMemory) * dedicatedMemoryRatio);
        workerCpuCoreNum = Runtime.getRuntime().availableProcessors();

        if(diskDeviceInfos == null) {
          workerDiskSlots = TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISKS.defaultIntVal;
        } else {
          workerDiskSlots = diskDeviceInfos.size();
        }
      } else {
        workerMemoryMB = systemConf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB);
        workerCpuCoreNum = systemConf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
        workerDiskSlots = systemConf.getFloatVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISKS);

        if (systemConf.getBoolVar(TajoConf.ConfVars.WORKER_RESOURCE_DFS_DIR_AWARE) && HDFS_DATANODE_STORAGE_SIZE > 0) {
          workerDiskSlots = HDFS_DATANODE_STORAGE_SIZE;
        }
      }

      systemInfo = TajoMasterProtocol.ServerStatusProto.System.newBuilder()
          .setAvailableProcessors(workerCpuCoreNum)
          .setFreeMemoryMB(0)
          .setMaxMemoryMB(0)
          .setTotalMemoryMB(getTotalMemoryMB())
          .build();
    }

    public void run() {
      LOG.info("Worker Resource Heartbeat Thread start.");
      int sendDiskInfoCount = 0;
      int pullServerPort = 0;

      String hostName = null;
      int peerRpcPort = 0;
      int queryMasterPort = 0;
      int clientPort = 0;

      if(context.getTajoWorkerManagerService() != null) {
        hostName = context.getTajoWorkerManagerService().getBindAddr().getHostName();
        peerRpcPort = context.getTajoWorkerManagerService().getBindAddr().getPort();
      }
      if(context.getQueryMasterManagerService() != null) {
        hostName = context.getQueryMasterManagerService().getBindAddr().getHostName();
        queryMasterPort = context.getQueryMasterManagerService().getBindAddr().getPort();
      }
      if(context.getTajoWorkerClientService() != null) {
        clientPort = context.getTajoWorkerClientService().getBindAddr().getPort();
      }

      // get pull server port
      long startTime = System.currentTimeMillis();
      while (true) {
        if (context.getPullService() != null) {
          pullServerPort = context.getPullService().getPort();
          if (pullServerPort > 0) {
            break;
          }
        } else {
          pullServerPort = TajoPullServerService.readPullServerPort();
          if (pullServerPort > 0) {
            break;
          }
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
      while(!stopped.get()) {
        if(sendDiskInfoCount == 0 && diskDeviceInfos != null) {
          getDiskUsageInfos();
        }
        TajoMasterProtocol.ServerStatusProto.JvmHeap jvmHeap =
            TajoMasterProtocol.ServerStatusProto.JvmHeap.newBuilder()
                .setMaxHeap(Runtime.getRuntime().maxMemory())
                .setFreeHeap(Runtime.getRuntime().freeMemory())
                .setTotalHeap(Runtime.getRuntime().totalMemory())
                .build();

        TajoMasterProtocol.ServerStatusProto serverStatus = TajoMasterProtocol.ServerStatusProto.newBuilder()
            .addAllDisk(diskInfos)
            .setRunningTaskNum(
                context.getTaskRunnerManager() == null ? 1 : context.getTaskRunnerManager().getNumTasks())
            .setSystem(systemInfo)
            .setDiskSlots(workerDiskSlots)
            .setMemoryResourceMB(workerMemoryMB)
            .setJvmHeap(jvmHeap)
            .setQueryMasterMode(PrimitiveProtos.BoolProto.newBuilder().setValue(context.isQueryMasterMode()))
            .setTaskRunnerMode(PrimitiveProtos.BoolProto.newBuilder().setValue(context.isTaskRunnerMode()))
            .build();

        NodeHeartbeat heartbeatProto = NodeHeartbeat.newBuilder()
            .setTajoWorkerHost(hostName)
            .setTajoQueryMasterPort(queryMasterPort)
            .setPeerRpcPort(peerRpcPort)
            .setTajoWorkerClientPort(clientPort)
            .setTajoWorkerHttpPort(context.getHttpPort())
            .setTajoWorkerPullServerPort(pullServerPort)
            .setServerStatus(serverStatus)
            .build();

        NettyClientBase rmClient = null;
        try {
          CallFuture<TajoMasterProtocol.TajoHeartbeatResponse> callBack =
              new CallFuture<TajoMasterProtocol.TajoHeartbeatResponse>();

          rmClient = connectionPool.getConnection(context.getResourceTrackerAddress(), TajoResourceTrackerProtocol.class, true);
          TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService resourceTracker = rmClient.getStub();
          resourceTracker.heartbeat(callBack.getController(), heartbeatProto, callBack);

          TajoMasterProtocol.TajoHeartbeatResponse response = callBack.get(2, TimeUnit.SECONDS);
          if(response != null) {
            TajoMasterProtocol.ClusterResourceSummary clusterResourceSummary = response.getClusterResourceSummary();
            if(clusterResourceSummary.getNumWorkers() > 0) {
              context.setNumClusterNodes(clusterResourceSummary.getNumWorkers());
            }
            context.setClusterResource(clusterResourceSummary);
          } else {
            if(callBack.getController().failed()) {
              throw new ServiceException(callBack.getController().errorText());
            }
          }
        } catch (InterruptedException e) {
          break;
        } catch (TimeoutException te) {
          LOG.warn("Heartbeat response is being delayed.");
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
          connectionPool.releaseConnection(rmClient);
        }

        try {
          synchronized (WorkerHeartbeatThread.this){
            wait(10 * 1000);
          }
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

    private void getDiskUsageInfos() {
      diskInfos.clear();
      for(DiskDeviceInfo eachDevice: diskDeviceInfos) {
        List<DiskMountInfo> mountInfos = eachDevice.getMountInfos();
        if(mountInfos != null) {
          for(DiskMountInfo eachMount: mountInfos) {
            File eachFile = new File(eachMount.getMountPath());
            diskInfos.add(TajoMasterProtocol.ServerStatusProto.Disk.newBuilder()
                .setAbsolutePath(eachFile.getAbsolutePath())
                .setTotalSpace(eachFile.getTotalSpace())
                .setFreeSpace(eachFile.getFreeSpace())
                .setUsableSpace(eachFile.getUsableSpace())
                .build());
          }
        }
      }
    }
  }

  public static int getTotalMemoryMB() {
    com.sun.management.OperatingSystemMXBean bean =
        (com.sun.management.OperatingSystemMXBean)
            java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long max = bean.getTotalPhysicalMemorySize();
    return ((int) (max / (1024 * 1024)));
  }
}
