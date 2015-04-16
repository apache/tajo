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

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.ClusterResourceSummary;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.ServerStatusProto;
import org.apache.tajo.ipc.QueryCoordinatorProtocol.TajoHeartbeatResponse;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.storage.DiskDeviceInfo;
import org.apache.tajo.storage.DiskMountInfo;
import org.apache.tajo.storage.DiskUtil;

import java.io.File;
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
  private RpcClientManager connectionManager;
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
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.systemConf = (TajoConf) conf;

    this.connectionManager = RpcClientManager.getInstance();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    thread = new WorkerHeartbeatThread();
    thread.start();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    if(thread.stopped.getAndSet(true)){
      return;
    }

    synchronized (thread) {
      thread.notifyAll();
    }

    super.serviceStop();
  }

  class WorkerHeartbeatThread extends Thread {
    private volatile AtomicBoolean stopped = new AtomicBoolean(false);
    ServerStatusProto.System systemInfo;
    List<ServerStatusProto.Disk> diskInfos = Lists.newArrayList();
    float workerDiskSlots;
    int workerMemoryMB;
    List<DiskDeviceInfo> diskDeviceInfos;

    public WorkerHeartbeatThread() {
      int workerCpuCoreNum;

      boolean dedicatedResource = systemConf.getBoolVar(TajoConf.ConfVars.WORKER_RESOURCE_DEDICATED);

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

      systemInfo = ServerStatusProto.System.newBuilder()
          .setAvailableProcessors(workerCpuCoreNum)
          .setFreeMemoryMB(0)
          .setMaxMemoryMB(0)
          .setTotalMemoryMB(getTotalMemoryMB())
          .build();
    }

    public void run() {
      LOG.info("Worker Resource Heartbeat Thread start.");
      int sendDiskInfoCount = 0;

      while(!stopped.get()) {
        if(sendDiskInfoCount == 0 && diskDeviceInfos != null) {
          getDiskUsageInfos();
        }
        ServerStatusProto.JvmHeap jvmHeap =
            ServerStatusProto.JvmHeap.newBuilder()
                .setMaxHeap(Runtime.getRuntime().maxMemory())
                .setFreeHeap(Runtime.getRuntime().freeMemory())
                .setTotalHeap(Runtime.getRuntime().totalMemory())
                .build();

        ServerStatusProto serverStatus = ServerStatusProto.newBuilder()
            .addAllDisk(diskInfos)
            .setRunningTaskNum(
                context.getTaskRunnerManager() == null ? 1 : context.getTaskRunnerManager().getNumTasks())
            .setSystem(systemInfo)
            .setDiskSlots(workerDiskSlots)
            .setMemoryResourceMB(workerMemoryMB)
            .setJvmHeap(jvmHeap)
            .build();

        NodeHeartbeat heartbeatProto = NodeHeartbeat.newBuilder()
            .setConnectionInfo(context.getConnectionInfo().getProto())
            .setServerStatus(serverStatus)
            .build();

        NettyClientBase rmClient = null;
        try {
          CallFuture<TajoHeartbeatResponse> callBack = new CallFuture<TajoHeartbeatResponse>();

          ServiceTracker serviceTracker = context.getServiceTracker();
          rmClient = connectionManager.getClient(serviceTracker.getResourceTrackerAddress(),
              TajoResourceTrackerProtocol.class, true);
          TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService resourceTracker = rmClient.getStub();
          resourceTracker.heartbeat(callBack.getController(), heartbeatProto, callBack);

          TajoHeartbeatResponse response = callBack.get(2, TimeUnit.SECONDS);
          if(response != null) {
            ClusterResourceSummary clusterResourceSummary = response.getClusterResourceSummary();
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
          LOG.warn("Heartbeat response is being delayed.", te);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }

        try {
          if(!stopped.get()){
            synchronized (thread){
              thread.wait(10 * 1000);
            }
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
            diskInfos.add(ServerStatusProto.Disk.newBuilder()
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
    javax.management.MBeanServer mBeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
    long max = 0;
    Object maxObject = null;
    try {
      javax.management.ObjectName osName = new javax.management.ObjectName("java.lang:type=OperatingSystem");
      if (!System.getProperty("java.vendor").startsWith("IBM")) {
        maxObject = mBeanServer.getAttribute(osName, "TotalPhysicalMemorySize");
      } else {
        maxObject = mBeanServer.getAttribute(osName, "TotalPhysicalMemory");
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    if (maxObject != null) {
      max = ((Long)maxObject).longValue();
    }
    return ((int) (max / (1024 * 1024)));
  }
}
