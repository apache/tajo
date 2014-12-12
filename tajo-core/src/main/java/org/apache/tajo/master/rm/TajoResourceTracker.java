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

package org.apache.tajo.master.rm;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;

import java.io.IOError;
import java.net.InetSocketAddress;

import static org.apache.tajo.ipc.TajoMasterProtocol.TajoHeartbeatResponse;
import static org.apache.tajo.ipc.TajoMasterProtocol.TajoHeartbeatResponse.Builder;
import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.NodeHeartbeat;
import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService;

/**
 * It receives pings that workers periodically send. The ping messages contains the worker resources and their statuses.
 * From ping messages, {@link TajoResourceTracker} tracks the recent status of all workers.
 *
 * In detail, it has two main roles as follows:
 *
 * <ul>
 *   <li>Membership management for nodes which join to a Tajo cluster</li>
 *   <ul>
 *    <li>Register - It receives the ping from a new worker. It registers the worker.</li>
 *    <li>Unregister - It unregisters a worker who does not send ping for some expiry time.</li>
 *   <ul>
 *   <li>Status Update - It updates the status of all participating workers</li>
 * </ul>
 */
public class TajoResourceTracker extends AbstractService implements TajoResourceTrackerProtocolService.Interface {
  /** Class logger */
  private Log LOG = LogFactory.getLog(TajoResourceTracker.class);
  /** the context of TajoWorkerResourceManager */
  private final TajoRMContext rmContext;
  /** Liveliness monitor which checks ping expiry times of workers */
  private final WorkerLivelinessMonitor workerLivelinessMonitor;

  /** RPC server for worker resource tracker */
  private AsyncRpcServer server;
  /** The bind address of RPC server of worker resource tracker */
  private InetSocketAddress bindAddress;

  public TajoResourceTracker(TajoRMContext rmContext, WorkerLivelinessMonitor workerLivelinessMonitor) {
    super(TajoResourceTracker.class.getSimpleName());
    this.rmContext = rmContext;
    this.workerLivelinessMonitor = workerLivelinessMonitor;
  }

  @Override
  public void serviceInit(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf, "Configuration must be a TajoConf instance");
    TajoConf systemConf = (TajoConf) conf;

    String confMasterServiceAddr = systemConf.getVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);

    try {
      server = new AsyncRpcServer(TajoResourceTrackerProtocol.class, this, initIsa, 3);
    } catch (Exception e) {
      LOG.error(e);
      throw new IOError(e);
    }

    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    // Set actual bind address to the systemConf
    systemConf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));

    LOG.info("TajoResourceTracker starts up (" + this.bindAddress + ")");
    super.start();
  }

  @Override
  public void serviceStop() {
    // server can be null if some exception occurs before the rpc server starts up.
    if(server != null) {
      server.shutdown();
      server = null;
    }
    super.stop();
  }

  /** The response builder */
  private static final Builder builder = TajoHeartbeatResponse.newBuilder().setHeartbeatResult(ProtoUtil.TRUE);

  private static WorkerStatusEvent createStatusEvent(int workerId, NodeHeartbeat heartbeat) {
    return new WorkerStatusEvent(
        workerId,
        heartbeat.getServerStatus().getRunningTaskNum(),
        heartbeat.getServerStatus().getJvmHeap().getMaxHeap(),
        heartbeat.getServerStatus().getJvmHeap().getFreeHeap(),
        heartbeat.getServerStatus().getJvmHeap().getTotalHeap());
  }

  @Override
  public void heartbeat(
      RpcController controller,
      NodeHeartbeat heartbeat,
      RpcCallback<TajoHeartbeatResponse> done) {

    try {
      // get a workerId from the heartbeat
      int workerId = heartbeat.getConnectionInfo().getId();

      if(rmContext.getWorkers().containsKey(workerId)) { // if worker is running

        // status update
        rmContext.getDispatcher().getEventHandler().handle(createStatusEvent(workerId, heartbeat));
        // refresh ping
        workerLivelinessMonitor.receivedPing(workerId);

      } else if (rmContext.getInactiveWorkers().containsKey(workerId)) { // worker was inactive

        // remove the inactive worker from the list of inactive workers.
        Worker worker = rmContext.getInactiveWorkers().remove(workerId);
        workerLivelinessMonitor.unregister(worker.getWorkerId());

        // create new worker instance
        Worker newWorker = createWorkerResource(heartbeat);
        int newWorkerId = newWorker.getWorkerId();
        // add the new worker to the list of active workers
        rmContext.getWorkers().putIfAbsent(newWorkerId, newWorker);

        // Transit the worker to RUNNING
        rmContext.getDispatcher().getEventHandler().handle(new WorkerEvent(newWorkerId, WorkerEventType.STARTED));
        // register the worker to the liveliness monitor
        workerLivelinessMonitor.register(newWorkerId);

      } else { // if new worker pings firstly

        // create new worker instance
        Worker newWorker = createWorkerResource(heartbeat);
        Worker oldWorker = rmContext.getWorkers().putIfAbsent(workerId, newWorker);

        if (oldWorker == null) {
          // Transit the worker to RUNNING
          rmContext.rmDispatcher.getEventHandler().handle(new WorkerEvent(workerId, WorkerEventType.STARTED));
        } else {
          LOG.info("Reconnect from the node at: " + workerId);
          workerLivelinessMonitor.unregister(workerId);
          rmContext.getDispatcher().getEventHandler().handle(new WorkerReconnectEvent(workerId, newWorker));
        }

        workerLivelinessMonitor.register(workerId);
      }

    } finally {
      builder.setClusterResourceSummary(getClusterResourceSummary());
      done.run(builder.build());
    }
  }

  private Worker createWorkerResource(NodeHeartbeat request) {
    boolean queryMasterMode = request.getServerStatus().getQueryMasterMode().getValue();
    boolean taskRunnerMode = request.getServerStatus().getTaskRunnerMode().getValue();

    WorkerResource workerResource = new WorkerResource();
    workerResource.setQueryMasterMode(queryMasterMode);
    workerResource.setTaskRunnerMode(taskRunnerMode);

    if(request.getServerStatus() != null) {
      workerResource.setMemoryMB(request.getServerStatus().getMemoryResourceMB());
      workerResource.setCpuCoreSlots(request.getServerStatus().getSystem().getAvailableProcessors());
      workerResource.setDiskSlots(request.getServerStatus().getDiskSlots());
      workerResource.setNumRunningTasks(request.getServerStatus().getRunningTaskNum());
      workerResource.setMaxHeap(request.getServerStatus().getJvmHeap().getMaxHeap());
      workerResource.setFreeHeap(request.getServerStatus().getJvmHeap().getFreeHeap());
      workerResource.setTotalHeap(request.getServerStatus().getJvmHeap().getTotalHeap());
    } else {
      workerResource.setMemoryMB(4096);
      workerResource.setDiskSlots(4);
      workerResource.setCpuCoreSlots(4);
    }

    return new Worker(rmContext, workerResource, new WorkerConnectionInfo(request.getConnectionInfo()));
  }

  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    int totalDiskSlots = 0;
    int totalCpuCoreSlots = 0;
    int totalMemoryMB = 0;

    int totalAvailableDiskSlots = 0;
    int totalAvailableCpuCoreSlots = 0;
    int totalAvailableMemoryMB = 0;

    synchronized(rmContext) {
      for(int eachWorker: rmContext.getWorkers().keySet()) {
        Worker worker = rmContext.getWorkers().get(eachWorker);
        WorkerResource resource = worker.getResource();
        if(worker != null) {
          totalMemoryMB += resource.getMemoryMB();
          totalAvailableMemoryMB += resource.getAvailableMemoryMB();

          totalDiskSlots += resource.getDiskSlots();
          totalAvailableDiskSlots += resource.getAvailableDiskSlots();

          totalCpuCoreSlots += resource.getCpuCoreSlots();
          totalAvailableCpuCoreSlots += resource.getAvailableCpuCoreSlots();
        }
      }
    }

    return TajoMasterProtocol.ClusterResourceSummary.newBuilder()
        .setNumWorkers(rmContext.getWorkers().size())
        .setTotalCpuCoreSlots(totalCpuCoreSlots)
        .setTotalDiskSlots(totalDiskSlots)
        .setTotalMemoryMB(totalMemoryMB)
        .setTotalAvailableCpuCoreSlots(totalAvailableCpuCoreSlots)
        .setTotalAvailableDiskSlots(totalAvailableDiskSlots)
        .setTotalAvailableMemoryMB(totalAvailableMemoryMB)
        .build();
  }
}
