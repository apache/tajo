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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.scheduler.event.SchedulerEvent;
import org.apache.tajo.master.scheduler.event.SchedulerEventType;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;

import java.net.InetSocketAddress;

import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.*;

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

  private final TajoResourceManager manager;
  /** the context of TajoResourceManager */
  private final TajoRMContext rmContext;
  /** Liveliness monitor which checks ping expiry times of workers */
  private final WorkerLivelinessMonitor workerLivelinessMonitor;

  /** RPC server for worker resource tracker */
  private AsyncRpcServer server;
  /** The bind address of RPC server of worker resource tracker */
  private InetSocketAddress bindAddress;

  public TajoResourceTracker(TajoResourceManager manager, WorkerLivelinessMonitor workerLivelinessMonitor) {
    super(TajoResourceTracker.class.getSimpleName());
    this.manager = manager;
    this.rmContext = manager.getRMContext();
    this.workerLivelinessMonitor = workerLivelinessMonitor;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    TajoConf systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);

    String confMasterServiceAddr = systemConf.getVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterServiceAddr);

    int workerNum = systemConf.getIntVar(TajoConf.ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM);
    server = new AsyncRpcServer(TajoResourceTrackerProtocol.class, this, initIsa, workerNum);

    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    // Set actual bind address to the systemConf
    systemConf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, NetUtils.normalizeInetSocketAddress(bindAddress));

    LOG.info("TajoResourceTracker starts up (" + this.bindAddress + ")");
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    // server can be null if some exception occurs before the rpc server starts up.
    if(server != null) {
      server.shutdown();
      server = null;
    }
    super.serviceStop();
  }

  private static WorkerStatusEvent createStatusEvent(NodeHeartbeatRequestProto heartbeat) {
    return new WorkerStatusEvent(
        heartbeat.getWorkerId(),
        heartbeat.getRunningTasks(),
        heartbeat.getRunningQueryMasters(),
        new NodeResource(heartbeat.getAvailableResource()),
        heartbeat.hasTotalResource() ? new NodeResource(heartbeat.getTotalResource()) : null);
  }

  @Override
  public void nodeHeartbeat(
      RpcController controller,
      NodeHeartbeatRequestProto heartbeat,
      RpcCallback<NodeHeartbeatResponseProto> done) {

    NodeHeartbeatResponseProto.Builder response = NodeHeartbeatResponseProto.newBuilder();
    ResponseCommand responseCommand = ResponseCommand.NORMAL;
    try {
      // get a workerId from the heartbeat
      int workerId = heartbeat.getWorkerId();

      if(rmContext.getWorkers().containsKey(workerId)) { // if worker is running

        if (heartbeat.hasAvailableResource()) {
          // status update
          rmContext.getDispatcher().getEventHandler().handle(createStatusEvent(heartbeat));

          //refresh scheduler resource
          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }

        // refresh ping
        workerLivelinessMonitor.receivedPing(workerId);

      } else if (rmContext.getInactiveWorkers().containsKey(workerId)) { // worker was inactive
        if (!heartbeat.hasConnectionInfo()) {
          // request membership to worker node
          responseCommand = ResponseCommand.MEMBERSHIP;
        } else {

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

          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }

      } else { // if new worker pings firstly

        // The pings have not membership information
        if (!heartbeat.hasConnectionInfo()) {
          // request membership to worker node
          responseCommand = ResponseCommand.MEMBERSHIP;
        } else {

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
          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }
      }
    } finally {
      if(manager.getScheduler().getRunningQuery() > 0) {
        response.setHeartBeatInterval(1000); //1 sec
      }
      done.run(response.setCommand(responseCommand).build());
    }
  }

  private Worker createWorkerResource(NodeHeartbeatRequestProto request) {
    return new Worker(rmContext, new NodeResource(request.getTotalResource()),
        new WorkerConnectionInfo(request.getConnectionInfo()));
  }
}
