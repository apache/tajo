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
import org.apache.tajo.ipc.TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.scheduler.event.SchedulerEvent;
import org.apache.tajo.master.scheduler.event.SchedulerEventType;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.AsyncRpcServer;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;

import java.net.InetSocketAddress;

import static org.apache.tajo.ResourceProtos.*;

/**
 * It receives pings that nodes periodically send. The ping messages contains the node resources and their statuses.
 * From ping messages, {@link TajoResourceTracker} tracks the recent status of all nodes.
 *
 * In detail, it has two main roles as follows:
 *
 * <ul>
 *   <li>Membership management for nodes which join to a Tajo cluster</li>
 *   <ul>
 *    <li>Register - It receives the ping from a new node. It registers the node.</li>
 *    <li>Unregister - It unregisters a node who does not send ping for some expiry time.</li>
 *   <ul>
 *   <li>Status Update - It updates the status of all participating nodes</li>
 * </ul>
 */
public class TajoResourceTracker extends AbstractService implements TajoResourceTrackerProtocolService.Interface {
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(TajoResourceTracker.class);

  private final TajoResourceManager manager;
  /** the context of TajoResourceManager */
  private final TajoRMContext rmContext;
  /** Liveliness monitor which checks ping expiry times of nodes */
  private final NodeLivelinessMonitor nodeLivelinessMonitor;

  /** RPC server for node resource tracker */
  private AsyncRpcServer server;
  /** The bind address of RPC server of node resource tracker */
  private InetSocketAddress bindAddress;

  /** node heartbeat interval in query running */
  private int activeInterval;

  public TajoResourceTracker(TajoResourceManager manager, NodeLivelinessMonitor nodeLivelinessMonitor) {
    super(TajoResourceTracker.class.getSimpleName());
    this.manager = manager;
    this.rmContext = manager.getRMContext();
    this.nodeLivelinessMonitor = nodeLivelinessMonitor;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    TajoConf systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    activeInterval = systemConf.getIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_ACTIVE_INTERVAL);

    InetSocketAddress initIsa = systemConf.getSocketAddrVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS);

    int workerNum = systemConf.getIntVar(TajoConf.ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM);
    server = new AsyncRpcServer(TajoResourceTrackerProtocol.class, this, initIsa, workerNum);

    server.start();
    bindAddress = NetUtils.getConnectAddress(server.getListenAddress());
    // Set actual bind address to the systemConf
    systemConf.setVar(TajoConf.ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, NetUtils.getHostPortString(bindAddress));

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

  private static NodeStatusEvent createStatusEvent(NodeHeartbeatRequest heartbeat) {
    return new NodeStatusEvent(
        heartbeat.getWorkerId(),
        heartbeat.getRunningTasks(),
        heartbeat.getRunningQueryMasters(),
        new NodeResource(heartbeat.getAvailableResource()),
        heartbeat.hasTotalResource() ? new NodeResource(heartbeat.getTotalResource()) : null);
  }

  @Override
  public void nodeHeartbeat(
      RpcController controller,
      NodeHeartbeatRequest heartbeat,
      RpcCallback<NodeHeartbeatResponse> done) {

    NodeHeartbeatResponse.Builder response = NodeHeartbeatResponse.newBuilder();
    ResponseCommand responseCommand = ResponseCommand.NORMAL;
    try {
      // get a workerId from the heartbeat
      int workerId = heartbeat.getWorkerId();

      if(rmContext.getNodes().containsKey(workerId)) { // if node is running

        // status update
        rmContext.getDispatcher().getEventHandler().handle(createStatusEvent(heartbeat));

        //refresh scheduler resource
        rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));

        // refresh ping
        nodeLivelinessMonitor.receivedPing(workerId);

      } else if (rmContext.getInactiveNodes().containsKey(workerId)) { // node was inactive
        if (!heartbeat.hasConnectionInfo()) {
          // request membership to worker node
          responseCommand = ResponseCommand.MEMBERSHIP;
        } else {

          // remove the inactive nodeStatus from the list of inactive nodes.
          NodeStatus nodeStatus = rmContext.getInactiveNodes().remove(workerId);
          nodeLivelinessMonitor.unregister(nodeStatus.getWorkerId());

          // create new nodeStatus instance
          NodeStatus newNodeStatus = createNodeStatus(heartbeat);
          int newWorkerId = newNodeStatus.getWorkerId();
          // add the new nodeStatus to the list of active nodes
          rmContext.getNodes().putIfAbsent(newWorkerId, newNodeStatus);

          // Transit the nodeStatus to RUNNING
          rmContext.getDispatcher().getEventHandler().handle(new NodeEvent(newWorkerId, NodeEventType.STARTED));
          // register the nodeStatus to the liveliness monitor
          nodeLivelinessMonitor.register(newWorkerId);

          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }

      } else { // if new node pings firstly

        // The pings have not membership information
        if (!heartbeat.hasConnectionInfo()) {
          // request membership to node
          responseCommand = ResponseCommand.MEMBERSHIP;
        } else {

          // create new node instance
          NodeStatus newNodeStatus = createNodeStatus(heartbeat);
          NodeStatus oldNodeStatus = rmContext.getNodes().putIfAbsent(workerId, newNodeStatus);

          if (oldNodeStatus == null) {
            // Transit the worker to RUNNING
            rmContext.rmDispatcher.getEventHandler().handle(new NodeEvent(workerId, NodeEventType.STARTED));
          } else {
            LOG.info("Reconnect from the node at: " + workerId);
            nodeLivelinessMonitor.unregister(workerId);
            rmContext.getDispatcher().getEventHandler().handle(new NodeReconnectEvent(workerId, newNodeStatus));
          }

          nodeLivelinessMonitor.register(workerId);
          rmContext.getDispatcher().getEventHandler().handle(new SchedulerEvent(SchedulerEventType.RESOURCE_UPDATE));
        }
      }
    } finally {
      if(manager.getScheduler().getRunningQuery() > 0) {
        response.setHeartBeatInterval(activeInterval);
      }
      done.run(response.setCommand(responseCommand).build());
    }
  }

  private NodeStatus createNodeStatus(NodeHeartbeatRequest request) {
    return new NodeStatus(rmContext, new NodeResource(request.getTotalResource()),
        new WorkerConnectionInfo(request.getConnectionInfo()));
  }
}
