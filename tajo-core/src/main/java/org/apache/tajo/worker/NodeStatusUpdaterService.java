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
import com.google.common.collect.Queues;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.*;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.worker.event.NodeStatusEvent;

import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.*;

/**
 * It periodically sends heartbeat to {@link org.apache.tajo.master.rm.TajoResourceTracker} via asynchronous rpc.
 */
public class NodeStatusUpdaterService extends AbstractService implements EventHandler<NodeStatusEvent> {

  private final static Log LOG = LogFactory.getLog(NodeStatusUpdaterService.class);

  private TajoConf tajoConf;
  private StatusUpdaterThread updaterThread;
  private AtomicBoolean flushReport = new AtomicBoolean();
  private volatile boolean isStopped;
  private volatile long heartBeatInterval;
  private BlockingQueue heartBeatRequestQueue;
  private TajoWorker.WorkerContext context;
  private NodeResourceManagerService nodeResourceManagerService;
  private AsyncRpcClient rmClient;
  private TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService resourceTracker;

  public NodeStatusUpdaterService(TajoWorker.WorkerContext context, NodeResourceManagerService resourceManagerService) {
    super(NodeStatusUpdaterService.class.getSimpleName());
    this.context = context;
    this.nodeResourceManagerService = resourceManagerService;
    this.heartBeatInterval = 10 * 1000; //10 sec
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.tajoConf = (TajoConf) conf;
    this.heartBeatRequestQueue = Queues.newLinkedBlockingQueue();
    this.updaterThread = new StatusUpdaterThread();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    updaterThread.start();
    super.serviceStart();
    LOG.info("Node status updater started.");
  }

  @Override
  public void serviceStop() throws Exception {
    this.isStopped = true;

    synchronized (updaterThread) {
      updaterThread.notifyAll();
    }
    super.serviceStop();
  }

  @Override
  public void handle(NodeStatusEvent event) {
    switch (event.getType()) {
      case REPORT_RESOURCE:
        heartBeatRequestQueue.add(event);
        break;
      case FLUSH_REPORTS:
        heartBeatRequestQueue.add(event);
        synchronized (updaterThread) {
          updaterThread.notifyAll();
        }
        break;
    }
  }

  private NodeHeartbeatRequestProto createResourceReport(NodeResource resource) {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setAvailableResource(resource.getProto());
    requestProto.setWorkerId(context.getConnectionInfo().getId());
    return requestProto.build();
  }

  private NodeHeartbeatRequestProto createHeartBeatReport() {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setWorkerId(context.getConnectionInfo().getId());
    return requestProto.build();
  }

  private NodeHeartbeatRequestProto createNodeStatusReport() {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setTotalResource(nodeResourceManagerService.getTotalResource().getProto());
    requestProto.setAvailableResource(nodeResourceManagerService.getAvailableResource().getProto());
    requestProto.setWorkerId(context.getConnectionInfo().getId());
    requestProto.setConnectionInfo(context.getConnectionInfo().getProto());

    //TODO set node status to requestProto.setStatus()
    return requestProto.build();
  }

  private TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService newStub()
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
    ServiceTracker serviceTracker = context.getServiceTracker();
    RpcClientManager.cleanup(rmClient);

    RpcClientManager rpcManager = RpcClientManager.getInstance();
    rmClient = rpcManager.newClient(serviceTracker.getResourceTrackerAddress(),
        TajoResourceTrackerProtocol.class, true, rpcManager.getRetries(),
        rpcManager.getTimeoutSeconds(), TimeUnit.SECONDS, false);
    return rmClient.getStub();
  }

  protected NodeHeartbeatResponseProto sendHeartbeat(NodeHeartbeatRequestProto requestProto)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException, ExecutionException {
    if (resourceTracker == null) {
      resourceTracker = newStub();
    }

    NodeHeartbeatResponseProto response = null;
    try {
      CallFuture<NodeHeartbeatResponseProto> callBack = new CallFuture<NodeHeartbeatResponseProto>();

      resourceTracker.nodeHeartbeat(callBack.getController(), requestProto, callBack);
      response = callBack.get(RpcConstants.DEFAULT_FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn(e.getMessage());
    } catch (TimeoutException te) {
      LOG.warn("Heartbeat response is being delayed.", te);
    } catch (ExecutionException ee) {
      LOG.warn("TajoMaster failure: " + ee.getMessage());
      resourceTracker = null;
      throw ee;
    }
    return response;
  }

  class StatusUpdaterThread extends Thread {

    public StatusUpdaterThread() {
      super("NodeStatusUpdater");
    }

    private <E> int drain(Collection<? super E> buffer, int numElements,
                          long timeout, TimeUnit unit) throws InterruptedException {

      long deadline = System.nanoTime() + unit.toNanos(timeout);
      int added = 0;
      while (added < numElements) {
        added += heartBeatRequestQueue.drainTo(buffer, numElements - added);
        if (added < numElements) { // not enough elements immediately available; will have to wait
          if (deadline <= System.currentTimeMillis()) {
            break;
          } else {
            synchronized (updaterThread) {
              updaterThread.wait(deadline - System.currentTimeMillis());
              if (deadline > System.nanoTime()) {
                added += heartBeatRequestQueue.drainTo(buffer, numElements - added);
                break;
              }
            }
          }
        }
      }
      return added;
    }

    @Override
    public void run() {
      NodeHeartbeatResponseProto lastResponse = null;
      while (!isStopped && !Thread.interrupted()) {

        try {
          if (lastResponse != null) {
            if (lastResponse.getCommand() == ResponseCommand.NORMAL) {
              List<NodeStatusEvent> events = Lists.newArrayList();
              try {
                drain(events,
                    Math.max(nodeResourceManagerService.getTotalResource().getVirtualCores() / 2, 1),
                    heartBeatInterval, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                break;
              }

              if (!events.isEmpty()) {
                // send last available resource;
                lastResponse = sendHeartbeat(createResourceReport(events.get(events.size() - 1).getResource()));
              } else {
                lastResponse = sendHeartbeat(createHeartBeatReport());
              }

            } else if (lastResponse.getCommand() == ResponseCommand.MEMBERSHIP) {
              lastResponse = sendHeartbeat(createNodeStatusReport());
            } else if (lastResponse.getCommand() == ResponseCommand.ABORT_QUERY) {
              //TODO abort failure queries
            }
          } else {
            lastResponse = sendHeartbeat(createNodeStatusReport());
          }
        } catch (NoSuchMethodException e) {
          LOG.fatal(e.getMessage(), e);
          Runtime.getRuntime().halt(1);
        } catch (ClassNotFoundException e) {
          LOG.fatal(e.getMessage(), e);
          Runtime.getRuntime().halt(1);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          if (!isStopped) {
            synchronized (updaterThread) {
              try {
                updaterThread.wait(heartBeatInterval);
              } catch (InterruptedException e1) {
                // Do Nothing
              }
            }
          }
        }
      }

      LOG.info("Heartbeat Thread stopped.");
    }
  }
}
