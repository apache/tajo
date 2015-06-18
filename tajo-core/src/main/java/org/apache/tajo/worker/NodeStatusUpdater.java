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
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.rpc.*;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.worker.event.NodeStatusEvent;

import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.*;

/**
 * It periodically sends heartbeat to {@link org.apache.tajo.master.rm.TajoResourceTracker} via asynchronous rpc.
 */
public class NodeStatusUpdater extends AbstractService implements EventHandler<NodeStatusEvent> {

  private final static Log LOG = LogFactory.getLog(NodeStatusUpdater.class);

  private TajoConf tajoConf;
  private StatusUpdaterThread updaterThread;
  private volatile boolean isStopped;
  private volatile long heartBeatInterval;
  private BlockingQueue<NodeStatusEvent> heartBeatRequestQueue;
  private final TajoWorker.WorkerContext workerContext;
  private final NodeResourceManager nodeResourceManager;
  private AsyncRpcClient rmClient;
  private ServiceTracker serviceTracker;
  private TajoResourceTrackerProtocolService.Interface resourceTracker;
  private int queueingLimit;

  public NodeStatusUpdater(TajoWorker.WorkerContext workerContext, NodeResourceManager resourceManager) {
    super(NodeStatusUpdater.class.getSimpleName());
    this.workerContext = workerContext;
    this.nodeResourceManager = resourceManager;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.tajoConf = (TajoConf) conf;
    this.heartBeatRequestQueue = Queues.newLinkedBlockingQueue();
    this.serviceTracker = ServiceTrackerFactory.get(tajoConf);
    this.nodeResourceManager.getDispatcher().register(NodeStatusEvent.EventType.class, this);
    this.heartBeatInterval = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_INTERVAL);
    this.updaterThread = new StatusUpdaterThread();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    // if resource changed over than 50%, send reports
    this.queueingLimit = nodeResourceManager.getTotalResource().getVirtualCores() / 2;

    updaterThread.start();
    super.serviceStart();
    LOG.info("NodeStatusUpdater started.");
  }

  @Override
  public void serviceStop() throws Exception {
    this.isStopped = true;

    synchronized (updaterThread) {
      updaterThread.interrupt();
      updaterThread.join();
    }
    super.serviceStop();
    LOG.info("NodeStatusUpdater stopped.");
  }

  @Override
  public void handle(NodeStatusEvent event) {
    heartBeatRequestQueue.add(event);
  }

  public int getQueueSize() {
    return heartBeatRequestQueue.size();
  }

  public int getQueueingLimit() {
    return queueingLimit;
  }

  private NodeHeartbeatRequestProto createResourceReport(NodeResource resource) {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setAvailableResource(resource.getProto());
    requestProto.setWorkerId(workerContext.getConnectionInfo().getId());
    return requestProto.build();
  }

  private NodeHeartbeatRequestProto createHeartBeatReport() {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setWorkerId(workerContext.getConnectionInfo().getId());
    return requestProto.build();
  }

  private NodeHeartbeatRequestProto createNodeStatusReport() {
    NodeHeartbeatRequestProto.Builder requestProto = NodeHeartbeatRequestProto.newBuilder();
    requestProto.setTotalResource(nodeResourceManager.getTotalResource().getProto());
    requestProto.setAvailableResource(nodeResourceManager.getAvailableResource().getProto());
    requestProto.setWorkerId(workerContext.getConnectionInfo().getId());
    requestProto.setConnectionInfo(workerContext.getConnectionInfo().getProto());

    //TODO set node status to requestProto.setStatus()
    return requestProto.build();
  }

  protected TajoResourceTrackerProtocolService.Interface newStub()
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
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

    private int drain(Collection<NodeStatusEvent> buffer, int numElements,
                          long timeout, TimeUnit unit) throws InterruptedException {

      long deadline = System.nanoTime() + unit.toNanos(timeout);
      int added = 0;
      while (added < numElements) {
        added += heartBeatRequestQueue.drainTo(buffer, numElements - added);
        if (added < numElements) { // not enough elements immediately available; will have to wait
          NodeStatusEvent e = heartBeatRequestQueue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
          if (e == null) {
            break; // we already waited enough, and there are no more elements in sight
          }
          buffer.add(e);
          added++;

          if (e.getType() == NodeStatusEvent.EventType.FLUSH_REPORTS) {
            break;
          }
        }
      }
      return added;
    }

    /* Node sends a heartbeats with its resource and status periodically to master. */
    @Override
    public void run() {
      NodeHeartbeatResponseProto lastResponse = null;
      while (!isStopped && !Thread.interrupted()) {

        try {
          if (lastResponse != null) {
            if (lastResponse.getCommand() == ResponseCommand.NORMAL) {
              List<NodeStatusEvent> events = Lists.newArrayList();
              try {
                /* batch update to ResourceTracker */
                drain(events, Math.max(queueingLimit, 1), heartBeatInterval, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                break;
              }

              if (!events.isEmpty()) {
                // send current available resource;
                lastResponse = sendHeartbeat(createResourceReport(nodeResourceManager.getAvailableResource()));
              } else {
                // send ping;
                lastResponse = sendHeartbeat(createHeartBeatReport());
              }

            } else if (lastResponse.getCommand() == ResponseCommand.MEMBERSHIP) {
              // Membership changed
              lastResponse = sendHeartbeat(createNodeStatusReport());
            } else if (lastResponse.getCommand() == ResponseCommand.ABORT_QUERY) {
              //TODO abort failure queries
            }
          } else {
            // Node registration on startup
            lastResponse = sendHeartbeat(createNodeStatusReport());
          }
        } catch (NoSuchMethodException nsme) {
          LOG.fatal(nsme.getMessage(), nsme);
          Runtime.getRuntime().halt(-1);
        } catch (ClassNotFoundException cnfe) {
          LOG.fatal(cnfe.getMessage(), cnfe);
          Runtime.getRuntime().halt(-1);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          if (!isStopped) {
            synchronized (updaterThread) {
              try {
                updaterThread.wait(heartBeatInterval);
              } catch (InterruptedException ie) {
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
