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
import org.apache.tajo.resource.DefaultResourceCalculator;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.rpc.AsyncRpcClient;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.RpcParameterFactory;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.NodeStatusEvent;

import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.ResourceProtos.*;

/**
 * It periodically sends heartbeat to {@link org.apache.tajo.master.rm.TajoResourceTracker} via asynchronous rpc.
 */
public class NodeStatusUpdater extends AbstractService implements EventHandler<NodeStatusEvent> {

  private final static Log LOG = LogFactory.getLog(NodeStatusUpdater.class);

  private TajoConf systemConf;
  private StatusUpdaterThread updaterThread;
  private volatile boolean isStopped;
  private int heartBeatInterval;
  private int nextHeartBeatInterval;
  private BlockingQueue<NodeStatusEvent> heartBeatRequestQueue;
  private final TajoWorker.WorkerContext workerContext;
  private AsyncRpcClient rmClient;
  private Properties rpcParams;
  private ServiceTracker serviceTracker;
  private TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService.Interface resourceTracker;
  private int queueingThreshold;

  public NodeStatusUpdater(TajoWorker.WorkerContext workerContext) {
    super(NodeStatusUpdater.class.getSimpleName());
    this.workerContext = workerContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    this.rpcParams = RpcParameterFactory.get(this.systemConf);
    this.heartBeatRequestQueue = Queues.newLinkedBlockingQueue();
    this.serviceTracker = ServiceTrackerFactory.get(systemConf);
    this.workerContext.getNodeResourceManager().getDispatcher().register(NodeStatusEvent.EventType.class, this);
    this.heartBeatInterval = systemConf.getIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_IDLE_INTERVAL);
    this.updaterThread = new StatusUpdaterThread();
    this.updaterThread.setName("NodeStatusUpdater");
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    DefaultResourceCalculator calculator = new DefaultResourceCalculator();
    int maxContainer = calculator.computeAvailableContainers(workerContext.getNodeResourceManager().getTotalResource(),
        NodeResources.createResource(systemConf.getIntVar(TajoConf.ConfVars.TASK_RESOURCE_MINIMUM_MEMORY)));

    // if resource changed over than 30%, send reports
    float queueingRate = systemConf.getFloatVar(TajoConf.ConfVars.WORKER_HEARTBEAT_QUEUE_THRESHOLD_RATE);
    this.queueingThreshold = Math.max((int) Math.floor(maxContainer * queueingRate), 1);
    LOG.info("Queueing threshold:" + queueingThreshold);

    updaterThread.start();
    super.serviceStart();
    LOG.info("NodeStatusUpdater started.");
  }

  @Override
  public void serviceStop() throws Exception {
    this.isStopped = true;
    synchronized (updaterThread) {
      updaterThread.interrupt();
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

  public int getQueueingThreshold() {
    return queueingThreshold;
  }

  private NodeHeartbeatRequest.Builder createResourceReport() {
    NodeHeartbeatRequest.Builder requestProto = NodeHeartbeatRequest.newBuilder();
    requestProto.setWorkerId(workerContext.getConnectionInfo().getId());
    requestProto.setAvailableResource(workerContext.getNodeResourceManager().getAvailableResource().getProto());
    requestProto.setRunningTasks(workerContext.getTaskManager().getRunningTasks());
    requestProto.setRunningQueryMasters(workerContext.getNodeResourceManager().getRunningQueryMasters());

    return requestProto;
  }

  private NodeHeartbeatRequest.Builder createNodeStatusReport() {
    NodeHeartbeatRequest.Builder requestProto = createResourceReport();
    requestProto.setTotalResource(workerContext.getNodeResourceManager().getTotalResource().getProto());
    requestProto.setConnectionInfo(workerContext.getConnectionInfo().getProto());

    //TODO set node status to requestProto.setStatus()
    return requestProto;
  }

  protected TajoResourceTrackerProtocol.TajoResourceTrackerProtocolService.Interface newStub()
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
    RpcClientManager.cleanup(rmClient);

    RpcClientManager rpcManager = RpcClientManager.getInstance();
    rmClient = rpcManager.newClient(serviceTracker.getResourceTrackerAddress(),
        TajoResourceTrackerProtocol.class, true, rpcParams);
    return rmClient.getStub();
  }

  protected NodeHeartbeatResponse sendHeartbeat(NodeHeartbeatRequest requestProto)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException, ExecutionException {
    if (resourceTracker == null) {
      resourceTracker = newStub();
    }

    NodeHeartbeatResponse response = null;
    try {
      CallFuture<NodeHeartbeatResponse> callBack = new CallFuture<>();

      resourceTracker.nodeHeartbeat(callBack.getController(), requestProto, callBack);
      response = callBack.get();
    } catch (InterruptedException e) {
      LOG.warn(e.getMessage());
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
        if (added < numElements) { // not enough elements immediately available; will have to wait
          NodeStatusEvent e = heartBeatRequestQueue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
          if (e == null) {
            break; // we already waited enough, and there are no more elements in sight
          }
          buffer.add(e);
          added++;

          if (e.getType() == NodeStatusEvent.EventType.FLUSH_REPORTS) {
            added += heartBeatRequestQueue.drainTo(buffer, numElements - added);
            break;
          }
        }
      }
      return added;
    }

    /* Node sends a heartbeats with its resource and status periodically to master. */
    @Override
    public void run() {
      NodeHeartbeatResponse lastResponse = null;
      while (!isStopped && !Thread.interrupted()) {

        try {
          if (lastResponse != null) {
            if (lastResponse.getCommand() == ResponseCommand.NORMAL) {
              List<NodeStatusEvent> events = Lists.newArrayList();

              if(lastResponse.hasHeartBeatInterval()) {
                nextHeartBeatInterval = lastResponse.getHeartBeatInterval();
              } else {
                nextHeartBeatInterval = heartBeatInterval;
              }

              try {
                /* batch update to ResourceTracker */
                drain(events, queueingThreshold, nextHeartBeatInterval, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                break;
              }

              // send current available resource;
              lastResponse = sendHeartbeat(createResourceReport().build());

            } else if (lastResponse.getCommand() == ResponseCommand.MEMBERSHIP) {
              // Membership changed
              lastResponse = sendHeartbeat(createNodeStatusReport().build());
            } else if (lastResponse.getCommand() == ResponseCommand.ABORT_QUERY) {
              //TODO abort failure queries
            }
          } else {
            // Node registration on startup
            lastResponse = sendHeartbeat(createNodeStatusReport().build());
          }
        } catch (NoSuchMethodException nsme) {
          LOG.fatal(nsme.getMessage(), nsme);
          Runtime.getRuntime().halt(-1);
        } catch (ClassNotFoundException cnfe) {
          LOG.fatal(cnfe.getMessage(), cnfe);
          Runtime.getRuntime().halt(-1);
        } catch (Exception e) {
          if (isStopped) {
              break;
          } else {
            LOG.error(e.getMessage(), e);
          }
        }
      }

      LOG.info("Heartbeat Thread stopped.");
    }
  }
}
