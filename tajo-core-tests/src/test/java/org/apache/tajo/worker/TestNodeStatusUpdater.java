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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.worker.event.NodeStatusEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static org.apache.tajo.ResourceProtos.NodeHeartbeatRequest;
import static org.junit.Assert.*;

public class TestNodeStatusUpdater {

  private NodeResourceManager resourceManager;
  private MockNodeStatusUpdater statusUpdater;
  private MockTaskManager taskManager;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher taskDispatcher;
  private CompositeService service;
  private TajoConf conf;
  private TajoWorker.WorkerContext workerContext;


  @Before
  public void setup() {
    conf = new TajoConf();
    conf.setBoolVar(TajoConf.ConfVars.$TEST_MODE, true);
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES, 2);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM, 2);

    workerContext = new MockWorkerContext() {
      WorkerConnectionInfo workerConnectionInfo;

      @Override
      public TajoConf getConf() {
        return conf;
      }

      @Override
      public TaskManager getTaskManager() {
        return taskManager;
      }

      @Override
      public TaskExecutor getTaskExecuor() {
        return null;
      }

      @Override
      public NodeResourceManager getNodeResourceManager() {
        return resourceManager;
      }

      @Override
      public WorkerConnectionInfo getConnectionInfo() {
        if (workerConnectionInfo == null) {
          workerConnectionInfo = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
        }
        return workerConnectionInfo;
      }
    };

    conf.setIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_IDLE_INTERVAL, 1000);
    dispatcher = new AsyncDispatcher();
    resourceManager = new NodeResourceManager(dispatcher, workerContext);
    taskDispatcher = new AsyncDispatcher();
    taskManager = new MockTaskManager(new Semaphore(0), taskDispatcher, workerContext) {
      @Override
      public int getRunningTasks() {
        return 0;
      }
    };

    service = new CompositeService("MockService") {
      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        addIfService(dispatcher);
        addIfService(taskDispatcher);
        addIfService(taskManager);
        addIfService(resourceManager);
        addIfService(statusUpdater);
        super.serviceInit(conf);
      }

      @Override
      protected void serviceStop() throws Exception {
        workerContext.getMetrics().stop();
        super.serviceStop();
      }
    };

    service.init(conf);
    service.start();
  }

  @After
  public void tearDown() {
    service.stop();
  }

  @Test(timeout = 20000)
  public void testNodeMembership() throws Exception {
    CountDownLatch barrier = new CountDownLatch(1);
    statusUpdater = new MockNodeStatusUpdater(barrier, workerContext);
    statusUpdater.init(conf);
    statusUpdater.start();

    MockNodeStatusUpdater.MockResourceTracker resourceTracker = statusUpdater.getResourceTracker();
    barrier.await();

    assertTrue(resourceTracker.getTotalResource().containsKey(workerContext.getConnectionInfo().getId()));
    assertEquals(resourceManager.getTotalResource(),
        resourceTracker.getTotalResource().get(workerContext.getConnectionInfo().getId()));

    assertEquals(resourceManager.getAvailableResource(),
        resourceTracker.getAvailableResource().get(workerContext.getConnectionInfo().getId()));
  }

  @Test(timeout = 20000)
  public void testPing() throws Exception {
    CountDownLatch barrier = new CountDownLatch(2);
    statusUpdater = new MockNodeStatusUpdater(barrier, workerContext);
    statusUpdater.init(conf);
    statusUpdater.start();

    MockNodeStatusUpdater.MockResourceTracker resourceTracker = statusUpdater.getResourceTracker();
    barrier.await();

    NodeHeartbeatRequest lastRequest = resourceTracker.getLastRequest();
    assertTrue(lastRequest.hasWorkerId());
    assertTrue(lastRequest.hasAvailableResource());
    assertTrue(lastRequest.hasRunningTasks());
    assertTrue(lastRequest.hasRunningQueryMasters());
    assertFalse(lastRequest.hasTotalResource());
    assertFalse(lastRequest.hasConnectionInfo());
  }

  @Test(timeout = 20000)
  public void testResourceReport() throws Exception {
    CountDownLatch barrier = new CountDownLatch(2);
    statusUpdater = new MockNodeStatusUpdater(barrier, workerContext);
    statusUpdater.init(conf);
    statusUpdater.start();

    assertEquals(0, statusUpdater.getQueueSize());
    for (int i = 0; i < statusUpdater.getQueueingThreshold(); i++) {
      dispatcher.getEventHandler().handle(new NodeStatusEvent(NodeStatusEvent.EventType.REPORT_RESOURCE));
    }
    barrier.await();
    assertEquals(0, statusUpdater.getQueueSize());
  }

  @Test(timeout = 20000)
  public void testFlushResourceReport() throws Exception {
    CountDownLatch barrier = new CountDownLatch(2);
    statusUpdater = new MockNodeStatusUpdater(barrier, workerContext);
    statusUpdater.init(conf);
    statusUpdater.start();

    assertEquals(0, statusUpdater.getQueueSize());
    dispatcher.getEventHandler().handle(new NodeStatusEvent(NodeStatusEvent.EventType.FLUSH_REPORTS));

    barrier.await();
    assertEquals(0, statusUpdater.getQueueSize());
  }
}
