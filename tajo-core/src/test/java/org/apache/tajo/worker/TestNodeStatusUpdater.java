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

import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoResourceTrackerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.event.NodeStatusEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import static org.junit.Assert.*;

public class TestNodeStatusUpdater {

  private NodeResourceManager resourceManager;
  private MockNodeStatusUpdater statusUpdater;
  private AsyncDispatcher dispatcher;
  private TajoConf conf;

  @Before
  public void setup() {
    conf = new TajoConf();
    conf.set(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);

    conf.setIntVar(TajoConf.ConfVars.WORKER_HEARTBEAT_INTERVAL, 1000);
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();

    resourceManager = new NodeResourceManager(dispatcher);
    resourceManager.init(conf);
    resourceManager.start();
  }

  @After
  public void tearDown() {
    resourceManager.stop();
    if (statusUpdater != null) statusUpdater.stop();
    dispatcher.stop();
  }

  @Test(timeout = 20000)
  public void testNodeMembership() throws Exception {
    CountDownLatch barrier = new CountDownLatch(1);
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    statusUpdater = new MockNodeStatusUpdater(barrier, worker, resourceManager);
    statusUpdater.init(conf);
    statusUpdater.start();

    MockNodeStatusUpdater.MockResourceTracker resourceTracker = statusUpdater.getResourceTracker();
    barrier.await();

    assertTrue(resourceTracker.getTotalResource().containsKey(worker.getId()));
    assertEquals(resourceManager.getTotalResource(),
        resourceTracker.getTotalResource().get(worker.getId()));

    assertEquals(resourceManager.getAvailableResource(),
        resourceTracker.getAvailableResource().get(worker.getId()));
  }

  @Test(timeout = 20000)
  public void testPing() throws Exception {
    CountDownLatch barrier = new CountDownLatch(2);
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    statusUpdater = new MockNodeStatusUpdater(barrier, worker, resourceManager);
    statusUpdater.init(conf);
    statusUpdater.start();

    MockNodeStatusUpdater.MockResourceTracker resourceTracker = statusUpdater.getResourceTracker();
    barrier.await();

    TajoResourceTrackerProtocol.NodeHeartbeatRequestProto lastRequest = resourceTracker.getLastRequest();
    assertTrue(lastRequest.hasWorkerId());
    assertFalse(lastRequest.hasAvailableResource());
    assertFalse(lastRequest.hasTotalResource());
    assertFalse(lastRequest.hasConnectionInfo());
  }

  @Test(timeout = 20000)
  public void testResourceReport() throws Exception {
    CountDownLatch barrier = new CountDownLatch(2);
    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    statusUpdater = new MockNodeStatusUpdater(barrier, worker, resourceManager);
    statusUpdater.init(conf);
    statusUpdater.start();

    for (int i = 0; i < statusUpdater.getQueueingLimit(); i++) {
      dispatcher.getEventHandler().handle(new NodeStatusEvent(NodeStatusEvent.EventType.REPORT_RESOURCE,
          resourceManager.getAvailableResource()));
    }
    barrier.await();
    assertEquals(0, statusUpdater.getQueueSize());
  }
}
