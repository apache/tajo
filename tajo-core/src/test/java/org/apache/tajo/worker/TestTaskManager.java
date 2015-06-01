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
import org.apache.tajo.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.event.ExecutionBlockStartEvent;
import org.apache.tajo.worker.event.ExecutionBlockStopEvent;
import org.apache.tajo.worker.event.NodeResourceAllocateEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.junit.Assert.*;

public class TestTaskManager {

  private NodeResourceManager resourceManager;
  private NodeStatusUpdater statusUpdater;
  private TaskManager taskManager;
  private TaskExecutor taskExecutor;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher taskDispatcher;
  private TajoWorker.WorkerContext workerContext;

  private CompositeService service;
  private int taskMemory;
  private TajoConf conf;
  private Semaphore barrier;

  @Before
  public void setup() {
    conf = new TajoConf();
    conf.set(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);

    taskMemory = 512;
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES, 4);
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB,
        taskMemory * conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES));
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISKS_NUM, 4);
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM, 1);

    dispatcher = new AsyncDispatcher();
    taskDispatcher = new AsyncDispatcher();

    workerContext = new MockWorkerContext() {
      WorkerConnectionInfo workerConnectionInfo;

      @Override
      public TajoConf getConf() {
        return conf;
      }

      @Override
      public WorkerConnectionInfo getConnectionInfo() {
        if (workerConnectionInfo == null) {
          workerConnectionInfo = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
        }
        return workerConnectionInfo;
      }
    };
    barrier = new Semaphore(0);
    taskManager = new MockTaskManager(barrier, taskDispatcher, workerContext, dispatcher.getEventHandler());
    taskExecutor = new MockTaskExecutor(new Semaphore(0), taskManager, dispatcher.getEventHandler());
    resourceManager = new NodeResourceManager(dispatcher, taskDispatcher.getEventHandler());
    statusUpdater = new MockNodeStatusUpdater(new CountDownLatch(0), workerContext, resourceManager);

    service = new CompositeService("MockService") {
      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        addIfService(dispatcher);
        addIfService(taskDispatcher);
        addIfService(taskManager);
        addIfService(taskExecutor);
        addIfService(resourceManager);
        addIfService(statusUpdater);
        super.serviceInit(conf);
      }


      @Override
      protected void serviceStop() throws Exception {
        workerContext.getWorkerSystemMetrics().stop();
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

  @Test(timeout = 10000)
  public void testExecutionBlockStart() throws Exception {
    int requestSize = 1;

    TajoWorkerProtocol.RunExecutionBlockRequestProto.Builder
        ebRequestProto = TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder();
    QueryId qid = LocalTajoTestingUtility.newQueryId();
    ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(qid, 1);

    ebRequestProto.setExecutionBlockId(ebId.getProto())
        .setQueryMaster(workerContext.getConnectionInfo().getProto())
        .setNodeId(workerContext.getConnectionInfo().getHost() + ":"
            + workerContext.getConnectionInfo().getQueryMasterPort())
        .setContainerId("test")
        .setQueryContext(new QueryContext(conf).getProto())
        .setPlanJson("test")
        .setShuffleType(PlanProto.ShuffleType.HASH_SHUFFLE);

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    requestProto.setExecutionBlockId(ebId.getProto());
    requestProto.setExecutionBlockRequest(ebRequestProto.build());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertNotNull(taskManager.getExecutionBlockContext(ebId));
    assertEquals(ebId, taskManager.getExecutionBlockContext(ebId).getExecutionBlockId());
  }

  @Test(timeout = 10000)
  public void testExecutionBlockStop() throws Exception {

    TajoWorkerProtocol.RunExecutionBlockRequestProto.Builder
        ebRequestProto = TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder();
    QueryId qid = LocalTajoTestingUtility.newQueryId();
    ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(qid, 1);

    ebRequestProto.setExecutionBlockId(ebId.getProto())
        .setQueryMaster(workerContext.getConnectionInfo().getProto())
        .setNodeId(workerContext.getConnectionInfo().getHost()+":"
            + workerContext.getConnectionInfo().getQueryMasterPort())
        .setContainerId("test")
        .setQueryContext(new QueryContext(conf).getProto())
        .setPlanJson("test")
        .setShuffleType(PlanProto.ShuffleType.HASH_SHUFFLE);

    taskDispatcher.getEventHandler().handle(new ExecutionBlockStartEvent(ebRequestProto.build()));
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertNotNull(taskManager.getExecutionBlockContext(ebId));
    assertEquals(ebId, taskManager.getExecutionBlockContext(ebId).getExecutionBlockId());

    ExecutionBlockListProto.Builder ebList = ExecutionBlockListProto.newBuilder();
    taskDispatcher.getEventHandler().handle(new ExecutionBlockStopEvent(ebId.getProto(), ebList.build()));
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertNull(taskManager.getExecutionBlockContext(ebId));
  }
}
