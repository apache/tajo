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
import org.apache.tajo.worker.event.NodeResourceAllocateEvent;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.junit.*;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
public class TestNodeResourceManager {

  private MockNodeResourceManager resourceManager;
  private NodeStatusUpdater statusUpdater;
  private TaskManager taskManager;
  private TaskExecutor taskExecutor;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher taskDispatcher;
  private TajoWorker.WorkerContext workerContext;

  private CompositeService service;
  private int taskMemory;
  private TajoConf conf;

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

    taskManager = new MockTaskManager(new Semaphore(0), taskDispatcher, workerContext, dispatcher.getEventHandler());
    taskExecutor = new MockTaskExecutor(new Semaphore(0), taskManager, dispatcher.getEventHandler());
    resourceManager = new MockNodeResourceManager(new Semaphore(0), dispatcher, taskDispatcher.getEventHandler());
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
    };

    service.init(conf);
    service.start();
  }

  @After
  public void tearDown() {
    service.stop();
  }

  @Test
  public void testNodeResourceAllocateEvent() throws Exception {
    int requestSize = 4;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponseProto responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    // allocated all
    assertEquals(0, responseProto.getCancellationTaskCount());
  }


  @Test
  public void testNodeResourceCancellation() throws Exception {
    int requestSize = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    int overSize = 10;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponseProto> callFuture = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(
        MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize + overSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    BatchAllocationResponseProto responseProto = callFuture.get();

    assertEquals(overSize, responseProto.getCancellationTaskCount());
  }

  @Test
  public void testNodeResourceDeallocateEvent() throws Exception {
    int requestSize = 4;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponseProto responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    assertEquals(0, responseProto.getCancellationTaskCount());

    //deallocate
    for(TaskAllocationRequestProto allocationRequestProto : requestProto.getTaskRequestList()) {
      // direct invoke handler for testing
      resourceManager.handle(new NodeResourceDeallocateEvent(allocationRequestProto.getResource()));
    }

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
  }

  @Test(timeout = 30000)
  public void testParallelRequest() throws Exception {
    final int parallelCount = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES) * 2;
    final int taskSize = 100000;
    resourceManager.setTaskHandlerEvent(true);

    final AtomicInteger totalComplete = new AtomicInteger();
    final AtomicInteger totalCanceled = new AtomicInteger();

    final ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    final Queue<TaskAllocationRequestProto>
        totalTasks = MockNodeResourceManager.createTaskRequests(ebId, taskMemory, taskSize);

    // first request with starting ExecutionBlock
    TajoWorkerProtocol.RunExecutionBlockRequestProto.Builder
        ebRequestProto = TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder();
    ebRequestProto.setExecutionBlockId(ebId.getProto())
        .setQueryMaster(workerContext.getConnectionInfo().getProto())
        .setNodeId(workerContext.getConnectionInfo().getHost() + ":" +
            workerContext.getConnectionInfo().getQueryMasterPort())
        .setContainerId("test")
        .setQueryContext(new QueryContext(conf).getProto())
        .setPlanJson("test")
        .setShuffleType(PlanProto.ShuffleType.HASH_SHUFFLE);

    TaskAllocationRequestProto task = totalTasks.poll();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    requestProto.addTaskRequest(task);
    requestProto.setExecutionBlockId(ebId.getProto());
    requestProto.setExecutionBlockRequest(ebRequestProto.build());
    CallFuture<BatchAllocationResponseProto> callFuture = new CallFuture<BatchAllocationResponseProto>();
    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    assertTrue(callFuture.get().getCancellationTaskCount() == 0);
    totalComplete.incrementAndGet();

    // start parallel request
    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);
    List<Future> futureList = Lists.newArrayList();

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < parallelCount; i++) {
      futureList.add(executor.submit(new Runnable() {
            @Override
            public void run() {
              int complete = 0;
              while (true) {
                TaskAllocationRequestProto task = totalTasks.poll();
                if (task == null) break;


                BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
                requestProto.addTaskRequest(task);
                requestProto.setExecutionBlockId(ebId.getProto());

                CallFuture<BatchAllocationResponseProto> callFuture = new CallFuture<BatchAllocationResponseProto>();
                dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
                try {
                  BatchAllocationResponseProto proto = callFuture.get();
                  if (proto.getCancellationTaskCount() > 0) {
                    totalTasks.addAll(proto.getCancellationTaskList());
                    totalCanceled.addAndGet(proto.getCancellationTaskCount());
                  } else {
                    complete++;
                  }
                } catch (Exception e) {
                  fail(e.getMessage());
                }
              }
              System.out.println(Thread.currentThread().getName() + " complete requests: " + complete);
              totalComplete.addAndGet(complete);
            }
          })
      );
    }

    for (Future future : futureList) {
      future.get();
    }

    System.out.println(parallelCount + " Thread, completed requests: " + totalComplete.get() + ", canceled requests:"
        + totalCanceled.get() + ", " + +(System.currentTimeMillis() - startTime) + " ms elapsed");
    executor.shutdown();
    assertEquals(taskSize, totalComplete.get());
  }
}
