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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.worker.event.NodeResourceAllocateEvent;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.apache.tajo.worker.event.NodeResourceEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ResourceProtos.*;
import static org.junit.Assert.*;
public class TestNodeResourceManager {

  @Rule
  public TestName name = new TestName();

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

  private static boolean enableEbCreateFailure(String testName) {
    if (testName.equals("testResourceDeallocateWithEbCreateFailure")) {
      return true;
    } else {
      return false;
    }
  }

  @Before
  public void setup() {
    conf = new TajoConf();
    conf.setBoolVar(TajoConf.ConfVars.$TEST_MODE, true);

    taskMemory = 512;
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES, 4);
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB,
        taskMemory * conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES));
    conf.setIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM, 1);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM, 2);

    dispatcher = new AsyncDispatcher();
    taskDispatcher = new AsyncDispatcher();

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
        return taskExecutor;
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

    taskManager = new MockTaskManager(new Semaphore(0), taskDispatcher, workerContext);
    taskExecutor = new MockTaskExecutor(new Semaphore(0), workerContext);
    resourceManager = new MockNodeResourceManager(new Semaphore(0), dispatcher, workerContext);
    statusUpdater = new MockNodeStatusUpdater(new CountDownLatch(0), workerContext);

    if (enableEbCreateFailure(name.getMethodName())) {
      ((MockTaskManager)taskManager).enableEbCreateFailure();
    }

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

  @Test
  public void testNodeResourceAllocateEvent() throws Exception {
    int requestSize = 4;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponse> callFuture  = new CallFuture<>();
    BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponse responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    // allocated all
    assertEquals(0, responseProto.getCancellationTaskCount());
  }


  @Test
  public void testNodeResourceCancellation() throws Exception {
    int requestSize = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    int overSize = 10;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();
    BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(
        MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize + overSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    BatchAllocationResponse responseProto = callFuture.get();

    assertEquals(overSize, responseProto.getCancellationTaskCount());
  }

  @Test
  public void testNodeResourceDeallocateEvent() throws Exception {
    int requestSize = 4;
    resourceManager.setTaskHandlerEvent(false); //skip task execution

    CallFuture<BatchAllocationResponse> callFuture  = new CallFuture<>();
    BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponse responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    assertEquals(0, responseProto.getCancellationTaskCount());

    //deallocate
    for(TaskAllocationProto allocationRequestProto : requestProto.getTaskRequestList()) {
      // direct invoke handler for testing
      resourceManager.handle(new NodeResourceDeallocateEvent(
          allocationRequestProto.getResource(), NodeResourceEvent.ResourceType.TASK));
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
    final Queue<TaskAllocationProto>
        totalTasks = MockNodeResourceManager.createTaskRequests(ebId, taskMemory, taskSize);


    TaskAllocationProto task = totalTasks.poll();
    BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
    requestProto.addTaskRequest(task);
    requestProto.setExecutionBlockId(ebId.getProto());
    CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();
    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    assertTrue(callFuture.get().getCancellationTaskCount() == 0);
    totalComplete.incrementAndGet();

    // start parallel request
    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);

    List<Future> futureList = Lists.newArrayList();

    for (int i = 0; i < parallelCount; i++) {
      futureList.add(executor.submit(new Runnable() {
            @Override
            public void run() {
              int complete = 0;
              while (true) {
                TaskAllocationProto task = totalTasks.poll();
                if (task == null) break;


                BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
                requestProto.addTaskRequest(task);
                requestProto.setExecutionBlockId(ebId.getProto());

                CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();
                dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
                try {
                  BatchAllocationResponse proto = callFuture.get();
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
              totalComplete.addAndGet(complete);
            }
          })
      );
    }

    for (Future future : futureList) {
      future.get();
    }

    executor.shutdown();
    assertEquals(taskSize, totalComplete.get());
  }

  @Test
  public void testResourceDeallocateWithEbCreateFailure() throws Exception {
    final int taskSize = 10;
    resourceManager.setTaskHandlerEvent(true);

    final ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    final Queue<TaskAllocationProto>
        totalTasks = MockNodeResourceManager.createTaskRequests(ebId, taskMemory, taskSize);

    TaskAllocationProto task = totalTasks.poll();
    BatchAllocationRequest.Builder requestProto = BatchAllocationRequest.newBuilder();
    requestProto.addTaskRequest(task);
    requestProto.setExecutionBlockId(ebId.getProto());
    CallFuture<BatchAllocationResponse> callFuture = new CallFuture<>();
    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    assertTrue(callFuture.get().getCancellationTaskCount() == 0);

    Thread.sleep(2000); // wait for resource deallocation
    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
  }
}
