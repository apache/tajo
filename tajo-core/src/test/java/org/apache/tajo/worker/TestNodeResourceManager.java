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
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.tajo.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.resource.NodeResources;
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

  private NodeResourceManager resourceManager;
  private MockNodeStatusUpdater statusUpdater;
  private AsyncDispatcher dispatcher;
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
    dispatcher.init(conf);
    dispatcher.start();

    resourceManager = new NodeResourceManager(dispatcher);
    resourceManager.init(conf);
    resourceManager.start();

    WorkerConnectionInfo worker = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);
    statusUpdater = new MockNodeStatusUpdater(new CountDownLatch(0), worker, resourceManager);
    statusUpdater.init(conf);
    statusUpdater.start();
  }

  @After
  public void tearDown() {
    resourceManager.stop();
    statusUpdater.stop();
    dispatcher.stop();
  }

  @Test
  public void testNodeResourceAllocateEvent() throws Exception {
    int requestSize = 4;

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(createTaskRequests(taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponseProto responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    assertEquals(0, responseProto.getCancellationTaskCount());
    assertEquals(requestSize, resourceManager.getAllocatedSize());
  }


  @Test
  public void testNodeResourceCancellation() throws Exception {
    int requestSize = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    int overSize = 10;

    CallFuture<BatchAllocationResponseProto> callFuture = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(createTaskRequests(taskMemory, requestSize + overSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));
    BatchAllocationResponseProto responseProto = callFuture.get();

    assertEquals(overSize, responseProto.getCancellationTaskCount());
    assertEquals(requestSize, resourceManager.getAllocatedSize());
  }

  @Test
  public void testNodeResourceDeallocateEvent() throws Exception {
    int requestSize = 4;

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    requestProto.setExecutionBlockId(ebId.getProto());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(createTaskRequests(taskMemory, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    BatchAllocationResponseProto responseProto = callFuture.get();
    assertNotEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    assertEquals(0, responseProto.getCancellationTaskCount());
    assertEquals(requestSize, resourceManager.getAllocatedSize());

    //deallocate
    for(TaskAllocationRequestProto allocationRequestProto : requestProto.getTaskRequestList()) {
      // direct invoke handler for testing
      resourceManager.handle(new NodeResourceDeallocateEvent(allocationRequestProto.getResource()));
    }
    assertEquals(0, resourceManager.getAllocatedSize());
    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
  }

  @Test(timeout = 30000)
  public void testParallelRequest() throws Exception {
    final int parallelCount = conf.getIntVar(TajoConf.ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES) * 2;
    final int taskSize = 100000;
    final AtomicInteger totalComplete = new AtomicInteger();
    final AtomicInteger totalCanceled = new AtomicInteger();

    final ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    final Queue<TaskAllocationRequestProto> totalTasks = createTaskRequests(taskMemory, taskSize);

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
                    dispatcher.getEventHandler().handle(new NodeResourceDeallocateEvent(task.getResource()));
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

  protected static Queue<TaskAllocationRequestProto> createTaskRequests(int memory, int size) {
    Queue<TaskAllocationRequestProto> requestProtoList = new LinkedBlockingQueue<TaskAllocationRequestProto>();
    for (int i = 0; i < size; i++) {

      ExecutionBlockId nullStage = QueryIdFactory.newExecutionBlockId(QueryIdFactory.NULL_QUERY_ID, 0);
      TaskAttemptId taskAttemptId = QueryIdFactory.newTaskAttemptId(QueryIdFactory.newTaskId(nullStage, i), 0);

      TajoWorkerProtocol.TaskRequestProto.Builder builder =
          TajoWorkerProtocol.TaskRequestProto.newBuilder();
      builder.setId(taskAttemptId.getProto());
      builder.setShouldDie(true);
      builder.setOutputTable("");
      builder.setPlan(PlanProto.LogicalNodeTree.newBuilder());
      builder.setClusteredOutput(false);


      requestProtoList.add(TaskAllocationRequestProto.newBuilder()
          .setResource(NodeResources.createResource(memory).getProto())
          .setTaskRequest(builder.build()).build());
    }
    return requestProtoList;
  }
}
