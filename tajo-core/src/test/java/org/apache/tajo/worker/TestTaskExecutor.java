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
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.*;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.querymaster.QueryMaster;
import org.apache.tajo.querymaster.QueryMasterManagerService;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.history.HistoryReader;
import org.apache.tajo.util.history.HistoryWriter;
import org.apache.tajo.util.metrics.TajoSystemMetrics;
import org.apache.tajo.worker.event.ExecutionBlockStartEvent;
import org.apache.tajo.worker.event.ExecutionBlockStopEvent;
import org.apache.tajo.worker.event.NodeResourceAllocateEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.junit.Assert.*;

public class TestTaskExecutor {

  private NodeResourceManager resourceManager;
  private NodeStatusUpdater statusUpdater;
  private TaskManager taskManager;
  private TaskExecutor taskExecutor;
  private AsyncDispatcher dispatcher;
  private AsyncDispatcher taskDispatcher;
  private TajoWorker.WorkerContext workerContext;

  private CompositeService service;
  private TajoConf conf;
  private Semaphore barrier;
  private Semaphore resourceManagerBarrier;

  @Before
  public void setup() {
    conf = new TajoConf();
    conf.set(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);
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
    resourceManagerBarrier = new Semaphore(0);
    taskManager = new MockTaskManager(new Semaphore(0), taskDispatcher, workerContext, dispatcher.getEventHandler());
    taskExecutor = new TaskExecutor(barrier, taskManager, dispatcher.getEventHandler());
    resourceManager = new MockNodeResourceManager(resourceManagerBarrier, dispatcher, taskDispatcher.getEventHandler());
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

  @Test
  public void testTaskRequest() throws Exception {
    int requestSize = 1;

    RunExecutionBlockRequestProto.Builder
        ebRequestProto = RunExecutionBlockRequestProto.newBuilder();
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
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, 10, requestSize));

    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    //verify running task
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertEquals(1, taskExecutor.getRunningTasks());
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertEquals(0, taskExecutor.getRunningTasks());
    assertEquals(1, taskExecutor.completeTasks);

    //verify the released resources
    Thread.sleep(100);
    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
  }

  @Test
  public void testTaskException() throws Exception {
    int requestSize = 1;

    RunExecutionBlockRequestProto.Builder
        ebRequestProto = RunExecutionBlockRequestProto.newBuilder();
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

    CallFuture<BatchAllocationResponseProto> callFuture  = new CallFuture<BatchAllocationResponseProto>();
    BatchAllocationRequestProto.Builder requestProto = BatchAllocationRequestProto.newBuilder();
    requestProto.setExecutionBlockId(ebId.getProto());
    requestProto.setExecutionBlockRequest(ebRequestProto.build());

    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
    requestProto.addAllTaskRequest(MockNodeResourceManager.createTaskRequests(ebId, 10, requestSize));

    taskExecutor.throwException.set(true);
    dispatcher.getEventHandler().handle(new NodeResourceAllocateEvent(requestProto.build(), callFuture));

    //verify running task
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertEquals(1, taskExecutor.getRunningTasks());
    assertTrue(barrier.tryAcquire(3, TimeUnit.SECONDS));
    assertEquals(0, taskExecutor.getRunningTasks());
    assertEquals(0, taskExecutor.completeTasks);

    //verify the released resources
    Thread.sleep(100);
    assertEquals(resourceManager.getTotalResource(), resourceManager.getAvailableResource());
  }

  class TaskExecutor extends MockTaskExecutor {
    int completeTasks;
    AtomicBoolean throwException = new AtomicBoolean();

    public TaskExecutor(Semaphore barrier, TaskManager taskManager, EventHandler rmEventHandler) {
      super(barrier, taskManager, rmEventHandler);
    }

    @Override
    protected void stopTask(TaskAttemptId taskId) {
      super.stopTask(taskId);
      super.barrier.release();
    }

    @Override
    protected Task createTask(final ExecutionBlockContext context, TajoWorkerProtocol.TaskRequestProto taskRequest) {
      final TaskAttemptId taskAttemptId = new TaskAttemptId(taskRequest.getId());
      final TaskAttemptContext taskAttemptContext = new TaskAttemptContext(null, context, taskAttemptId, null, null);

      return new Task() {
        @Override
        public void init() throws IOException {

          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        @Override
        public void fetch() {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        @Override
        public void run() throws Exception {
          Thread.sleep(50);

          if(throwException.get()) throw new RuntimeException();

          taskAttemptContext.stop();
          taskAttemptContext.setProgress(1.0f);
          taskAttemptContext.setState(TajoProtos.TaskAttemptState.TA_SUCCEEDED);
          completeTasks++;
        }

        @Override
        public void kill() {

        }

        @Override
        public void abort() {

        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean hasFetchPhase() {
          return false;
        }

        @Override
        public boolean isProgressChanged() {
          return false;
        }

        @Override
        public boolean isStopped() {
          return taskAttemptContext.isStopped();
        }

        @Override
        public void updateProgress() {

        }

        @Override
        public TaskAttemptContext getTaskContext() {
          return taskAttemptContext;
        }

        @Override
        public ExecutionBlockContext getExecutionBlockContext() {
          return context;
        }

        @Override
        public TajoWorkerProtocol.TaskStatusProto getReport() {
          TajoWorkerProtocol.TaskStatusProto.Builder builder = TajoWorkerProtocol.TaskStatusProto.newBuilder();
          builder.setWorkerName("localhost:0");
          builder.setId(taskAttemptContext.getTaskId().getProto())
              .setProgress(taskAttemptContext.getProgress())
              .setState(taskAttemptContext.getState());

          builder.setInputStats(new TableStats().getProto());
          return builder.build();
        }
      };
    }
  }
}
