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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.TaskRequest;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.worker.event.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor<T extends TaskContainer> extends AbstractService
    implements EventHandler<TaskExecutorEvent> {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private final TaskManager taskManager;
  private final EventHandler rmEventHandler;
  private final Map<TaskAttemptId, NodeResource> allocatedResourceMap;
  private final BlockingQueue<Task> taskQueue;
  private final AtomicInteger runningTasks;
  private ThreadPoolExecutor fetcherExecutor;
  private ExecutorService threadPool;
  private TajoConf tajoConf;

  public TaskExecutor(TaskManager taskManager, EventHandler rmEventHandler) {
    super(TaskExecutor.class.getName());
    this.taskManager = taskManager;
    this.rmEventHandler = rmEventHandler;
    this.allocatedResourceMap = Maps.newConcurrentMap();
    this.runningTasks = new AtomicInteger();
    this.taskQueue = new LinkedBlockingQueue<Task>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.tajoConf = (TajoConf) conf;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    int nThreads = this.tajoConf.getIntVar(ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    this.threadPool = Executors.newFixedThreadPool(nThreads,
        new ThreadFactoryBuilder().setNameFormat("Task executor #%d").build());

    int maxFetcherThreads = tajoConf.getIntVar(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM);
    this.fetcherExecutor = new ThreadPoolExecutor(Math.min(nThreads, maxFetcherThreads),
        maxFetcherThreads,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(true));


    for (int i = 0; i < nThreads; i++) {
      threadPool.submit(new TaskContainer(i, this));
    }

    super.serviceStart();
    LOG.info("Started TaskExecutor[" + nThreads + "], Fetcher executor[" + maxFetcherThreads + "]");
  }

  /**
   * This will block until a task is available.
   */
  protected Task getNextTask() {
    Task task = null;
    try {
      task = taskQueue.take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return task;
  }


  protected void beforeExecute(Thread t, Runnable r) {
    System.out.println(r);
    Future<TaskContainer> future = (Future<TaskContainer>) r;

    TaskContainer container = null;
//    try {
//      container = future.get();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    } catch (ExecutionException e) {
//      e.printStackTrace();
//    }
//    try {
//      container.init();
//    } catch (IOException e) {
//      LOG.error(e.getMessage(), e);
//      container.getTask().abort();
//      container.getContext().fatalError(container.getTask().getId(), e.getMessage());
    //}
  }


  protected void afterExecute(Runnable r, Throwable t) {
    TaskContainer container = null;
    try {
      container = ((Future<TaskContainer>) r).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    try {

      if (t != null) {
        LOG.error(t.getMessage(), t);
       // container.getContext().fatalError(container.getTask().getId(), t.getMessage());
        return;
      }

      try {
       // container.stop();
      } catch (Throwable throwable) {
        LOG.error(throwable.getMessage(), throwable);
        //container.getContext().fatalError(container.getTask().getId(), throwable.getMessage());
      }
    } finally {

    }
  }

  @SuppressWarnings("unchecked")
  protected void stopTask(Task task) {
    //FIXME change to TaskStopEvent ?
    if(task != null) {
      rmEventHandler
          .handle(new NodeResourceDeallocateEvent(allocatedResourceMap.remove(task.getTaskContext().getTaskId())));
      runningTasks.decrementAndGet();
    }

  }
  protected ExecutorService getFetcherExecutor() {
    return fetcherExecutor;
  }

  /*FIXME*/
  protected Task createTask(final ExecutionBlockContext context, TajoWorkerProtocol.TaskRequestProto taskRequest){

    final TaskAttemptId taskAttemptId = new TaskAttemptId(taskRequest.getId());
//    if (getContext().getTasks().containsKey(taskAttemptId)) {
//      LOG.error("Duplicate Task Attempt: " + taskAttemptId);
//      fatalError(qmClientService, taskAttemptId, "Duplicate Task Attempt: " + taskAttemptId);
//      continue;
//    }
     final TaskAttemptContext taskAttemptContext = new TaskAttemptContext(null,context,taskAttemptId, null, null);
    //LOG.info("Initializing: " + taskAttemptId);
    return new Task() {
      @Override
      public void init() throws IOException {

      }

      @Override
      public void fetch() {

      }

      @Override
      public void run() throws Exception {

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
        return null;
      }
    };
  }

  @Override
  public void handle(TaskExecutorEvent event) {

    if(event instanceof TaskStartEvent)
     switch (event.getType()) {
       case START:{
         final TaskStartEvent startEvent = (TaskStartEvent)event;
         allocatedResourceMap.put(startEvent.getTaskId(), startEvent.getAllocatedResource());

         ExecutionBlockContext context = taskManager.getExecutionBlockContext(
             startEvent.getTaskId().getTaskId().getExecutionBlockId());

         try {
           taskQueue.put(createTask(context, startEvent.getTaskRequest()));
           runningTasks.incrementAndGet();
         } catch (InterruptedException e) {
           LOG.fatal(e.getMessage(), e);
         }
       }
     }
  }
}
