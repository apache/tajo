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
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.apache.tajo.worker.event.NodeResourceEvent;
import org.apache.tajo.worker.event.TaskStartEvent;
import org.apache.tajo.ResourceProtos.TaskRequestProto;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TaskExecutor uses a number of threads equal to the number of slots available for running tasks on the Worker
 */
public class TaskExecutor extends AbstractService implements EventHandler<TaskStartEvent> {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private final TajoWorker.WorkerContext workerContext;
  private final Map<TaskAttemptId, NodeResource> allocatedResourceMap;
  private final BlockingQueue<Task> taskQueue;
  private final AtomicInteger runningTasks;
  private ExecutorService fetcherThreadPool;
  private ExecutorService threadPool;
  private TajoConf tajoConf;
  private volatile boolean isStopped;

  public TaskExecutor(TajoWorker.WorkerContext workerContext) {
    super(TaskExecutor.class.getName());
    this.workerContext = workerContext;
    this.allocatedResourceMap = Maps.newConcurrentMap();
    this.runningTasks = new AtomicInteger();
    this.taskQueue = new LinkedBlockingQueue<Task>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.tajoConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    int nThreads = this.tajoConf.getIntVar(ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES);
    this.threadPool = Executors.newFixedThreadPool(nThreads,
        new ThreadFactoryBuilder().setNameFormat("Task executor #%d").build());

    int maxFetcherThreads = tajoConf.getIntVar(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM);
    this.fetcherThreadPool = Executors.newFixedThreadPool(nThreads,
        new ThreadFactoryBuilder().setNameFormat("Fetcher executor #%d").build());


    for (int i = 0; i < nThreads; i++) {
      threadPool.submit(new TaskContainer(i, this));
    }

    super.serviceStart();
    LOG.info("Started TaskExecutor[" + nThreads + "], Fetcher executor[" + maxFetcherThreads + "]");
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;

    threadPool.shutdown();
    fetcherThreadPool.shutdown();
    super.serviceStop();
  }

  public boolean isStopped() {
    return isStopped;
  }

  public int getRunningTasks() {
    return runningTasks.get();
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

  protected void stopTask(TaskAttemptId taskId) {
    runningTasks.decrementAndGet();
    releaseResource(taskId);
  }

  @SuppressWarnings("unchecked")
  protected void releaseResource(TaskAttemptId taskId) {
    NodeResource resource =  allocatedResourceMap.remove(taskId);

    if(resource != null) {
      workerContext.getNodeResourceManager().getDispatcher().getEventHandler().handle(
          new NodeResourceDeallocateEvent(resource, NodeResourceEvent.ResourceType.TASK));
    }
  }

  protected ExecutorService getFetcherExecutor() {
    return fetcherThreadPool;
  }


  protected Task createTask(ExecutionBlockContext executionBlockContext,
                            TaskRequestProto taskRequest) throws IOException {
    Task task = null;
    TaskAttemptId taskAttemptId = new TaskAttemptId(taskRequest.getId());
    if (executionBlockContext.getTasks().containsKey(taskAttemptId)) {
      String errorMessage = "Duplicate Task Attempt: " + taskAttemptId;
      LOG.error(errorMessage);
      executionBlockContext.fatalError(taskAttemptId, errorMessage);
    } else {
      task = new TaskImpl(new TaskRequestImpl(taskRequest), executionBlockContext, getFetcherExecutor());
      executionBlockContext.getTasks().put(task.getTaskContext().getTaskId(), task);
    }
    return task;
  }

  @Override
  public void handle(TaskStartEvent event) {

    allocatedResourceMap.put(event.getTaskAttemptId(), event.getAllocatedResource());

    ExecutionBlockContext context = workerContext.getTaskManager().getExecutionBlockContext(
        event.getTaskAttemptId().getTaskId().getExecutionBlockId());

    try {
      Task task = createTask(context, event.getTaskRequest());
      if (task != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Arrival task: " + task.getTaskContext().getTaskId() +
              ", allocated resource: " + event.getAllocatedResource());
        }
        taskQueue.put(task);
        runningTasks.incrementAndGet();
        context.getWorkerContext().getWorkerSystemMetrics()
            .histogram("tasks", "running").update(runningTasks.get());
      } else {
        LOG.warn("Release duplicate task resource: " + event.getAllocatedResource());
        stopTask(event.getTaskAttemptId());
      }
    } catch (InterruptedException e) {
      if (!isStopped) {
        LOG.fatal(e.getMessage(), e);
      }
    } catch (Exception e) {
      stopTask(event.getTaskAttemptId());
    }
  }
}
