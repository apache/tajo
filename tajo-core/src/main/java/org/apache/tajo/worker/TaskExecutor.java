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
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.worker.event.NodeResourceDeallocateEvent;
import org.apache.tajo.worker.event.TaskExecutorEvent;
import org.apache.tajo.worker.event.TaskManagerEvent;
import org.apache.tajo.worker.event.TaskStartEvent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class TaskExecutor<T extends TaskContainer> extends ThreadPoolExecutor
    implements EventHandler<TaskExecutorEvent> {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private final TajoConf conf;
  private final TaskManager taskManager;
  private final NodeResourceManager resourceManager;
  private final Map<TaskAttemptId, NodeResource> allocatedResourceMap;
  private final ThreadPoolExecutor fetcherExecutor;

  public TaskExecutor(TajoConf conf, TaskManager taskManager, NodeResourceManager resourceManager) {
    this(conf, conf.getIntVar(ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES), taskManager, resourceManager);

  }

  public TaskExecutor(TajoConf conf, int nThreads, TaskManager taskManager, NodeResourceManager resourceManager) {
    super(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setNameFormat("Task executor #%d").build());
    this.conf = conf;
    this.taskManager = taskManager;
    this.resourceManager = resourceManager;
    this.allocatedResourceMap = Maps.newHashMap();

    int maxFetcherThreads = conf.getIntVar(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM);
    this.fetcherExecutor = new ThreadPoolExecutor(Math.min(nThreads, maxFetcherThreads),
        maxFetcherThreads,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(true));
    LOG.info("Startup TaskExecutor");
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    TaskContainer container = (T) r;
    try {
      container.init();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      container.getContext().fatalError(container.getTask().getId(), e.getMessage());
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    TaskContainer container = (T) r;
    try {

      if (container != null) {
        LOG.error(t.getMessage(), t);
        container.getContext().fatalError(container.getTask().getId(), t.getMessage());
        return;
      }

      try {
        container.stop();
      } catch (Throwable throwable) {
        LOG.error(t.getMessage(), t);
        container.getContext().fatalError(container.getTask().getId(), t.getMessage());
      }
    } finally {
      //FIXME change to TaskStopEvent ?
      resourceManager.getDispatcher().getEventHandler()
          .handle(new NodeResourceDeallocateEvent(allocatedResourceMap.remove(container.getTask().getId())));
    }
  }

  protected ExecutorService getFetcherExecutor() {
    return fetcherExecutor;
  }

  @Override
  public void handle(TaskExecutorEvent event) {

    if(event instanceof TaskStartEvent)
     switch (event.getType()) {
       case START:{
         final TaskStartEvent startEvent = (TaskStartEvent)event;
         allocatedResourceMap.put(startEvent.getTaskId(), startEvent.getAllocatedResource());

//         FIXME
         submit(new TaskContainer(
             taskManager.getExecutionBlockContext(
                 startEvent.getTaskId().getTaskId().getExecutionBlockId()), new Task(){

           @Override
           public void waitForFetch() throws InterruptedException, IOException {

           }

           @Override
           public void run() throws Exception {

           }

           @Override
           public TaskAttemptId getId() {
             return startEvent.getTaskId();
           }
         }));
       }
     }
  }
}
