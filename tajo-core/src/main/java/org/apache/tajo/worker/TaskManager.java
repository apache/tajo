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
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.worker.event.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class TaskManager extends CompositeService implements EventHandler<TaskManagerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskManager.class);

  private final TajoWorker.WorkerContext workerContext;
  private final Map<ExecutionBlockId, ExecutionBlockContext> executionBlockContextMap;
  private final Dispatcher dispatcher;

  private TajoConf tajoConf;
  private TaskExecutor<TaskContainer> taskExecutor;
  private volatile boolean isStopped = false;
  private TaskHistoryCleanerThread taskHistoryCleanerThread;
  private EventHandler rmEventHandler;

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext,
                     EventHandler rmEventHandler) {
    this(dispatcher, workerContext, null, rmEventHandler);
  }

  public TaskManager(Dispatcher dispatcher, TajoWorker.WorkerContext workerContext, TaskExecutor taskExecutor,
                     EventHandler rmEventHandler) {
    super(TaskManager.class.getName());

    this.dispatcher = dispatcher;
    this.workerContext = workerContext;
    this.executionBlockContextMap = Maps.newHashMap();
    this.taskExecutor = taskExecutor;
    this.rmEventHandler = rmEventHandler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    this.tajoConf = (TajoConf)conf;

    if (taskExecutor == null) {
      this.taskExecutor = new TaskExecutor<TaskContainer>(this, rmEventHandler);
    }
    addIfService(dispatcher);
    addService(taskExecutor);

    dispatcher.register(TaskExecutorEvent.EventType.class, taskExecutor);
    dispatcher.register(TaskManagerEvent.EventType.class, this);

    taskHistoryCleanerThread = new TaskHistoryCleanerThread();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    taskHistoryCleanerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;

    for(ExecutionBlockContext context: executionBlockContextMap.values()) {
      context.stop();
    }
    executionBlockContextMap.clear();

    if(taskHistoryCleanerThread != null) {
      taskHistoryCleanerThread.interrupt();
    }

    super.serviceStop();
  }

  protected EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  @Override
  public void handle(TaskManagerEvent event) {
    LOG.info("======================== Processing " + event.getExecutionBlockId() + " of type " + event.getType());
    if (event instanceof ExecutionBlockStartEvent) {
      ExecutionBlockStartEvent startEvent = (ExecutionBlockStartEvent) event;
      ExecutionBlockContext context = executionBlockContextMap.get(startEvent.getExecutionBlockId());

      if(context == null){
        TajoWorkerProtocol.RunExecutionBlockRequestProto request = startEvent.getRequestProto();
        try {
          context = new ExecutionBlockContext(context.getConf(),
              workerContext,
              null, //TODO
              new QueryContext(tajoConf, request.getQueryContext()),
              request.getPlanJson(),
              startEvent.getExecutionBlockId(),
              new WorkerConnectionInfo(request.getQueryMaster()),
              request.getShuffleType());

          context.init();
        } catch (Throwable e) {
          LOG.fatal(e.getMessage(), e);
          throw new RuntimeException(e);
        }
        executionBlockContextMap.put(startEvent.getExecutionBlockId(), context);
      }
    } else if (event instanceof ExecutionBlockStopEvent) {
      ExecutionBlockStopEvent stopEvent = (ExecutionBlockStopEvent) event;
      ExecutionBlockContext executionBlockContext =  executionBlockContextMap.remove(stopEvent.getExecutionBlockId());
      if(executionBlockContext != null){
        try {
          executionBlockContext.getSharedResource().releaseBroadcastCache(event.getExecutionBlockId());
          executionBlockContext.sendShuffleReport();
          workerContext.getTaskHistoryWriter().flushTaskHistories();
        } catch (Exception e) {
          LOG.fatal(e.getMessage(), e);
          throw new RuntimeException(e);
        } finally {
          executionBlockContext.stop();

          /* cleanup intermediate files */
          List<TajoIdProtos.ExecutionBlockIdProto> cleanupList = stopEvent.getCleanupList().getExecutionBlockIdList();
          for (TajoIdProtos.ExecutionBlockIdProto ebId : cleanupList) {
            String inputDir = ExecutionBlockContext.getBaseInputDir(new ExecutionBlockId(ebId)).toString();
            workerContext.cleanup(inputDir);
            String outputDir = ExecutionBlockContext.getBaseOutputDir(new ExecutionBlockId(ebId)).toString();
            workerContext.cleanup(outputDir);
          }
        }
      }
      LOG.info("Stopped execution block:" + event.getExecutionBlockId());
    }
  }

  protected ExecutionBlockContext getExecutionBlockContext(ExecutionBlockId executionBlockId) {
    return executionBlockContextMap.get(executionBlockId);
  }


  /*public Collection<TaskRunner> getTaskRunners() {
    return Collections.unmodifiableCollection(taskRunnerMap.values());
  }

  public Collection<TaskRunnerHistory> getExecutionBlockHistories() {
    return Collections.unmodifiableCollection(taskRunnerHistoryMap.values());
  }

  public TaskRunnerHistory getExcutionBlockHistoryByTaskRunnerId(String taskRunnerId) {
    return taskRunnerHistoryMap.get(taskRunnerId);
  }

  public TaskRunner getTaskRunner(String taskRunnerId) {
    return taskRunnerMap.get(taskRunnerId);
  }

  public Task getTaskByTaskAttemptId(TaskAttemptId taskAttemptId) {
    ExecutionBlockContext context = executionBlockContextMap.get(taskAttemptId.getTaskId().getExecutionBlockId());
    if (context != null) {
      return context.getTask(taskAttemptId);
    }
    return null;
  }

  public TaskHistory getTaskHistoryByTaskAttemptId(TaskAttemptId quAttemptId) {
    synchronized (taskRunnerHistoryMap) {
      for (TaskRunnerHistory history : taskRunnerHistoryMap.values()) {
        TaskHistory taskHistory = history.getTaskHistory(quAttemptId);
        if (taskHistory != null) return taskHistory;
      }
    }

    return null;
  }
  */

  class TaskHistoryCleanerThread extends Thread {
    //TODO if history size is large, the historyMap should remove immediately
    public void run() {
      int expireIntervalTime = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!isStopped) {
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        try {
          long expireTime = System.currentTimeMillis() - expireIntervalTime * 60 * 1000l;
          cleanExpiredFinishedQueryMasterTask(expireTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    private void cleanExpiredFinishedQueryMasterTask(long expireTime) {
//      synchronized(taskRunnerHistoryMap) {
//        List<String> expiredIds = new ArrayList<String>();
//        for(Map.Entry<String, TaskRunnerHistory> entry: taskRunnerHistoryMap.entrySet()) {
//           /* If a task runner are abnormal termination, the finished time will be zero. */
//          long finishedTime = Math.max(entry.getValue().getStartTime(), entry.getValue().getFinishTime());
//          if(finishedTime < expireTime) {
//            expiredIds.add(entry.getKey());
//          }
//        }
//
//        for(String eachId: expiredIds) {
//          taskRunnerHistoryMap.remove(eachId);
//        }
//      }
    }
  }
}
