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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.worker.event.TaskRunnerEvent;
import org.apache.tajo.worker.event.TaskRunnerStartEvent;
import org.apache.tajo.worker.event.TaskRunnerStopEvent;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunnerManager extends CompositeService implements EventHandler<TaskRunnerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskRunnerManager.class);

  private final ConcurrentMap<ExecutionBlockId, ExecutionBlockContext> executionBlockContextMap = Maps.newConcurrentMap();
  private final ConcurrentMap<String, TaskRunner> taskRunnerMap = Maps.newConcurrentMap();
  private final ConcurrentMap<String, TaskRunnerHistory> taskRunnerHistoryMap = Maps.newConcurrentMap();
  private TajoWorker.WorkerContext workerContext;
  private TajoConf tajoConf;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private FinishedTaskCleanThread finishedTaskCleanThread;
  private Dispatcher dispatcher;

  public TaskRunnerManager(TajoWorker.WorkerContext workerContext, Dispatcher dispatcher) {
    super(TaskRunnerManager.class.getName());

    this.workerContext = workerContext;
    this.dispatcher = dispatcher;
  }

  public TajoWorker.WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public void init(Configuration conf) {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    tajoConf = (TajoConf)conf;
    dispatcher.register(TaskRunnerEvent.EventType.class, this);
    super.init(tajoConf);
  }

  @Override
  public void start() {
    finishedTaskCleanThread = new FinishedTaskCleanThread();
    finishedTaskCleanThread.start();
    super.start();
  }

  @Override
  public void stop() {
    if(stop.getAndSet(true)) {
      return;
    }

    synchronized(taskRunnerMap) {
      for(TaskRunner eachTaskRunner: taskRunnerMap.values()) {
        if(!eachTaskRunner.isStopped()) {
          eachTaskRunner.stop();
        }
      }
    }
    for(ExecutionBlockContext context: executionBlockContextMap.values()) {
      context.stop();
    }

    if(finishedTaskCleanThread != null) {
      finishedTaskCleanThread.interrupt();
    }

    super.stop();
  }

  public void stopTaskRunner(String id) {
    LOG.info("Stop Task:" + id);
    TaskRunner taskRunner = taskRunnerMap.remove(id);
    taskRunner.stop();
  }

  public Collection<TaskRunner> getTaskRunners() {
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

  public int getNumTasks() {
    return taskRunnerMap.size();
  }

  @Override
  public void handle(TaskRunnerEvent event) {
    LOG.info("======================== Processing " + event.getExecutionBlockId() + " of type " + event.getType());
    if (event instanceof TaskRunnerStartEvent) {
      TaskRunnerStartEvent startEvent = (TaskRunnerStartEvent) event;
      ExecutionBlockContext context = executionBlockContextMap.get(event.getExecutionBlockId());

      if(context == null){
        try {
          context = new ExecutionBlockContext(getTajoConf(),
              getWorkerContext(),
              this,
              startEvent.getQueryContext(),
              startEvent.getPlan(),
              startEvent.getExecutionBlockId(),
              startEvent.getQueryMaster(),
              startEvent.getShuffleType());
          context.init();
        } catch (Throwable e) {
          LOG.fatal(e.getMessage(), e);
          throw new RuntimeException(e);
        }
        executionBlockContextMap.put(event.getExecutionBlockId(), context);
      }

      TaskRunner taskRunner = new TaskRunner(context, startEvent.getContainerId());
      LOG.info("Start TaskRunner:" + taskRunner.getId());
      taskRunnerMap.put(taskRunner.getId(), taskRunner);
      taskRunnerHistoryMap.put(taskRunner.getId(), taskRunner.getHistory());

      taskRunner.init(context.getConf());
      taskRunner.start();

    } else if (event instanceof TaskRunnerStopEvent) {
      ExecutionBlockContext executionBlockContext =  executionBlockContextMap.remove(event.getExecutionBlockId());
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
        }
      }
      LOG.info("Stopped execution block:" + event.getExecutionBlockId());
    }
  }

  public EventHandler getEventHandler(){
    return dispatcher.getEventHandler();
  }

  public TajoConf getTajoConf() {
    return tajoConf;
  }

  class FinishedTaskCleanThread extends Thread {
    //TODO if history size is large, the historyMap should remove immediately
    public void run() {
      int expireIntervalTime = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!stop.get()) {
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
      synchronized(taskRunnerHistoryMap) {
        List<String> expiredIds = new ArrayList<String>();
        for(Map.Entry<String, TaskRunnerHistory> entry: taskRunnerHistoryMap.entrySet()) {
           /* If a task runner are abnormal termination, the finished time will be zero. */
          long finishedTime = Math.max(entry.getValue().getStartTime(), entry.getValue().getFinishTime());
          if(finishedTime < expireTime) {
            expiredIds.add(entry.getKey());
          }
        }

        for(String eachId: expiredIds) {
          taskRunnerHistoryMap.remove(eachId);
        }
      }
    }
  }
}
