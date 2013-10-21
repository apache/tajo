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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.tajo.conf.TajoConf;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunnerManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TaskRunnerManager.class);

  private final Map<String, TaskRunner> taskRunnerMap = new HashMap<String, TaskRunner>();
  private final Map<String, TaskRunner> finishedTaskRunnerMap = new HashMap<String, TaskRunner>();
  private TajoWorker.WorkerContext workerContext;
  private TajoConf tajoConf;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private FinishedTaskCleanThread finishedTaskCleanThread;

  public TaskRunnerManager(TajoWorker.WorkerContext workerContext) {
    super(TaskRunnerManager.class.getName());

    this.workerContext = workerContext;
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf)conf;
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
    if(stop.get()) {
      return;
    }
    stop.set(true);
    synchronized(taskRunnerMap) {
      for(TaskRunner eachTaskRunner: taskRunnerMap.values()) {
        if(!eachTaskRunner.isStopped()) {
          eachTaskRunner.stop();
        }
      }
    }

    if(finishedTaskCleanThread != null) {
      finishedTaskCleanThread.interrupted();
    }
    super.stop();
    if(!workerContext.isStandbyMode()) {
      workerContext.stopWorker(true);
    }
  }

  public void stopTask(String id) {
    LOG.info("Stop Task:" + id);
    synchronized(taskRunnerMap) {
      TaskRunner taskRunner = taskRunnerMap.remove(id);
      if(taskRunner != null) {
        finishedTaskRunnerMap.put(id, taskRunner);
      }
    }
    if(!workerContext.isStandbyMode()) {
      stop();
    }
  }

  public Collection<TaskRunner> getTaskRunners() {
    synchronized(taskRunnerMap) {
      return Collections.unmodifiableCollection(taskRunnerMap.values());
    }
  }

  public Collection<TaskRunner> getFinishedTaskRunners() {
    synchronized(finishedTaskRunnerMap) {
      return Collections.unmodifiableCollection(finishedTaskRunnerMap.values());
    }
  }

  public int getNumTasks() {
    synchronized(taskRunnerMap) {
      return taskRunnerMap.size();
    }
  }

  public void startTask(final String[] params) {
    //TODO change to use event dispatcher
    Thread t = new Thread() {
      public void run() {
        try {
          TajoConf systemConf = new TajoConf(tajoConf);
          TaskRunner taskRunner = new TaskRunner(TaskRunnerManager.this, systemConf, params);
          LOG.info("Start TaskRunner:" + taskRunner.getId());
          synchronized(taskRunnerMap) {
            taskRunnerMap.put(taskRunner.getId(), taskRunner);
          }
          taskRunner.init(systemConf);
          taskRunner.start();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          throw new RuntimeException(e.getMessage(), e);
        }
      }
    };

    t.start();
  }

  class FinishedTaskCleanThread extends Thread {
    public void run() {
      int expireIntervalTime = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!stop.get()) {
        try {
          Thread.sleep(60 * 1000 * 60);   // hourly check
        } catch (InterruptedException e) {
          break;
        }
        try {
          long expireTime = System.currentTimeMillis() - expireIntervalTime * 60 * 1000;
          cleanExpiredFinishedQueryMasterTask(expireTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    private void cleanExpiredFinishedQueryMasterTask(long expireTime) {
      synchronized(finishedTaskRunnerMap) {
        List<String> expiredIds = new ArrayList<String>();
        for(Map.Entry<String, TaskRunner> entry: finishedTaskRunnerMap.entrySet()) {
          if(entry.getValue().getStartTime() > expireTime) {
            expiredIds.add(entry.getKey());
          }
        }

        for(String eachId: expiredIds) {
          finishedTaskRunnerMap.remove(eachId);
        }
      }
    }
  }
}
