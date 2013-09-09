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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunnerManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TaskRunnerManager.class);

  private final Map<String, TaskRunner> taskRunnerMap = new HashMap<String, TaskRunner>();
  private TajoWorker.WorkerContext workerContext;
  private TajoConf tajoConf;
  private AtomicBoolean stop = new AtomicBoolean(false);

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
    super.stop();
    if(!workerContext.isStandbyMode()) {
      workerContext.stopWorker(true);
    }
  }

  public void stopTask(String id) {
    LOG.info("Stop Task:" + id);
    synchronized(taskRunnerMap) {
      taskRunnerMap.remove(id);
    }
    if(!workerContext.isStandbyMode()) {
      stop();
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
}
