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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.ipc.QueryMasterProtocol.QueryMasterProtocolService;
import org.apache.tajo.master.container.TajoContainerId;
import org.apache.tajo.master.container.TajoContainerIdPBImpl;
import org.apache.tajo.master.container.TajoConverterUtils;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NullCallback;

import java.io.IOException;
import java.util.concurrent.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

/**
 * The driver class for Tajo Task processing.
 */
public class TaskContainer implements Runnable {
  private static final Log LOG = LogFactory.getLog(TaskContainer.class);

  // Contains the object references related for TaskRunner
  private ExecutionBlockContext executionBlockContext;
  private Task task = null;

  public TaskContainer(ExecutionBlockContext executionBlockContext, Task task) {
    this.executionBlockContext = executionBlockContext;
    this.task = task;
  }

  public void init() throws IOException {
    if (executionBlockContext.isStopped()) return;

    LOG.info("Initializing: " + task.getId());
    getContext().getWorkerContext().getWorkerSystemMetrics().counter("query", "task").inc();
    task.init();
  }

  public void stop() {
    task.cleanupTask();
  }

  public Task getTask() {
    return task;
  }

  public ExecutionBlockContext getContext() {
    return executionBlockContext;
  }

  @Override
  public void run() {
    if (executionBlockContext.isStopped()) return;

    try {
      if (task.hasFetchPhase()) {
        task.fetch(); // The fetch is performed in an asynchronous way.
      }
      if (!executionBlockContext.isStopped()) {
        task.run();
      }
    } catch (Exception t) {
      LOG.error(t.getMessage(), t);
      getContext().fatalError(task.getId(), t.getMessage());
    } finally {
      task.cleanupTask();
    }
  }
}
