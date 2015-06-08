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
import org.apache.tajo.TajoProtos;

/**
 * The driver class for Tajo Task processing.
 */
public class TaskContainer implements Runnable {
  private static final Log LOG = LogFactory.getLog(TaskContainer.class);

  private final TaskExecutor executor;
  private final int sequenceId;

  public TaskContainer(int sequenceId, TaskExecutor executor) {
    this.sequenceId = sequenceId;
    this.executor = executor;
  }

  @Override
  public void run() {
    while (!executor.isStopped()) {

      Task task = null;
      try {
        task = executor.getNextTask();

        task.getExecutionBlockContext().getWorkerContext().getWorkerSystemMetrics().counter("query", "task").inc();

        if (LOG.isDebugEnabled()) {
          LOG.debug(sequenceId + TaskContainer.class.getSimpleName() +
              " got task:" + task.getTaskContext().getTaskId());
        }

        TaskAttemptContext taskAttemptContext = task.getTaskContext();
        if (taskAttemptContext.isStopped()) return;

        task.init();

        if (task.hasFetchPhase()) {
          task.fetch(); // The fetch is performed in an asynchronous way.
        }

        if (!taskAttemptContext.isStopped()) {
          task.run();
        }

        task.cleanup();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        if (task != null) {
          try {
            task.abort();
            task.getExecutionBlockContext().fatalError(task.getTaskContext().getTaskId(), e.getMessage());
          } catch (Throwable t) {
            LOG.fatal(t.getMessage(), t);
          }
        }
      } finally {
        if (task != null) {
          executor.stopTask(task.getTaskContext().getTaskId());
        }
      }
    }
  }
}
