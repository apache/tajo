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

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.worker.event.TaskExecutorEvent;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class MockTaskExecutor extends TaskExecutor {

  protected final Semaphore barrier;

  public MockTaskExecutor(Semaphore barrier, TaskManager taskManager, EventHandler rmEventHandler) {
    super(taskManager, rmEventHandler);
    this.barrier = barrier;
  }

  @Override
  public void handle(TaskExecutorEvent event) {
    super.handle(event);
    barrier.release();
  }

  @Override
  protected Task createTask(final ExecutionBlockContext context, TajoWorkerProtocol.TaskRequestProto taskRequest) {
    final TaskAttemptId taskAttemptId = new TaskAttemptId(taskRequest.getId());

    //ignore status changed log
    final TaskAttemptContext taskAttemptContext = new TaskAttemptContext(null, context, taskAttemptId, null, null) {
      private TajoProtos.TaskAttemptState state;

      @Override
      public TajoProtos.TaskAttemptState getState() {
        return state;
      }

      @Override
      public void setState(TajoProtos.TaskAttemptState state) {
        this.state = state;
      }
    };

    return new Task() {
      @Override
      public void init() throws IOException {

      }

      @Override
      public void fetch() {

      }

      @Override
      public void run() throws Exception {
        taskAttemptContext.stop();
        taskAttemptContext.setProgress(1.0f);
        taskAttemptContext.setState(TajoProtos.TaskAttemptState.TA_SUCCEEDED);
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
