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

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.worker.event.TaskManagerEvent;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class MockTaskManager extends TaskManager {

  private final Semaphore barrier;

  public MockTaskManager(Semaphore barrier, Dispatcher dispatcher, TajoWorker.WorkerContext workerContext, EventHandler rmEventHandler) {
    super(dispatcher, workerContext, rmEventHandler);
    this.barrier = barrier;
  }

  @Override
  protected ExecutionBlockContext createExecutionBlock(TajoWorkerProtocol.RunExecutionBlockRequestProto request) {
    try {
      return new MockExecutionBlock(getWorkerContext(), request);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void stopExecutionBlock(ExecutionBlockContext context,
                                    TajoWorkerProtocol.ExecutionBlockListProto cleanupList) {
    //skip for testing
  }

  @Override
  public void handle(TaskManagerEvent event) {
    super.handle(event);
    barrier.release();
  }
}
