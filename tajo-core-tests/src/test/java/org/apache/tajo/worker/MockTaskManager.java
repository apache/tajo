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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ResourceProtos.ExecutionBlockContextResponse;
import org.apache.tajo.ResourceProtos.ExecutionBlockListProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.worker.event.TaskManagerEvent;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class MockTaskManager extends TaskManager {

  private final Semaphore barrier;
  private boolean testEbCreateFailure = false;

  public MockTaskManager(Semaphore barrier, Dispatcher dispatcher, TajoWorker.WorkerContext workerContext) {
    super(dispatcher, workerContext);
    this.barrier = barrier;
  }

  public void enableEbCreateFailure() {
    testEbCreateFailure = true;
  }

  public void disableEbCreateFailure() {
    testEbCreateFailure = true;
  }

  @Override
  protected ExecutionBlockContext createExecutionBlock(ExecutionBlockId executionBlockId, String queryMaster) {
    if (testEbCreateFailure) {
      throw new RuntimeException("Failure for test");
    }
    try {
      ExecutionBlockContextResponse.Builder builder = ExecutionBlockContextResponse.newBuilder();
      builder.setExecutionBlockId(executionBlockId.getProto())
          .setPlanJson("test")
          .setQueryContext(new QueryContext(new TajoConf()).getProto())
          .setQueryOutputPath("testpath")
          .setShuffleType(PlanProto.ShuffleType.HASH_SHUFFLE);
      return new MockExecutionBlockContext(getWorkerContext(), builder.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void stopExecutionBlock(ExecutionBlockContext context,
                                    ExecutionBlockListProto cleanupList) {
    //skip for testing
  }

  @Override
  public void handle(TaskManagerEvent event) {
    super.handle(event);
    barrier.release();
  }
}
