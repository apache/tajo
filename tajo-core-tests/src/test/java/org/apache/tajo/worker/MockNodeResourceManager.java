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
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.ResourceProtos.TaskAllocationProto;
import org.apache.tajo.ResourceProtos.TaskRequestProto;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.worker.event.NodeResourceEvent;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class MockNodeResourceManager extends NodeResourceManager {

  volatile boolean enableTaskHandlerEvent = true;
  private final Semaphore barrier;

  public MockNodeResourceManager(Semaphore barrier, Dispatcher dispatcher, TajoWorker.WorkerContext workerContext) {
    super(dispatcher, workerContext);
    this.barrier = barrier;
  }

  @Override
  public void handle(NodeResourceEvent event) {
    super.handle(event);
    barrier.release();
  }

  @Override
  protected void startTask(TaskRequestProto request, Allocation resource) {
    if(enableTaskHandlerEvent) {
      super.startTask(request, resource);
    }
  }

  /**
   * skip task execution and deallocation for testing
   * */
  public void setTaskHandlerEvent(boolean flag) {
    enableTaskHandlerEvent = flag;
  }

  protected static Queue<TaskAllocationProto> createTaskRequests(
      ExecutionBlockId ebId, int memory, int size) {

    Queue<TaskAllocationProto>
        requestProtoList = new LinkedBlockingQueue<>();
    for (int i = 0; i < size; i++) {

      TaskAttemptId taskAttemptId = QueryIdFactory.newTaskAttemptId(QueryIdFactory.newTaskId(ebId, i), 0);
      TaskRequestProto.Builder builder = TaskRequestProto.newBuilder();
      builder.setQueryMasterHostAndPort("localhost:0");
      builder.setId(taskAttemptId.getProto());
      builder.setOutputTable("");
      builder.setPlan(PlanProto.LogicalNodeTree.newBuilder());
      builder.setClusteredOutput(false);


      requestProtoList.add(TaskAllocationProto.newBuilder()
          .setResource(NodeResources.createResource(memory).getProto())
          .setTaskRequest(builder.build()).build());
    }
    return requestProtoList;
  }
}
