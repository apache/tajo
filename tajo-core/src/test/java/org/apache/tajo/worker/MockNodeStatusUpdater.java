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
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.tajo.ipc.QueryCoordinatorProtocol;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.tajo.ipc.TajoResourceTrackerProtocol.*;

public class MockNodeStatusUpdater extends NodeStatusUpdater {

  private CountDownLatch barrier;
  private Map<Integer, NodeResource> membership = Maps.newHashMap();
  private Map<Integer, NodeResource> resources = Maps.newHashMap();
  private MockResourceTracker resourceTracker;

  public MockNodeStatusUpdater(CountDownLatch barrier, TajoWorker.WorkerContext workerContext,
                               NodeResourceManager resourceManager) {
    super(workerContext, resourceManager);
    this.barrier = barrier;
    this.resourceTracker = new MockResourceTracker();
  }

  @Override
  protected TajoResourceTrackerProtocolService.Interface newStub()
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {

    return resourceTracker;
  }

  protected MockResourceTracker getResourceTracker() {
    return resourceTracker;
  }

  class MockResourceTracker implements TajoResourceTrackerProtocolService.Interface {
    private NodeHeartbeatRequestProto lastRequest;

    protected Map<Integer, NodeResource> getTotalResource() {
      return membership;
    }

    protected Map<Integer, NodeResource> getAvailableResource() {
      return membership;
    }

    protected NodeHeartbeatRequestProto getLastRequest() {
      return lastRequest;
    }

    @Override
    public void heartbeat(RpcController controller, NodeHeartbeat request,
                          RpcCallback<QueryCoordinatorProtocol.TajoHeartbeatResponse> done) {

    }

    @Override
    public void nodeHeartbeat(RpcController controller, NodeHeartbeatRequestProto request,
                              RpcCallback<NodeHeartbeatResponseProto> done) {

      NodeHeartbeatResponseProto.Builder response = NodeHeartbeatResponseProto.newBuilder();
      if (membership.containsKey(request.getWorkerId())) {
        if (request.hasAvailableResource()) {
          NodeResource resource = resources.get(request.getWorkerId());
          NodeResources.update(resource, new NodeResource(request.getAvailableResource()));
        }
        done.run(response.setCommand(ResponseCommand.NORMAL).build());
      } else {
        if (request.hasConnectionInfo()) {
          membership.put(request.getWorkerId(), new NodeResource(request.getTotalResource()));
          resources.put(request.getWorkerId(), new NodeResource(request.getAvailableResource()));
          done.run(response.setCommand(ResponseCommand.NORMAL).build());
        } else {
          done.run(response.setCommand(ResponseCommand.MEMBERSHIP).build());
        }
      }
      lastRequest = request;
      barrier.countDown();
    }
  }
}
