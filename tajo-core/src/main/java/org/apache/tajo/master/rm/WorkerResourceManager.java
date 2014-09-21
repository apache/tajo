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

package org.apache.tajo.master.rm;

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.tajo.QueryId;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.querymaster.QueryInProgress;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.apache.tajo.ipc.TajoMasterProtocol.WorkerAllocatedResource;
import static org.apache.tajo.ipc.TajoMasterProtocol.WorkerResourceAllocationResponse;

/**
 * An interface of WorkerResourceManager which allows TajoMaster to request allocation for containers
 * and release the allocated containers.
 */
public interface WorkerResourceManager extends Service {

  /**
   * Request a resource container for a QueryMaster.
   *
   * @param queryInProgress QueryInProgress
   * @return A allocated container resource
   */
  @Deprecated
  public WorkerAllocatedResource allocateQueryMaster(QueryInProgress queryInProgress);

  /**
   * Request one or more resource containers. You can set the number of containers and resource capabilities, such as
   * memory, CPU cores, and disk slots. This is an asynchronous call. You should use a callback to get allocated
   * resource containers. Each container is identified {@link org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto}.
   *
   * @param request Request description
   * @param rpcCallBack Callback function
   */
  public void allocateWorkerResources(TajoMasterProtocol.WorkerResourceAllocationRequest request,
      RpcCallback<WorkerResourceAllocationResponse> rpcCallBack);

  /**
   * Release a container
   *
   * @param containerId ContainerIdProto to be released
   */
  public void releaseWorkerResource(ContainerIdProto containerId);

  public String getSeedQueryId() throws IOException;

  /**
   * Check if a query master is stopped.
   *
   * @param queryId QueryId to be checked
   * @return True if QueryMaster is stopped
   */
  public boolean isQueryMasterStopped(QueryId queryId);

  /**
   * Stop a query master
   *
   * @param queryId QueryId to be stopped
   */
  public void stopQueryMaster(QueryId queryId);

  /**
   *
   * @return a Map instance containing active workers
   */
  public Map<Integer, Worker> getWorkers();

  /**
   *
   * @return a Map instance containing inactive workers
   */
  public Map<Integer, Worker> getInactiveWorkers();

  public void stop();

  /**
   *
   * @return The overall summary of cluster resources
   */
  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary();

  /**
   *
   * @return WorkerIds on which QueryMasters are running
   */
  Collection<Integer> getQueryMasters();
}
