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
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.QueryId;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.querymaster.QueryInProgress;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface WorkerResourceManager {

  /**
   * select Worker for QueryMaster
   * @param queryInProgress
   * @return
   */
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress);

  public void allocateWorkerResources(TajoMasterProtocol.WorkerResourceAllocationRequest request,
      RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> rpcCallBack);

  /**
   * start Worker for query master(YARN) or assign query master role(Standby Mode)
   * @param queryInProgress
   */
  public void startQueryMaster(QueryInProgress queryInProgress);

  public String getSeedQueryId() throws IOException;

  public boolean isQueryMasterStopped(QueryId queryId);

  public void init(Configuration conf);

  public void stopQueryMaster(QueryId queryId);

  public void workerHeartbeat(TajoMasterProtocol.TajoHeartbeat request);

  public void releaseWorkerResource(QueryId queryId, WorkerResource workerResource);

  public Map<String, WorkerResource> getWorkers();

  public void stop();

  public int getNumClusterSlots();

  Collection<String> getQueryMasters();
}
