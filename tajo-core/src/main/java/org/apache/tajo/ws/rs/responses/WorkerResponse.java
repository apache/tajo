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

package org.apache.tajo.ws.rs.responses;

import org.apache.tajo.master.rm.NodeStatus;

import com.google.gson.annotations.Expose;
import org.apache.tajo.resource.NodeResource;

public class WorkerResponse {
  
  @Expose private WorkerConnectionInfoResponse connectionInfo;

  @Expose private float diskSlots;
  @Expose private int cpuCoreSlots;
  @Expose private int memoryMB;

  @Expose private float usedDiskSlots;
  @Expose private int usedMemoryMB;
  @Expose private int usedCpuCoreSlots;

  @Expose private int numRunningTasks;
  @Expose private int numQueryMasterTasks;
  
  @Expose private long lastHeartbeatTime;
  
  public WorkerResponse(NodeStatus nodeStatus) {
    this(nodeStatus.getTotalResourceCapability(), nodeStatus.getAvailableResource(),
        nodeStatus.getNumRunningTasks(), nodeStatus.getNumRunningQueryMaster());

    this.connectionInfo = new WorkerConnectionInfoResponse(nodeStatus.getConnectionInfo());
    
    this.lastHeartbeatTime = nodeStatus.getLastHeartbeatTime();
  }

  private WorkerResponse(NodeResource total, NodeResource available, int numRunningTasks, int numQueryMasterTasks) {
    this.cpuCoreSlots = total.getVirtualCores();
    this.memoryMB = total.getMemory();
    this.usedMemoryMB = available.getMemory();
    this.usedCpuCoreSlots = available.getVirtualCores();
    this.numRunningTasks = numRunningTasks;
    this.numQueryMasterTasks = numQueryMasterTasks;
  }

  public WorkerConnectionInfoResponse getConnectionInfo() {
    return connectionInfo;
  }

  public int getCpuCoreSlots() {
    return cpuCoreSlots;
  }

  public int getMemoryMB() {
    return memoryMB;
  }

  public float getUsedDiskSlots() {
    return usedDiskSlots;
  }

  public int getUsedMemoryMB() {
    return usedMemoryMB;
  }

  public int getUsedCpuCoreSlots() {
    return usedCpuCoreSlots;
  }

  public int getNumRunningTasks() {
    return numRunningTasks;
  }

  public int getNumQueryMasterTasks() {
    return numQueryMasterTasks;
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  public float getDiskSlots() {
    return diskSlots;
  }
}
