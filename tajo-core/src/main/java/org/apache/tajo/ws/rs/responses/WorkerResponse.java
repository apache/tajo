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

import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;

import com.google.gson.annotations.Expose;

public class WorkerResponse {
  
  @Expose private WorkerConnectionInfoResponse connectionInfo;

  @Expose private float diskSlots;
  @Expose private int cpuCoreSlots;
  @Expose private int memoryMB;

  @Expose private float usedDiskSlots;
  @Expose private int usedMemoryMB;
  @Expose private int usedCpuCoreSlots;

  @Expose private long maxHeap;
  @Expose private long freeHeap;
  @Expose private long totalHeap;

  @Expose private int numRunningTasks;
  @Expose private int numQueryMasterTasks;
  
  @Expose private long lastHeartbeatTime;
  
  public WorkerResponse(Worker worker) {
    this(worker.getResource());
    
    this.connectionInfo = new WorkerConnectionInfoResponse(worker.getConnectionInfo());
    
    this.lastHeartbeatTime = worker.getLastHeartbeatTime();
  }
  
  private WorkerResponse(WorkerResource resource) {
    this.cpuCoreSlots = resource.getCpuCoreSlots();
    this.memoryMB = resource.getMemoryMB();
    this.usedDiskSlots = resource.getUsedDiskSlots();
    this.usedMemoryMB = resource.getUsedMemoryMB();
    this.usedCpuCoreSlots = resource.getUsedCpuCoreSlots();
    this.maxHeap = resource.getMaxHeap();
    this.freeHeap = resource.getFreeHeap();
    this.totalHeap = resource.getTotalHeap();
    this.numRunningTasks = resource.getNumRunningTasks();
    this.numQueryMasterTasks = resource.getNumQueryMasterTasks();
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

  public long getMaxHeap() {
    return maxHeap;
  }

  public long getFreeHeap() {
    return freeHeap;
  }

  public long getTotalHeap() {
    return totalHeap;
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
