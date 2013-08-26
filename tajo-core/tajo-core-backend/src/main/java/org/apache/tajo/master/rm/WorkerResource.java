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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WorkerResource {
  private static final Log LOG = LogFactory.getLog(WorkerResource.class);

  private String allocatedHost;
  private int[] ports;

  private int diskSlots;
  private int cpuCoreSlots;
  private int memoryMBSlots;

  private int usedDiskSlots;
  private int usedMemoryMBSlots;
  private int usedCpuCoreSlots;

  private boolean queryMasterAllocated;

  private WorkerStatus workerStatus;

  private long lastHeartbeat;

  public String getId() {
    if(ports.length > 0) {
      return allocatedHost + ":" + ports[0];
    } else {
      return allocatedHost;
    }
  }

  public String getAllocatedHost() {
    return allocatedHost;
  }

  public void setAllocatedHost(String allocatedHost) {
    this.allocatedHost = allocatedHost;
  }

  public int[] getPorts() {
    return ports;
  }

  public void setPorts(int[] ports) {
    this.ports = ports;
  }

  public void addUsedDiskSlots(int diskSlots) {
    usedDiskSlots += diskSlots;
  }

  public void addUsedMemoryMBSlots(int memoryMBSlots) {
    usedMemoryMBSlots += memoryMBSlots;
  }

  public void addUsedCpuCoreSlots(int cpuCoreSlots) {
    usedCpuCoreSlots += cpuCoreSlots;
  }

  public int getDiskSlots() {
    return diskSlots;
  }

  public void setDiskSlots(int diskSlots) {
    this.diskSlots = diskSlots;
  }

  public int getCpuCoreSlots() {
    return cpuCoreSlots;
  }

  public void setCpuCoreSlots(int cpuCoreSlots) {
    this.cpuCoreSlots = cpuCoreSlots;
  }

  public int getMemoryMBSlots() {
    return memoryMBSlots;
  }

  public void setMemoryMBSlots(int memoryMBSlots) {
    this.memoryMBSlots = memoryMBSlots;
  }

  public int getAvailableDiskSlots() {
    return diskSlots - usedDiskSlots;
  }

  public int getAvailableMemoryMBSlots() {
    return memoryMBSlots - usedMemoryMBSlots;
  }

  @Override
  public String toString() {
    return "host:" + allocatedHost + ", port=" + portsToStr() + ", slots=" + memoryMBSlots + ":" + cpuCoreSlots + ":" + diskSlots +
        ", used=" + usedMemoryMBSlots + ":" + usedCpuCoreSlots + ":" + usedDiskSlots;
  }

  private String portsToStr() {
    if(ports == null) {
      return "null";
    }
    String result = "";
    String prefix = "";
    for(int i = 0; i < ports.length; i++) {
      result += prefix + ports[i];
      prefix = ",";
    }

    return result;
  }

  public void setLastHeartbeat(long heartbeatTime) {
    this.lastHeartbeat = heartbeatTime;
  }

  public int getUsedMemoryMBSlots() {
    return usedMemoryMBSlots;
  }

  public void setUsedMemoryMBSlots(int usedMemoryMBSlots) {
    this.usedMemoryMBSlots = usedMemoryMBSlots;
  }

  public int getUsedCpuCoreSlots() {
    return usedCpuCoreSlots;
  }

  public void setUsedCpuCoreSlots(int usedCpuCoreSlots) {
    this.usedCpuCoreSlots = usedCpuCoreSlots;
  }

  public int getUsedDiskSlots() {
    return usedDiskSlots;
  }

  public void setUsedDiskSlots(int usedDiskSlots) {
    this.usedDiskSlots = usedDiskSlots;
  }

  public WorkerStatus getWorkerStatus() {
    return workerStatus;
  }

  public void setWorkerStatus(WorkerStatus workerStatus) {
    this.workerStatus = workerStatus;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public boolean isQueryMasterAllocated() {
    return queryMasterAllocated;
  }

  public void setQueryMasterAllocated(boolean queryMasterAllocated) {
    this.queryMasterAllocated = queryMasterAllocated;
  }

  public void releaseResource(WorkerResource workerResource) {
    if(workerResource.isQueryMasterAllocated()) {
        queryMasterAllocated = false;
    }

    usedMemoryMBSlots -= workerResource.memoryMBSlots;
    usedDiskSlots -= workerResource.diskSlots;

    if(usedMemoryMBSlots < 0 || usedDiskSlots < 0 || usedCpuCoreSlots < 0) {
//      LOG.warn("Used resources can't be a minus.");
      LOG.trace("Used resources can't be a minus.");
      LOG.warn(this + " ==> " + workerResource);
    }
  }
}
