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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkerResource implements Comparable<WorkerResource> {
  private static final Log LOG = LogFactory.getLog(WorkerResource.class);

  private String allocatedHost;
  private int peerRpcPort;
  private int queryMasterPort;
  private int clientPort;
  private int pullServerPort;
  private int httpPort;

  private float diskSlots;
  private int cpuCoreSlots;
  private int memoryMB;

  private float usedDiskSlots;
  private int usedMemoryMB;
  private int usedCpuCoreSlots;

  private long maxHeap;
  private long freeHeap;
  private long totalHeap;

  private int numRunningTasks;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock rlock = lock.readLock();
  private final Lock wlock = lock.writeLock();

  private WorkerStatus workerStatus;

  private long lastHeartbeat;

  private boolean queryMasterMode;

  private boolean taskRunnerMode;

  private AtomicInteger numQueryMasterTasks = new AtomicInteger(0);

  public String getId() {
    return allocatedHost + ":" + queryMasterPort + ":" + peerRpcPort;
  }

  public String getAllocatedHost() {
    return allocatedHost;
  }

  public void setAllocatedHost(String allocatedHost) {
    this.allocatedHost = allocatedHost;
  }

  public float getDiskSlots() {
    return diskSlots;
  }

  public void setDiskSlots(float diskSlots) {
    this.diskSlots = diskSlots;
  }

  public int getCpuCoreSlots() {
    return cpuCoreSlots;
  }

  public void setCpuCoreSlots(int cpuCoreSlots) {
    this.cpuCoreSlots = cpuCoreSlots;
  }

  public int getMemoryMB() {
    try {
      rlock.lock();
      return memoryMB;
    } finally {
      rlock.unlock();
    }
  }

  public void setMemoryMB(int memoryMB) {
    try {
      wlock.lock();
      this.memoryMB = memoryMB;
    } finally {
      wlock.unlock();
    }
  }

  public float getAvailableDiskSlots() {
    return diskSlots - usedDiskSlots;
  }

  public int getAvailableMemoryMB() {
    return memoryMB - usedMemoryMB;
  }

  public int getAvailableCpuCoreSlots() {
    return cpuCoreSlots - usedCpuCoreSlots;
  }

  @Override
  public String toString() {
    return "host:" + allocatedHost + ", port=" + portsToStr() + ", slots=m:" + memoryMB + ",d:" + diskSlots +
        ",c:" + cpuCoreSlots + ", used=m:" + usedMemoryMB + ",d:" + usedDiskSlots + ",c:" + usedCpuCoreSlots;
  }

  public String portsToStr() {
    return queryMasterPort + "," + peerRpcPort + "," + clientPort + "," + pullServerPort;
  }

  public void setLastHeartbeat(long heartbeatTime) {
    this.lastHeartbeat = heartbeatTime;
  }

  public int getUsedMemoryMB() {
    try {
      rlock.lock();
      return usedMemoryMB;
    } finally {
      rlock.unlock();
    }
  }

  public void setUsedMemoryMB(int usedMemoryMB) {
    try {
      wlock.lock();
      this.usedMemoryMB = usedMemoryMB;
    } finally {
      wlock.unlock();
    }
  }

  public int getUsedCpuCoreSlots() {
    return usedCpuCoreSlots;
  }

  public void setUsedCpuCoreSlots(int usedCpuCoreSlots) {
    this.usedCpuCoreSlots = usedCpuCoreSlots;
  }

  public float getUsedDiskSlots() {
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

  public boolean isQueryMasterMode() {
    return queryMasterMode;
  }

  public void setQueryMasterMode(boolean queryMasterMode) {
    this.queryMasterMode = queryMasterMode;
  }

  public boolean isTaskRunnerMode() {
    return taskRunnerMode;
  }

  public void setTaskRunnerMode(boolean taskRunnerMode) {
    this.taskRunnerMode = taskRunnerMode;
  }

  public void releaseResource(float diskSlots, int memoryMB) {
    try {
      wlock.lock();
      usedMemoryMB = usedMemoryMB - memoryMB;
      usedDiskSlots -= diskSlots;
      if(usedMemoryMB < 0) {
        LOG.warn("Used memory can't be a minus: " + usedMemoryMB);
        usedMemoryMB = 0;
      }
      if(usedDiskSlots < 0) {
        LOG.warn("Used disk slot can't be a minus: " + usedDiskSlots);
        usedDiskSlots = 0;
      }
    } finally {
      wlock.unlock();
    }
  }

  public void allocateResource(float diskSlots, int memoryMB) {
    try {
      wlock.lock();
      usedMemoryMB += memoryMB;
      usedDiskSlots += diskSlots;

      if(usedMemoryMB > this.memoryMB) {
        usedMemoryMB = this.memoryMB;
      }

      if(usedDiskSlots > this.diskSlots) {
        usedDiskSlots = this.diskSlots;
      }
    } finally {
      wlock.unlock();
    }
  }

  public int getPeerRpcPort() {
    return peerRpcPort;
  }

  public void setPeerRpcPort(int peerRpcPort) {
    this.peerRpcPort = peerRpcPort;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public void setQueryMasterPort(int queryMasterPort) {
    this.queryMasterPort = queryMasterPort;
  }
  
  public int getClientPort() {
    return clientPort;
  }

  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }

  public int getPullServerPort() {
    return pullServerPort;
  }

  public void setPullServerPort(int pullServerPort) {
    this.pullServerPort = pullServerPort;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public long getMaxHeap() {
    return maxHeap;
  }

  public void setMaxHeap(long maxHeap) {
    this.maxHeap = maxHeap;
  }

  public long getFreeHeap() {
    return freeHeap;
  }

  public void setFreeHeap(long freeHeap) {
    this.freeHeap = freeHeap;
  }

  public long getTotalHeap() {
    return totalHeap;
  }

  public void setTotalHeap(long totalHeap) {
    this.totalHeap = totalHeap;
  }

  public int getNumRunningTasks() {
    return numRunningTasks;
  }

  public void setNumRunningTasks(int numRunningTasks) {
    this.numRunningTasks = numRunningTasks;
  }

  public int getNumQueryMasterTasks() {
    return numQueryMasterTasks.get();
  }

  public void addNumQueryMasterTask(float diskSlots, int memoryMB) {
    numQueryMasterTasks.getAndIncrement();
    allocateResource(diskSlots, memoryMB);
  }

  public void releaseQueryMasterTask(float diskSlots, int memoryMB) {
    numQueryMasterTasks.getAndDecrement();
    releaseResource(diskSlots, memoryMB);
  }

  @Override
  public int compareTo(WorkerResource workerResource) {
    if(workerResource == null) {
      return 1;
    }
    return getId().compareTo(workerResource.getId());
  }
}
