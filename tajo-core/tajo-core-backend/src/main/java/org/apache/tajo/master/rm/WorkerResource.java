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
import org.apache.tajo.worker.TajoWorker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkerResource {
  private static final Log LOG = LogFactory.getLog(WorkerResource.class);

  private String allocatedHost;
  private int peerRpcPort;
  private int queryMasterPort;
  private int clientPort;
  private int pullServerPort;
  private int httpPort;

  private int diskSlots;
  private int cpuCoreSlots;
  private int memoryMBSlots;

  private int usedDiskSlots;
  private int usedMemoryMBSlots;
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

  public void addUsedDiskSlots(int diskSlots) {
    usedDiskSlots += diskSlots;
  }

  public void addUsedMemoryMBSlots(int memoryMBSlots) {
    try {
      wlock.lock();
      usedMemoryMBSlots += memoryMBSlots;
    } finally {
      wlock.unlock();
    }
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
    try {
      rlock.lock();
      return memoryMBSlots;
    } finally {
      rlock.unlock();
    }
  }

  public void setMemoryMBSlots(int memoryMBSlots) {
    try {
      wlock.lock();
      this.memoryMBSlots = memoryMBSlots;
    } finally {
      wlock.unlock();
    }
  }

  public int getAvailableDiskSlots() {
    return diskSlots - usedDiskSlots;
  }

  public int getAvailableMemoryMBSlots() {
    return getMemoryMBSlots() - getUsedMemoryMBSlots();
  }

  @Override
  public String toString() {
    return "host:" + allocatedHost + ", port=" + portsToStr() + ", slots=" + memoryMBSlots + ":" + cpuCoreSlots + ":" + diskSlots +
        ", used=" + getUsedMemoryMBSlots() + ":" + usedCpuCoreSlots + ":" + usedDiskSlots;
  }

  public String portsToStr() {
    return queryMasterPort + "," + peerRpcPort + "," + clientPort + "," + pullServerPort;
  }

  public void setLastHeartbeat(long heartbeatTime) {
    this.lastHeartbeat = heartbeatTime;
  }

  public int getUsedMemoryMBSlots() {
    try {
      rlock.lock();
      return usedMemoryMBSlots;
    } finally {
      rlock.unlock();
    }
  }

  public void setUsedMemoryMBSlots(int usedMemoryMBSlots) {
    try {
      wlock.lock();
      this.usedMemoryMBSlots = usedMemoryMBSlots;
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

  public void releaseResource(WorkerResource workerResource) {
    try {
      wlock.lock();
      usedMemoryMBSlots = usedMemoryMBSlots - workerResource.getMemoryMBSlots();
    } finally {
      wlock.unlock();
    }

    if(getUsedMemoryMBSlots() < 0 || usedDiskSlots < 0 || usedCpuCoreSlots < 0) {
      LOG.warn("Used resources can't be a minus.");
      LOG.warn(this + " ==> " + workerResource);
    }
  }

  public int getSlots() {
    //TODO what is slot? 512MB = 1slot?
    return getMemoryMBSlots()/512;
  }

  public int getAvaliableSlots() {
    //TODO what is slot? 512MB = 1slot?
    return getAvailableMemoryMBSlots()/512;
  }

  public int getUsedSlots() {
    //TODO what is slot? 512MB = 1slot?
    return getUsedMemoryMBSlots()/512;
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

  public void setNumQueryMasterTasks(int numQueryMasterTasks) {
    this.numQueryMasterTasks.set(numQueryMasterTasks);
  }

  public void addNumQueryMasterTask() {
    numQueryMasterTasks.getAndIncrement();
  }

  public void releaseQueryMasterTask() {
    numQueryMasterTasks.getAndDecrement();
  }
}
