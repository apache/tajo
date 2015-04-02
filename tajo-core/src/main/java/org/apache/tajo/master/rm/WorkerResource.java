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

/**
 * Describe current resources of a worker.
 *
 * It includes various resource capability of a worker as follows:
 * <ul>
 *   <li>used/total disk slots</li>
 *   <li>used/total core slots</li>
 *   <li>used/total memory</li>
 *   <li>the number of running tasks</li>
 * </ul>
 */
public class WorkerResource {
  private static final Log LOG = LogFactory.getLog(WorkerResource.class);

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

  private AtomicInteger numQueryMasterTasks = new AtomicInteger(0);

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
    return "slots=m:" + memoryMB + ",d:" + diskSlots +
        ",c:" + cpuCoreSlots + ", used=m:" + usedMemoryMB + ",d:" + usedDiskSlots + ",c:" + usedCpuCoreSlots;
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
}
