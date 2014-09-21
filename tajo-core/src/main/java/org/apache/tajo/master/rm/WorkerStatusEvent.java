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

/**
 * {@link TajoResourceTracker} produces this event, and its destination is
 * {@link org.apache.tajo.master.rm.Worker.StatusUpdateTransition} of {@link Worker}.
 */
public class WorkerStatusEvent extends WorkerEvent {
  private final int runningTaskNum;
  private final long maxHeap;
  private final long freeHeap;
  private final long totalHeap;

  public WorkerStatusEvent(int workerId, int runningTaskNum, long maxHeap, long freeHeap, long totalHeap) {
    super(workerId, WorkerEventType.STATE_UPDATE);
    this.runningTaskNum = runningTaskNum;
    this.maxHeap = maxHeap;
    this.freeHeap = freeHeap;
    this.totalHeap = totalHeap;
  }

  public int getRunningTaskNum() {
    return runningTaskNum;
  }

  public long maxHeap() {
    return maxHeap;
  }

  public long getFreeHeap() {
    return freeHeap;
  }

  public long getTotalHeap() {
    return totalHeap;
  }
}
