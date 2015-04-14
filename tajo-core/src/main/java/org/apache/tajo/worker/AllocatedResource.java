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

import com.google.common.base.Predicate;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;

import java.util.concurrent.atomic.AtomicBoolean;

public class AllocatedResource extends Resources {

  private final int resourceId;

  /** Worker connection information */
  private final WorkerConnectionInfo connectionInfo;

  private final AtomicBoolean occupied = new AtomicBoolean(false);

  public AllocatedResource(int resourceId, int cpuCoreSlots, int memoryMB, float diskSlots, WorkerConnectionInfo connectionInfo) {
    super(cpuCoreSlots, memoryMB, diskSlots);
    this.resourceId = resourceId;
    this.connectionInfo = connectionInfo;
  }

  public int getResourceId() {
    return resourceId;
  }

  public WorkerConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  public synchronized boolean isOccupied() {
    return occupied.get();
  }

  public synchronized boolean release() {
    return occupied.compareAndSet(true, false);
  }

  public synchronized boolean acquire() {
    return occupied.compareAndSet(false, true);
  }

  public String toString() {
    return connectionInfo.getId() + "::" + super.toString();
  }

  public static Predicate<AllocatedResource> getMinimum(final Resources required) {
    final Predicate<Resources> predicate = Resources.getMinimum(required);
    return new Predicate<AllocatedResource>() {
      @Override
      public boolean apply(AllocatedResource input) {
        return !input.isOccupied() && predicate.apply(input);
      }
    };
  }
}
