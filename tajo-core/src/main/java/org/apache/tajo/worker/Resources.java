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
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import org.apache.hadoop.yarn.api.records.Resource;

public class Resources extends Resource {
  // todo: there should be min-max
  protected final int cpuCoreSlots;
  protected final int memoryMB;
  protected final float diskSlots;

  public Resources(int cpuCoreSlots, int memoryMB, float diskSlots) {
    this.memoryMB = memoryMB;
    this.cpuCoreSlots = cpuCoreSlots;
    this.diskSlots = diskSlots;
  }

  public float getDiskSlots() {
    return diskSlots;
  }

  @Override
  public int getMemory() {
    return memoryMB;
  }

  @Override
  public void setMemory(int i) {
    throw new UnsupportedOperationException("setMemory");
  }

  @Override
  public int getVirtualCores() {
    return cpuCoreSlots;
  }

  @Override
  public void setVirtualCores(int i) {
    throw new UnsupportedOperationException("setVirtualCores");
  }

  @Override
  public String toString() {
    return "CPU=" + cpuCoreSlots + ", MEMORY=" + memoryMB + "MB, DISK=" + diskSlots;
  }

  public static <T extends Resources> Predicate<T> getMinimum(final T required) {
    return new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        return required.cpuCoreSlots >= input.cpuCoreSlots &&
               required.memoryMB >= input.memoryMB &&
               required.diskSlots >= input.diskSlots;
      }
    };
  }

  @Override
  public int compareTo(Resource o) {
    int compare = Ints.compare(cpuCoreSlots, o.getVirtualCores());
    if (compare == 0) {
      compare = Ints.compare(memoryMB, o.getMemory());
    }
    if (compare == 0 && o instanceof Resources) {
      compare = Floats.compare(diskSlots, ((Resources)o).getDiskSlots());
    }
    return compare;
  }
}
