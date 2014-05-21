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

package org.apache.tajo.memory;

import com.google.common.collect.Maps;

import java.util.Map;

public class MemoryContextGroup {
  private final String name;
  private final long maxMemory;
  private volatile long freeMemory;

  static final Map<String, Long> reserved = Maps.newConcurrentMap();

  public MemoryContextGroup(String name, long maxMemory) {
    this.name = name;
    this.maxMemory = maxMemory;
    this.freeMemory = maxMemory;
  }

  public synchronized boolean reserve(String name, long memorySize) {
    if (reserved.containsKey(name)) {
      throw new AlreadyReservedContextException(String.format("'%s' already reserved the memory.", name));
    }

    long remainMemorySize = freeMemory - memorySize;
    if (remainMemorySize < 0) {
      return false;
    }

    freeMemory -= memorySize;
    reserved.put(name, memorySize);
    return true;
  }

  public synchronized void free(String name) {
    if (!reserved.containsKey(name)) {
      throw new AlreadyReservedContextException(String.format("No context '%s' in this group", name));
    }

    freeMemory += reserved.remove(name);
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public long getReservedMemory() {
    return maxMemory - freeMemory;
  }

  public long getFreeMemory() {
    return freeMemory;
  }
}
