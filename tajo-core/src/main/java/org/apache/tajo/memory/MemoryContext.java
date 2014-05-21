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

import org.apache.tajo.storage.columnar.UnsafeUtil;

/**
 * It's very simple memory manager.
 */
public class MemoryContext {
  private final String name;
  private volatile long maxMemory;
  private volatile long freeMemory;
  private volatile long objects = 0;

  public MemoryContext(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public long allocate(long memorySize) {
    if (freeMemory < 0) {
      return 0;
    }
    freeMemory -= memorySize;
    return UnsafeUtil.alloc(memorySize);
  }

  public void release(long ptr) {
    UnsafeUtil.free(ptr);
  }

  public long objectCounts() {
    return objects;
  }

  public long allocatedMemory() {
    return maxMemory - freeMemory;
  }

  public long freeMemory() {
    return freeMemory;
  }
}
