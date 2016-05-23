/*
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

package org.apache.tajo.storage.thirdparty.orc;

import com.google.common.collect.ComparisonChain;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class ByteBufferAllocatorPool {
  private static final class Key implements Comparable<Key> {
    private final int capacity;
    private final long insertionGeneration;

    Key(int capacity, long insertionGeneration) {
      this.capacity = capacity;
      this.insertionGeneration = insertionGeneration;
    }

    @Override
    public int compareTo(Key other) {
      return ComparisonChain.start().compare(capacity, other.capacity)
          .compare(insertionGeneration, other.insertionGeneration).result();
    }

    @Override
    public boolean equals(Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        Key o = (Key) rhs;
        return (compareTo(o) == 0);
      } catch (ClassCastException e) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(capacity).append(insertionGeneration)
          .toHashCode();
    }
  }

  private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<Key, ByteBuffer>();

  private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<Key, ByteBuffer>();

  private long currentGeneration = 0;

  private final TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
    return direct ? directBuffers : buffers;
  }

  public void clear() {
    buffers.clear();
    directBuffers.clear();
  }

  public ByteBuffer getBuffer(boolean direct, int length) {
    TreeMap<Key, ByteBuffer> tree = getBufferTree(direct);
    Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0));
    if (entry == null) {
      return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer
          .allocate(length);
    }
    tree.remove(entry.getKey());
    return entry.getValue();
  }

  public void putBuffer(ByteBuffer buffer) {
    TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
    while (true) {
      Key key = new Key(buffer.capacity(), currentGeneration++);
      if (!tree.containsKey(key)) {
        tree.put(key, buffer);
        return;
      }
      // Buffers are indexed by (capacity, generation).
      // If our key is not unique on the first try, we try again
    }
  }
}
