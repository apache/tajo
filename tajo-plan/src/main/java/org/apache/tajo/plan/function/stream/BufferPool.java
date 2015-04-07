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

package org.apache.tajo.plan.function.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.apache.hadoop.classification.InterfaceStability;

/* this class is PooledBuffer holder */
public class BufferPool {

  private static final PooledByteBufAllocator allocator;

  private BufferPool() {
  }

  static {
    //TODO we need determine the default params
    allocator = new PooledByteBufAllocator(PlatformDependent.directBufferPreferred());

    /* if you are finding memory leak, please enable this line */
    //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
  }

  public static long maxDirectMemory() {
    return PlatformDependent.maxDirectMemory();
  }


  public static ByteBuf directBuffer(int size) {
    return allocator.directBuffer(size);
  }

  /**
   *
   * @param size the initial capacity
   * @param max the max capacity
   * @return allocated ByteBuf from pool
   */
  public static ByteBuf directBuffer(int size, int max) {
    return allocator.directBuffer(size, max);
  }

  @InterfaceStability.Unstable
  public static void forceRelease(ByteBuf buf) {
    buf.release(buf.refCnt());
  }

  /**
   * the ByteBuf will increase to writable size
   * @param buf
   * @param minWritableBytes required minimum writable size
   */
  public static void ensureWritable(ByteBuf buf, int minWritableBytes) {
    buf.ensureWritable(minWritableBytes);
  }
}
