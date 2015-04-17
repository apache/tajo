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

package org.apache.tajo.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.util.CommonTestingUtil;

import java.lang.reflect.Field;

/* this class is PooledBuffer holder */
public class BufferPool {

  private static final PooledByteBufAllocator allocator;

  private BufferPool() {
  }

  static {
    /* TODO Enable thread cache
    *  Create a pooled ByteBuf allocator but disables the thread-local cache.
    *  Because the TaskRunner thread is newly created
    * */

     System.getProperty(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);
    int maxOrder = 11; // 16MiB chunkSize = pageSize << maxOrder
    if (System.getProperty(CommonTestingUtil.TAJO_TEST_KEY, "FALSE").equalsIgnoreCase("TRUE")) {
       maxOrder = 7; //1MiB chunk
    }
    allocator = createPooledByteBufAllocator(true, false, 0, maxOrder);

    /* if you are finding memory leak, please enable this line */
    //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
  }

  /**
   * borrowed from Spark
   */
  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs,
      boolean allowCache,
      int maxOrder,
      int numCores) {
    if (numCores == 0) {
      numCores = Runtime.getRuntime().availableProcessors();
    }
    return new PooledByteBufAllocator(
        allowDirectBufs && PlatformDependent.directBufferPreferred(),
        Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),
        Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
        getPrivateStaticField("DEFAULT_PAGE_SIZE"),
        Math.min(getPrivateStaticField("DEFAULT_MAX_ORDER"), maxOrder),
        allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
        allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
        allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0
    );
  }

  /** Used to get defaults from Netty's private static fields. */
  private static int getPrivateStaticField(String name) {
    try {
      Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
