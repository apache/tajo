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

import io.netty.buffer.*;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.PlatformDependent;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/* this class is PooledBuffer holder */
public class BufferPool {

  public static final String ALLOW_CACHE = "tajo.storage.buffer.thread-local.cache";
  private static final ByteBufAllocator ALLOCATOR;

  private BufferPool() {
  }

  static {

    if (TajoConstants.IS_TEST_MODE) {
      /* Disable pooling buffers for memory usage  */
      ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

      /* if you are finding memory leak, please enable this line */
      ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    } else {
      TajoConf tajoConf = new TajoConf();
      ALLOCATOR = createPooledByteBufAllocator(true, tajoConf.getBoolean(ALLOW_CACHE, true), 0);
    }
  }

  /**
   * borrowed from Spark
   */
  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs,
      boolean allowCache,
      int numCores) {
    if (numCores == 0) {
      numCores = Runtime.getRuntime().availableProcessors();
    }
    return new PooledByteBufAllocator(
        allowDirectBufs && PlatformDependent.directBufferPreferred(),
        Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),
        Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
        getPrivateStaticField("DEFAULT_PAGE_SIZE"),
        getPrivateStaticField("DEFAULT_MAX_ORDER"),
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
    return directBuffer(size, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * @param size  the initial capacity
   * @param order the endianness
   * @return allocated ByteBuf from pool
   */
  public static ByteBuf directBuffer(int size, ByteOrder order) {
    ByteBuf byteBuf = ALLOCATOR.directBuffer(size);
    if (byteBuf.order() != order) byteBuf.order(order);
    return byteBuf;
  }

  /**
   * @param size the initial capacity
   * @param max  the max capacity
   * @return allocated ByteBuf from pool
   */
  public static ByteBuf directBuffer(int size, int max) {
    return directBuffer(size, max, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * @param size  the initial capacity
   * @param max   the max capacity
   * @param order the endianness
   * @return allocated ByteBuf from pool
   */
  public static ByteBuf directBuffer(int size, int max, ByteOrder order) {
    ByteBuf byteBuf = ALLOCATOR.directBuffer(size, max);
    if (byteBuf.order() != order) byteBuf.order(order);
    return byteBuf;
  }

  /**
   *
   * @param size the initial capacity
   * @param max the max capacity
   * @return heap ByteBuf
   */
  public static ByteBuf heapBuffer(int size, int max) {
    return Unpooled.buffer(size, max).order(ByteOrder.LITTLE_ENDIAN);
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
  public static ByteBuf ensureWritable(ByteBuf buf, int minWritableBytes) {
    return buf.ensureWritable(minWritableBytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * deallocate the specified direct memory
   * @param byteBuffer
   */
  public static void free(ByteBuffer byteBuffer) {
    PlatformDependent.freeDirectBuffer(byteBuffer);
  }

  /**
   * get the specified direct memory bean
   */
  public static BufferPoolMXBean getDirectBufferPool() {
    for (BufferPoolMXBean pool : getBufferPools()) {
      if (pool.getName().equals("direct")) {
        return pool;
      }
    }
    return null;
  }

  /**
   * get the specified mapped memory bean
   */
  public static BufferPoolMXBean getMappedBufferPool() {
    for (BufferPoolMXBean pool : getBufferPools()) {
      if (pool.getName().equals("mapped")) {
        return pool;
      }
    }
    return null;
  }

  private static List<BufferPoolMXBean> getBufferPools() {
    return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
  }
}
