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

package org.apache.tajo.engine.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public interface CacheHolder<T> {

  /**
   * Get a shared data from the TableCache.
   */
  T acquire() throws Exception;

  /**
   * Release a cache to the memory.
   */
  boolean release();

  /**
   * This is a cache-holder for a join table
   * It will release when execution block is finished
   */
  public static abstract class BasicCacheHolder<T> implements CacheHolder<T> {

    protected T cached;
    protected Exception ex;
    protected final AtomicInteger counter = new AtomicInteger(-1);
    protected final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public T acquire() throws Exception {
      if (counter.compareAndSet(-1, 0)) {
        try {
          this.cached = init();
        } catch (Exception e) {
          this.ex = e;
        } finally {
          latch.countDown();
        }
      }
      latch.await();
      if (ex != null) {
        throw ex;
      }
      counter.incrementAndGet();
      return cached;
    }

    protected abstract T init() throws Exception;

    protected abstract void clear(T cached);

    @Override
    public boolean release() {
      if (counter.decrementAndGet() == 0) {
        clear(cached);
        return true;
      }
      return false;
    }
  }
}
