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

package org.apache.tajo.engine.util;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestTableCache {

  @Test
  public void testBroadcastTableCache() throws Exception {

    ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(
        QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));

    final TableCacheKey key = new TableCacheKey(ebId.toString(), "testBroadcastTableCache", "path");
    final ExecutionBlockSharedResource resource = new ExecutionBlockSharedResource();

    final int parallelCount = 30;
    ExecutorService executor = Executors.newFixedThreadPool(parallelCount);
    List<Future<CacheHolder<Long>>> tasks = new ArrayList<>();
    for (int i = 0; i < parallelCount; i++) {
      tasks.add(executor.submit(createTask(key, resource)));
    }

    long expected = tasks.get(0).get().getData().longValue();

    for (Future<CacheHolder<Long>> future : tasks) {
      assertEquals(expected, future.get().getData().longValue());
    }

    resource.releaseBroadcastCache(ebId);
    assertFalse(resource.hasBroadcastCache(key));
    executor.shutdown();
  }

  private Callable<CacheHolder<Long>> createTask(final TableCacheKey key, final ExecutionBlockSharedResource resource) {
    return () -> {
      CacheHolder<Long> result;
      synchronized (resource.getLock()) {
        if (!TableCache.getInstance().hasCache(key)) {
          final long nanoTime = System.nanoTime();
          final TableStats tableStats = new TableStats();
          tableStats.setNumRows(100);
          tableStats.setNumBytes(1000);

          final CacheHolder<Long> cacheHolder = new CacheHolder<Long>() {

            @Override
            public Long getData() {
              return nanoTime;
            }

            @Override
            public TableStats getTableStats() {
              return tableStats;
            }

            @Override
            public void release() {

            }
          };

          resource.addBroadcastCache(key, cacheHolder);
        }
      }

      CacheHolder<?> holder = resource.getBroadcastCache(key);
      result = (CacheHolder<Long>) holder;
      return result;
    };
  }
}
