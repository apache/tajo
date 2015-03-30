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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;

import java.util.List;
import java.util.Map;

/**
 * This is a simple TableCache which just added CacheHolder as needed.
 */
 public class TableCache {
  public static final Log LOG = LogFactory.getLog(TableCache.class);

  private static TableCache instance;
  private Map<TableCacheKey, CacheHolder<?>> cacheMap = Maps.newHashMap();

  private TableCache() {
  }

  public static synchronized TableCache getInstance() {
    if (instance == null) {
      instance = new TableCache();
    }
    return instance;
  }

  public Object getLock() {
    return this;
  }

  public synchronized void releaseCache(ExecutionBlockId ebId) {
    final String target = ebId.toString();
    List<TableCacheKey> purge = Lists.newArrayList();
    for (Map.Entry<TableCacheKey, CacheHolder<?>> entry : cacheMap.entrySet()) {
      TableCacheKey eachKey = entry.getKey();
      if (eachKey.ebId.equals(target) && entry.getValue().release()) {
        purge.add(eachKey);
      }
    }
    for (TableCacheKey cacheKey: purge) {
      cacheMap.remove(cacheKey).release();
      LOG.info("Removed Broadcast Table Cache: " + cacheKey.getTableName() + " EbId: " + cacheKey.ebId);
    }
  }

  public synchronized void addCache(TableCacheKey cacheKey, CacheHolder<?> cacheData) {
    cacheMap.put(cacheKey, cacheData);
    LOG.info("Added Broadcast Table Cache: " + cacheKey.getTableName() + " EbId: " + cacheKey.ebId);
  }

  public synchronized boolean hasCache(TableCacheKey cacheKey) {
    return cacheMap.containsKey(cacheKey);
  }

  @SuppressWarnings("unchecked")
  public synchronized <T> CacheHolder<T> getCache(TableCacheKey cacheKey) {
    return (CacheHolder<T>) cacheMap.get(cacheKey);
  }
}
