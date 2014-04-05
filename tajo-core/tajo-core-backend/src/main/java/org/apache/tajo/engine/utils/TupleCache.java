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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleCache {
  private static TupleCache instance;

  private Map<TupleCacheKey, List<Tuple>> broadcastTupleCacheData
      = new HashMap<TupleCacheKey, List<Tuple>>();
  private Map<TupleCacheKey, TupleCacheStatus> broadcastTupleCacheStatus
      = new HashMap<TupleCacheKey, TupleCacheStatus>();

  private Object lockMonitor = new Object();

  public static enum TupleCacheStatus {
    STARTED,
    ENDED
  };

  private TupleCache() {
  }

  public static synchronized TupleCache getInstance() {
    if (instance == null) {
      instance = new TupleCache();
    }
    return instance;
  }

  public Object getLockMonitor() {
    return lockMonitor;
  }

  public void removeBroadcastCache(ExecutionBlockId ebId) {
    if (ebId == null) {
      return;
    }
    synchronized (lockMonitor) {
      TupleCacheKey matchedKey = null;
      for (TupleCacheKey eachKey: broadcastTupleCacheStatus.keySet()) {
        if (eachKey.ebId.equals(ebId.toString())) {
          matchedKey = eachKey;
          break;
        }
      }
      if (matchedKey != null) {
        broadcastTupleCacheStatus.remove(matchedKey);
        broadcastTupleCacheData.remove(matchedKey);
      }
    }
  }

  public void addBroadcastCache(TupleCacheKey cacheKey, List<Tuple> cacheData) {
    synchronized (lockMonitor) {
      if (broadcastTupleCacheStatus.containsKey(cacheKey) &&
          broadcastTupleCacheStatus.get(cacheKey) == TupleCacheStatus.ENDED) {
        return;
      }
      broadcastTupleCacheData.put(cacheKey, cacheData);
      broadcastTupleCacheStatus.put(cacheKey, TupleCacheStatus.ENDED);
      lockMonitor.notifyAll();
    }
  }

  public boolean lockBroadcastScan(TupleCacheKey cacheKey) {
    synchronized (lockMonitor) {
      if (broadcastTupleCacheStatus.containsKey(cacheKey)) {
        return false;
      } else {
        broadcastTupleCacheStatus.put(cacheKey, TupleCacheStatus.STARTED);
        return true;
      }
    }
  }

  public boolean isBroadcastCacheReady(TupleCacheKey cacheKey) {
    synchronized (lockMonitor) {
      if (!broadcastTupleCacheStatus.containsKey(cacheKey)) {
        return false;
      }
      return broadcastTupleCacheStatus.get(cacheKey) == TupleCacheStatus.ENDED;
    }
  }

  public TupleCacheScanner openCacheScanner(TupleCacheKey cacheKey, Schema schema) throws IOException {
    synchronized (lockMonitor) {
      List<Tuple> cacheData = broadcastTupleCacheData.get(cacheKey);
      if (cacheData != null) {
        TupleCacheScanner scanner = new TupleCacheScanner(cacheData, schema);
        scanner.init();
        return scanner;
      } else {
        return null;
      }
    }
  }
}
