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

package org.apache.tajo.ws.rs;

import org.apache.tajo.QueryId;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.ws.rs.resources.ClusterResource;
import org.apache.tajo.ws.rs.resources.DatabasesResource;
import org.apache.tajo.ws.rs.resources.FunctionsResource;
import org.apache.tajo.ws.rs.resources.QueryResource;
import org.apache.tajo.ws.rs.resources.SessionsResource;
import org.apache.tajo.ws.rs.resources.TablesResource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.ws.rs.core.Application;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * It loads client classes for Tajo REST protocol.
 */
public class ClientApplication extends Application {

  private final MasterContext masterContext;
  private final ConcurrentMap<QueryId, Long> queryIdToResultSetCacheIdMap;
  private final Cache<Long, NonForwardQueryResultScanner> queryResultScannerCache;
  
  private final SecureRandom secureRandom;

  public ClientApplication(MasterContext masterContext) {
    this.masterContext = masterContext;
    
    this.secureRandom = new SecureRandom();
    
    this.queryIdToResultSetCacheIdMap = new ConcurrentHashMap<QueryId, Long>();
    this.queryResultScannerCache = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .maximumSize(1000)
        .expireAfterAccess(30, TimeUnit.MINUTES)
        .build();
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> classes = new HashSet<Class<?>>();
    
    classes.add(SessionsResource.class);
    classes.add(DatabasesResource.class);
    classes.add(TablesResource.class);
    classes.add(FunctionsResource.class);
    classes.add(ClusterResource.class);
    classes.add(QueryResource.class);
    
    return classes;
  }

  public MasterContext getMasterContext() {
    return masterContext;
  }
  
  /**
   * It returns generated 8-byte size integer.
   * 
   * @return
   */
  private long generateCacheId() {
    byte[] generatedBytes = new byte[8];
    long generatedId = 0;
    
    secureRandom.nextBytes(generatedBytes);
    for (byte generatedByte: generatedBytes) {
      generatedId = (generatedId << 8) + (generatedByte & 0xff);
    }

    if (generatedId < 0) {
      generatedId = generatedId * -1;
    }

    return generatedId;
  }
  
  /**
   * If cannot find any cache id for supplied query id, it will generate a new cache id.
   * 
   * @param queryId
   * @return
   */
  public long generateCacheIdIfAbsent(QueryId queryId) {
    Long cacheId = this.queryIdToResultSetCacheIdMap.get(queryId);
    long newCacheId = 0;
    
    if (cacheId == null) {
      boolean generated = false;
      do {
        newCacheId = generateCacheId();
        if (queryResultScannerCache.getIfPresent(newCacheId) == null) {
          generated = true;
        }
      } while (!generated);
      cacheId = this.queryIdToResultSetCacheIdMap.putIfAbsent(queryId, newCacheId);
      if (cacheId != null) {
        newCacheId = cacheId.longValue();
      }
    } else {
      newCacheId = cacheId.longValue();
    }
    
    return newCacheId;
  }
  
  /**
   * get cached NonForwardResultScanner instance by query id and cache id.
   * 
   * @param queryId
   * @param cacheId
   * @return
   */
  public NonForwardQueryResultScanner getCachedNonForwardResultScanner(QueryId queryId, long cacheId) {
    Long cachedCacheId = queryIdToResultSetCacheIdMap.get(queryId);
    
    if (cachedCacheId == null) {
      throw new RuntimeException("Supplied cache id " + cacheId + " was expired or invalid.");
    }
    
    if (cacheId != cachedCacheId.longValue()) {
      throw new RuntimeException("Supplied cache id " + cacheId + " was expired or invalid. " +
          "Please use the valid cache id.");
    }
    
    return queryResultScannerCache.getIfPresent(cachedCacheId);
  }
  
  /**
   * Store NonForwardResultScanner instance to cached memory if not present.
   * 
   * @param queryId
   * @param cacheId
   * @param resultScanner
   */
  public NonForwardQueryResultScanner setCachedNonForwardResultScanner(QueryId queryId, long cacheId, 
      NonForwardQueryResultScanner resultScanner) {
    NonForwardQueryResultScanner cachedScanner = null;
    
    if (cacheId == 0) {
      cacheId = generateCacheIdIfAbsent(queryId);
    }
    
    cachedScanner = getCachedNonForwardResultScanner(queryId, cacheId);
    if (cachedScanner == null) {
      cachedScanner = this.queryResultScannerCache.asMap().putIfAbsent(cacheId, resultScanner);
      if (cachedScanner == null) {
        cachedScanner = resultScanner;
      }
    }
    
    return cachedScanner;
  }
  
}
