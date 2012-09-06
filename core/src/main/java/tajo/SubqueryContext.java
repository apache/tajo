/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import tajo.catalog.statistics.StatSet;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.ipc.protocolrecords.QueryUnitRequest;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;


/**
 * 실행 중인 subquery에 대한 정보를 담는다.
 *
 */
public class SubqueryContext extends Context {
  private final Map<String, List<Fragment>> fragmentMap
    = new HashMap<String, List<Fragment>>();
  
  private QueryStatus status;
  private final Map<String, StatSet> stats;
  private TableStat resultStats;
  private QueryUnitAttemptId queryId;
  private final File workDir;
  private boolean needFetch = false;
  private CountDownLatch doneFetchPhaseSignal;
  private float progress = 0;
  private Map<Integer, String> repartitions;
  private File fetchIn;
  private boolean stopped = false;
  private boolean interQuery = false;
  
  @VisibleForTesting
  SubqueryContext(final QueryUnitAttemptId queryId, final Fragment [] fragments, final File workDir) {
    this.queryId = queryId;
    
    for(Fragment t : fragments) {
      if (fragmentMap.containsKey(t.getId())) {
        fragmentMap.get(t.getId()).add(t);
      } else {
        List<Fragment> frags = new ArrayList<Fragment>();
        frags.add(t);
        fragmentMap.put(t.getId(), frags);
      }
    }
    
    stats = Maps.newHashMap();
    this.workDir = workDir;
    this.repartitions = Maps.newHashMap();
    
    status = QueryStatus.QUERY_INITED;
  }
  
  public QueryStatus getStatus() {
    synchronized (status) {
      return this.status;
    }
  }
  
  public void setStatus(QueryStatus status) {
    synchronized (status) {
      this.status = status;
    }
  }
  
  public void addStatSet(String name, StatSet stats) {
    this.stats.put(name, stats);
  }
  
  public StatSet getStatSet(String name) {
    return stats.get(name);
  }
  
  public Iterator<Entry<String, StatSet>> getAllStats() {
    return stats.entrySet().iterator();
  }

  public boolean hasResultStats() {
    return resultStats != null;
  }

  public void setResultStats(TableStat stats) {
    this.resultStats = stats;
  }

  public TableStat getResultStats() {
    return this.resultStats;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }

  public void setInterQuery() {
    this.interQuery = true;
  }

  public boolean isInterQuery() {
    return this.interQuery;
  }
  
  public void stop() {
    this.stopped = true;
  }
  
  public void addFetchPhase(int count, File fetchIn) {
    this.needFetch = true;
    this.doneFetchPhaseSignal = new CountDownLatch(count);
    this.fetchIn = fetchIn;
  }
  
  public File getFetchIn() {
    return this.fetchIn;
  }
  
  public boolean hasFetchPhase() {
    return this.needFetch;
  }
  
  public CountDownLatch getFetchLatch() {
    return doneFetchPhaseSignal;
  }
  
  public void addRepartition(int partKey, String path) {
    repartitions.put(partKey, path);
  }
  
  public Iterator<Entry<Integer,String>> getRepartitions() {
    return repartitions.entrySet().iterator();
  }
  
  public void clearFragment() {
    this.fragmentMap.clear();
  }
  
  public void changeFragment(String tableId, Fragment [] fragments) {
    fragmentMap.remove(tableId);
    for(Fragment t : fragments) {
      if (fragmentMap.containsKey(t.getId())) {
        fragmentMap.get(t.getId()).add(t);
      } else {
        List<Fragment> frags = new ArrayList<Fragment>();
        frags.add(t);
        fragmentMap.put(t.getId(), frags);
      }
    }
  }
  
  public static class Factory {
    public Factory() {
    }
    
    @VisibleForTesting
    public SubqueryContext create(QueryUnitAttemptId id, Fragment [] frags,
        File workDir) {
      return new SubqueryContext(id, frags, workDir);
    }
    
    public SubqueryContext create(QueryUnitRequest request, File workDir) {
      return new SubqueryContext(request.getId(), 
          request.getFragments().toArray(
              new Fragment [request.getFragments().size()]), workDir);
    }
  }
  
  public File getWorkDir() {
    return this.workDir;
  }
  
  public QueryUnitAttemptId getQueryId() {
    return this.queryId;
  }
  
  public float getProgress() {
    return this.progress;
  }
  
  public void setProgress(float progress) {
    this.progress = progress;
  }

  @Override
  public Fragment getTable(String id) {
    return fragmentMap.get(id).get(0);
  }

  public int getFragmentSize() {
    return fragmentMap.size();
  }
  
  @Override
  public Collection<String> getInputTables() {
    return fragmentMap.keySet();
  }
  
  public Fragment [] getTables(String id) {
    return fragmentMap.get(id).toArray(new Fragment[fragmentMap.get(id).size()]);
  }
  
  public int hashCode() {
    return Objects.hashCode(queryId);
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof SubqueryContext) {
      SubqueryContext other = (SubqueryContext) obj;
      return queryId.equals(other.getQueryId());
    } else {
      return false;
    }
  }
}