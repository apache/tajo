/**
 * 
 */
package nta.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import nta.catalog.CatalogService;
import nta.catalog.statistics.StatSet;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;

import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;


/**
 * 실행 중인 subquery에 대한 정보를 담는다.
 * 
 * @author Hyunsik Choi
 *
 */
public class SubqueryContext extends Context {
  private final Map<String, List<Fragment>> fragmentMap
    = new HashMap<String, List<Fragment>>();
  
  private QueryStatus status;
  private final Map<String, StatSet> stats;
  private QueryUnitId queryId;
  private final Path workDir;
  private boolean needFetch = false;
  private CountDownLatch doneFetchPhaseSignal;
  private float progress = 0;
  private Map<Integer, String> repartitions;
  private File fetchIn;
  private boolean stopped = false;
  
  @VisibleForTesting
  SubqueryContext(QueryUnitId queryId, Fragment [] fragments, Path workDir) {    
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
    
    status = QueryStatus.INITED;
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
  
  public boolean isStopped() {
    return this.stopped;
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
  
  public void changeFragment(Fragment [] fragments) {
    this.fragmentMap.clear();
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
    @SuppressWarnings("unused")
    private final CatalogService catalog;
    public Factory(CatalogService catalog) {
      this.catalog = catalog;
    }
    
    @VisibleForTesting
    public SubqueryContext create(QueryUnitId id, Fragment [] frags, 
        Path workDir) {
      return new SubqueryContext(id, frags, workDir);
    }
    
    public SubqueryContext create(QueryUnitRequest request, Path workDir) {
      return new SubqueryContext(request.getId(), 
          request.getFragments().toArray(
              new Fragment [request.getFragments().size()]), workDir);
    }
  }
  
  public Path getWorkDir() {
    return this.workDir;
  }
  
  public QueryUnitId getQueryId() {
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