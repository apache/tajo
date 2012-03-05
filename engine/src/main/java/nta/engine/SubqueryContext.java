/**
 * 
 */
package nta.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nta.catalog.CatalogService;
import nta.catalog.statistics.StatSet;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.ipc.protocolrecords.SubQueryRequest;

import com.google.common.annotations.VisibleForTesting;
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
  
  private final Map<String, StatSet> stats;
  private QueryUnitId queryId;
  
  @VisibleForTesting
  SubqueryContext(QueryUnitId queryId, Fragment [] fragments) {
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
  
  public static class Factory {
    private final CatalogService catalog;
    public Factory(CatalogService catalog) {
      this.catalog = catalog;
    }
    
    @VisibleForTesting
    public SubqueryContext create(QueryUnitId id, Fragment [] frags) {
      return new SubqueryContext(id, frags);
    }
    
    public SubqueryContext create(SubQueryRequest request) {
      return new SubqueryContext(request.getId(), 
          request.getFragments().toArray(
              new Fragment [request.getFragments().size()]));
    }
    
    public SubqueryContext create(QueryUnitRequest request) {
      return new SubqueryContext(request.getId(), 
          request.getFragments().toArray(
              new Fragment [request.getFragments().size()]));          
    }
  }
  
  public QueryUnitId getQueryId() {
    return this.queryId;
  }

  @Override
  public Fragment getTable(String id) {
    return fragmentMap.get(id).get(0);
  }
  
  public Fragment [] getTables(String id) {
    return fragmentMap.get(id).toArray(new Fragment[fragmentMap.size()]);
  }
}