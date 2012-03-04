/**
 * 
 */
package nta.engine.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import nta.engine.Query;
import nta.engine.QueryId;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.SubQuery;
import nta.engine.SubQueryId;
import nta.engine.planner.global.QueryUnit;

import com.google.common.collect.MapMaker;

/**
 * @author jihoon
 *
 */
public class QueryManager {

  private final static long EXPIRE_TIME = 3000;
  
  public class WaitStatus {
    private InProgressStatus status;
    private long expire;
    
    public WaitStatus(InProgressStatus status, long expire) {
      set(status, expire);
    }
    
    public WaitStatus set(InProgressStatus status, long expire) {
      this.status = status;
      this.expire = expire;
      return this;
    }
    
    public WaitStatus reset() {
      this.expire = EXPIRE_TIME;
      return this;
    }
    
    public void update(long period) {
      this.expire -= period;
    }
    
    public InProgressStatus getInProgressStatus() {
      return this.status;
    }
    
    public long getLeftTime() {
      return this.expire;
    }
  }
  
  private Map<QueryId, Query> queries;
  private Map<QueryUnitId, WaitStatus> inProgressQueries;
  
  public QueryManager() {
    queries = new MapMaker().concurrencyLevel(4).makeMap();
    inProgressQueries = new MapMaker().concurrencyLevel(4).makeMap();
  }
  
  public synchronized void addQuery(Query q) {
    queries.put(q.getId(), q);
  }
  
  public void addSubQuery(SubQuery subQuery) {
    // TODO
  }
  
  public void addQueryUnit(QueryUnit queryUnit) {
    // TODO 
  }
  
  public synchronized void updateProgress(QueryUnitId queryUnitId, 
      InProgressStatus progress) {
    if (inProgressQueries.containsKey(queryUnitId)) {
      inProgressQueries.put(queryUnitId, 
          inProgressQueries.get(queryUnitId).set(progress, EXPIRE_TIME));
    } else {
      inProgressQueries.put(queryUnitId, new WaitStatus(progress, EXPIRE_TIME));
    }
  }
  
  public Query getQuery(QueryId queryId) {
    return this.queries.get(queryId);
  }
  
  public SubQuery getSubQuery(SubQueryId subQueryId) {
    return null;
  }
  
  public QueryUnit getQueryUnit(QueryUnitId queryUnitId) {
    return null;
  }
  
  public WaitStatus getWaitStatus(QueryUnitId unitId) {
    return this.inProgressQueries.get(unitId);
  }
  
  public InProgressStatus getProgress(QueryUnitId queryUnitId) {
    if (inProgressQueries.containsKey(queryUnitId)) {
      return this.inProgressQueries.get(queryUnitId).status;
    } else {
      return null;
    }
  }
  
  public Map<QueryUnitId, InProgressStatus> getAllProgresses() {
    Map<QueryUnitId, InProgressStatus> map = 
        new HashMap<QueryUnitId, InProgressStatus>();
    Iterator<Entry<QueryUnitId, WaitStatus>> it = inProgressQueries.entrySet().iterator();
    while (it.hasNext()) {
      Entry<QueryUnitId, WaitStatus> e = it.next();
      map.put(e.getKey(), e.getValue().status);
    }
    return map;
  }
}
