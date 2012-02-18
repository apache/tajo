/**
 * 
 */
package nta.engine.cluster;

import java.util.Map;

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

  private Map<QueryId, Query> queries;
  private Map<QueryUnitId, InProgressStatus> inProgressQueries;
  
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
  
  public synchronized void updateProgress(QueryUnitId queryUnitId, InProgressStatus progress) {
    inProgressQueries.put(queryUnitId, progress);
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
  
  public InProgressStatus getProgress(QueryUnitId queryUnitId) {
    InProgressStatus progress = this.inProgressQueries.get(queryUnitId);
    return progress;
  }
  
  public Map<QueryUnitId, InProgressStatus> getAllProgresses() {
    return this.inProgressQueries;
  }
}
