/**
 * 
 */
package nta.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;

/**
 * @author jihoon
 *
 */
public class Query extends AbstractQuery {

  private final QueryId id;
  private String queryStr;
  private Map<SubQueryId, SubQuery> subqueries;
  
  public Query(QueryId id, String queryStr) {
    this.id = id;
    this.queryStr = queryStr;
    subqueries = new HashMap<SubQueryId, SubQuery>();
  }
  
  public void addSubQuery(SubQuery q) {
    subqueries.put(q.getId(), q);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public String getQueryStr() {
    return this.queryStr;
  }

  public Iterator<SubQuery> getSubQueryIterator() {
    return this.subqueries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
  
  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }
  
  public ScheduleUnit getScheduleUnit(ScheduleUnitId id) {
    return this.getSubQuery(id.getSubQueryId()).getScheduleUnit(id);
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getScheduleUnit(id.getScheduleUnitId()).getQueryUnit(id);
  }
}
