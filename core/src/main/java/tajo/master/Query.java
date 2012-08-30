/**
 * 
 */
package tajo.master;

import com.google.common.collect.Maps;
import tajo.QueryId;
import tajo.QueryUnitId;
import tajo.ScheduleUnitId;
import tajo.SubQueryId;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.ScheduleUnit;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author jihoon
 *
 */
public class Query extends AbstractQuery {

  private final QueryId id;
  private String queryStr;
  private Map<SubQueryId, SubQuery> subqueries;
  private QueryStatus status;
  
  public Query(QueryId id, String queryStr) {
    this.id = id;
    this.queryStr = queryStr;
    subqueries = Maps.newHashMap();
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

  public QueryStatus getStatus() {
    return this.status;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }
}
