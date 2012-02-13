/**
 * 
 */
package nta.engine;

import java.util.HashMap;
import java.util.Map;

import nta.engine.planner.global.QueryUnit;

/**
 * @author jihoon
 *
 */
public class SubQuery {

  private final SubQueryId id;
  private Map<QueryUnitId, QueryUnit> queryunits;
  
  public SubQuery(SubQueryId id) {
    this.id = id;
  }
  
  public SubQuery(SubQueryId id, Map<QueryUnitId, QueryUnit> queryUnits) {
    this(id);
    this.set(queryUnits);
  }
  
  public void set(Map<QueryUnitId, QueryUnit> queryUnits) {
    this.queryunits = queryUnits;
  }
  
  public void addQueryUnit(QueryUnit q) {
    if (queryunits == null) {
      queryunits = new HashMap<QueryUnitId, QueryUnit>();
    }
    queryunits.put(q.getId(), q);
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public Map<QueryUnitId, QueryUnit> getQueryUnits() {
    return this.queryunits;
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.queryunits.get(id);
  }
}
