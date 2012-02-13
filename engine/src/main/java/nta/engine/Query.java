/**
 * 
 */
package nta.engine;

import java.util.HashMap;
import java.util.Map;

import nta.datum.Datum;

/**
 * @author jihoon
 *
 */
public class Query {

  private final QueryId id;
  private Map<SubQueryId, SubQuery> subqueries;
  
  private Map<String, Datum> scalarVars;
  private Map<String, String> tableVars;
  
  public Query(QueryId id) {
    this.id = id;
    scalarVars = new HashMap<String, Datum>();
    tableVars = new HashMap<String, String>();
  }
  
  public Query(QueryId id, Map<SubQueryId, SubQuery> subqueries) {
    this(id);
    this.subqueries = subqueries;
  }
  
  public void set(Map<SubQueryId, SubQuery> subqueries) {
    this.subqueries = subqueries;
  }
  
  public void addSubQuery(SubQuery q) {
    if (subqueries == null) {
      subqueries = new HashMap<SubQueryId, SubQuery>();
    }
    subqueries.put(q.getId(), q);
  }
  
  public void setScalarVar(String name, Datum val) {
    scalarVars.put(name, val);
  }
  
  public void setTableVar(String name, String table) {
    tableVars.put(name, table);
  }
  
  public QueryId getId() {
    return this.id;
  }
  
  public Map<SubQueryId, SubQuery> getSubQueries() {
    return this.subqueries;
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
  
  public Datum getScalarVar(String name) {
    return this.scalarVars.get(name);
  }
  
  public String getTableVar(String name) {
    return this.tableVars.get(name);
  }
}
