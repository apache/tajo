/**
 * 
 */
package nta.engine;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author jihoon
 *
 */
public class Query {

  private final QueryId id;
  private Map<SubQueryId, SubQuery> subqueries;
  
  public Query(QueryId id) {
    this.id = id;
    subqueries = new LinkedHashMap<SubQueryId, SubQuery>();
  }
  
  public void addSubQuery(SubQuery q) {
    subqueries.put(q.getId(), q);
  }
  
  public QueryId getId() {
    return this.id;
  }
  
  public Iterator<SubQuery> getSubQueryIterator() {
    return this.subqueries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
}
