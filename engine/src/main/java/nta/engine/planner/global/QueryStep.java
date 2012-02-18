/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.List;

import nta.engine.QueryStepId;
import nta.engine.SubQueryId;

/**
 * @author jihoon
 *
 */
public class QueryStep {
  
  public enum Phase {
    LOCAL,
    MAP,
    MERGE
  }
  
  private final QueryStepId id;
  private Phase phase;
  
	private List<QueryUnit> queries;
	
	public QueryStep(QueryStepId queryStepId) {
	  this.id = queryStepId;
		queries = new ArrayList<QueryUnit>();
	}
	
	public void addQuery(QueryUnit q) {
		this.queries.add(q);
	}
	
	public void addQueries(QueryUnit [] queries) {
	  for (QueryUnit q : queries) {
	    this.addQuery(q);
	  }
	}
	
	public void setPhase(Phase phase) {
	  this.phase = phase;
	}
	
	public void removeQuery(QueryUnit q) {
	  for (int i = 0; i < queries.size(); i++) {
	    if (queries.get(i).getId().equals(q.getId())) {
	      queries.remove(i);
	    }
	  }
	}
	
	public QueryStepId getId() {
	  return this.id;
	}
	
	public QueryUnit getQuery(int index) {
		return this.queries.get(index);
	}
	
	public Phase getPhase() {
	  return this.phase;
	}
	
	public int size() {
		return queries.size();
	}
}
