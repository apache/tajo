/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jihoon
 *
 */
public class QueryStep {

	private List<QueryUnit> queries;
	
	public QueryStep() {
		queries = new ArrayList<QueryUnit>();
	}
	
	public void addQuery(QueryUnit q) {
		this.queries.add(q);
	}
	
	public QueryUnit getQuery(int index) {
		return this.queries.get(index);
	}
	
	public int size() {
		return queries.size();
	}
}
