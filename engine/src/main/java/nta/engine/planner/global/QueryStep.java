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

	private List<UnitQuery> queries;
	
	public QueryStep() {
		queries = new ArrayList<UnitQuery>();
	}
	
	public void addQuery(UnitQuery q) {
		this.queries.add(q);
	}
	
	public UnitQuery getQuery(int index) {
		return this.queries.get(index);
	}
	
	public int size() {
		return queries.size();
	}
}
