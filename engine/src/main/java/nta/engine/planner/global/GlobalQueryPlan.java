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
public class GlobalQueryPlan {

	private List<QueryStep> querySteps;
	
	public GlobalQueryPlan() {
		querySteps = new ArrayList<QueryStep>();
	}
	
	public void addQueryStep(QueryStep step) {
		querySteps.add(step);
	}
	
	public QueryStep getQueryStep(int index) {
		return querySteps.get(index);
	}
	
	public int size() {
		return querySteps.size();
	}
}
