/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public class QueryUnitGraph {

	private QueryUnit root;
	
	public QueryUnitGraph(QueryUnit root) {
		this.root = root;
	}
	
	public void setRoot(QueryUnit root) {
		this.root = root;
	}
	
	public QueryUnit getRoot() {
		return this.root;
	}
}
