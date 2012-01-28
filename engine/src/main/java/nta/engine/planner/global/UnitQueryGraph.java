/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public class UnitQueryGraph {

	private UnitQuery root;
	
	public UnitQueryGraph(UnitQuery root) {
		this.root = root;
	}
	
	public void setRoot(UnitQuery root) {
		this.root = root;
	}
	
	public UnitQuery getRoot() {
		return this.root;
	}
}
