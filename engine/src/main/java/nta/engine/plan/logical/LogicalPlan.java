/**
 * 
 */
package nta.engine.plan.logical;

/**
 * @author Hyunsik Choi
 *
 */
public class LogicalPlan {
	LogicalOp root;
	
	public LogicalPlan() {
		root = null;
	}
	
	public LogicalPlan(LogicalOp op) {
		this.root = op;
	}
	
	public void setRoot(LogicalOp op) {
		this.root = op;
	}
	
	public LogicalOp getRoot() {
		return this.root;
	}
}
