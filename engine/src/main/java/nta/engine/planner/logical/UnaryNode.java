/**
 * 
 */
package nta.engine.planner.logical;


/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryNode extends LogicalNode {
	LogicalNode subExpr;
	
	/**
	 * @param type
	 */
	public UnaryNode(ExprType type) {
		super(type);
	}
	
	public void setSubNode(LogicalNode subNode) {
		this.subExpr = subNode;
	}
	
	public LogicalNode getSubNode() {
		return this.subExpr;
	}
}
