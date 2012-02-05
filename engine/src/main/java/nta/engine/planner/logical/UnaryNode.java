/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;


/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryNode extends LogicalNode {
	@Expose
	LogicalNode subExpr;
	
	public UnaryNode() {
		super();
	}
	
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
