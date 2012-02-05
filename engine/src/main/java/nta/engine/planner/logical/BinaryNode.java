/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class BinaryNode extends LogicalNode {
	@Expose
	LogicalNode outer = null;
	@Expose
	LogicalNode inner = null;
	
	public BinaryNode() {
		super();
	}
	
	/**
	 * @param opType
	 */
	public BinaryNode(ExprType opType) {
		super(opType);
	}
	
	public LogicalNode getRightSubNode() {
		return this.outer;
	}
	
	public void setOuter(LogicalNode op) {
		this.outer = op;
	}

	public LogicalNode getLeftSubNode() {
		return this.inner;
	}

	public void setInner(LogicalNode op) {
		this.inner = op;
	}
}
