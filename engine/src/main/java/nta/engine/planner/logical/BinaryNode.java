/**
 * 
 */
package nta.engine.planner.logical;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class BinaryNode extends LogicalNode {
	LogicalNode outer = null;
	LogicalNode inner = null;
	
	/**
	 * @param opType
	 */
	public BinaryNode(ExprType opType) {
		super(opType);
	}
	
	public LogicalNode getOuter() {
		return this.outer;
	}
	
	public void setOuter(LogicalNode op) {
		this.outer = op;
	}

	public LogicalNode getInner() {
		return this.inner;
	}

	public void setInner(LogicalNode op) {
		this.inner = op;
	}
}
