/**
 * 
 */
package nta.engine.plan.logical;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class BinaryOp extends LogicalOp {
	LogicalOp outer = null;
	LogicalOp inner = null;
	
	/**
	 * @param opType
	 */
	public BinaryOp(OpType opType) {
		super(opType);
	}
	
	public LogicalOp getOuter() {
		return this.outer;
	}
	
	public void setOuter(LogicalOp op) {
		this.outer = op;
	}

	public LogicalOp getInner() {
		return this.inner;
	}

	public void setInner(LogicalOp op) {
		this.inner = op;
	}
}
