/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryOp extends LogicalOp {
	LogicalOp subOp;
	
	/**
	 * @param opType
	 */
	public UnaryOp(OpType opType) {
		super(opType);
	}
	
	public void setSubOp(LogicalOp subOp) {
		this.subOp = subOp;
	}
	
	public LogicalOp getSubOp() {
		return this.subOp;
	}
	
	@Override
	public Schema getSchema() {
		return subOp.getSchema();
	}
}
