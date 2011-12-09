/**
 * 
 */
package nta.engine.plan.logical;

import nta.catalog.Schema;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class LogicalOp {

	private OpType opType;

	private double cost = 0;

	public LogicalOp(OpType opType) {
		this.opType = opType;
	}
	
	public OpType getType() {
		return this.opType;
	}

	public void setType(OpType type) {
		this.opType = type;
	}

	public double getCost() {
		return this.cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}
	
	public abstract Schema getSchema();
}
