/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Schema;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class LogicalNode {

	private ExprType type;

	private double cost = 0;

	public LogicalNode(ExprType type) {
		this.type = type;
	}
	
	public ExprType getType() {
		return this.type;
	}

	public void setType(ExprType type) {
		this.type = type;
	}

	public double getCost() {
		return this.cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}
	
	public abstract Schema getOutputSchema();
}
