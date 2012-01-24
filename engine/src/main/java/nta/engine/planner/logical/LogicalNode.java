/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.planner.LogicalPlanner.TargetList;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class LogicalNode {
	private ExprType type;
	private TargetList inputSchema;
	private TargetList outputSchema;

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
	
	public void setInputSchema(TargetList inSchema) {
	  this.inputSchema = inSchema;
	}
	
	public TargetList getInputSchema() {
	  return this.inputSchema;
	}
	
	public void setOutputSchema(TargetList outSchema) {
	  this.outputSchema = outSchema;
	}
	
	public TargetList getOutputSchema() {
	  return this.outputSchema;
	}
}
