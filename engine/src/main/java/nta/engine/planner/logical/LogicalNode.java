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
	private Schema inputSchema;
	private Schema outputSchema;

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
	
	public void setInputSchema(Schema inSchema) {
	  this.inputSchema = inSchema;
	}
	
	public Schema getInputSchema() {
	  return this.inputSchema;
	}
	
	public void setOutputSchema(Schema outSchema) {
	  this.outputSchema = outSchema;
	}
	
	public Schema getOutputSchema() {
	  return this.outputSchema;
	}
}
