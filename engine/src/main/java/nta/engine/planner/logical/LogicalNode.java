/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Schema;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class LogicalNode {
	@Expose
	private ExprType type;
	@Expose
	private Schema inputSchema;
	@Expose
	private Schema outputSchema;

	@Expose
	private double cost = 0;
	
	public LogicalNode() {
		
	}

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
	
	public abstract String toJSON();
}
