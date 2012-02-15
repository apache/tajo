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
public abstract class LogicalNode implements Cloneable {
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
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof LogicalNode) {
	    LogicalNode other = (LogicalNode) obj;
	    
	    return this.type == other.type 
	        && this.inputSchema.equals(other.inputSchema) 
	        && this.outputSchema.equals(other.outputSchema)
	        && this.cost == other.cost;
	  } else {
	    return false;
	  }
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  LogicalNode node = (LogicalNode)super.clone();
	  node.type = type;
	  node.inputSchema = (Schema) inputSchema.clone();
	  node.outputSchema = (Schema) outputSchema.clone();
	  
	  return node;
	}
	
	public abstract String toJSON();

  public abstract void accept(LogicalNodeVisitor visitor);
}
