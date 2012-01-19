package nta.engine.planner.logical;

import nta.catalog.Schema;
import nta.engine.parser.QueryBlock.Target;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class ProjectionNode extends UnaryNode {
	private Target [] targets;
	private Schema schema;

	public ProjectionNode(Target [] targets) {		
		super(ExprType.PROJECTION);
		this.schema = new Schema();
		this.targets = targets;
	}
	
	public Target [] getTargetList() {
	  return this.targets;
	}

	@Override
	public Schema getOutputSchema() {
	  // TODO - to be completed
		return getSubNode().getOutputSchema();
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder("Projection: ");
	  
	  for (int i = 0; i < targets.length; i++) {
	    sb.append(targets[i]);
	    if( i < targets.length - 1) {
	      sb.append(",");
	    }
	  }
	  
	  return sb.toString()+"\n"
	      + getSubNode().toString();
	}
}
