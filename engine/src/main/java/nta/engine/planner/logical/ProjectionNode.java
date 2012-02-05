package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock.Target;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class ProjectionNode extends UnaryNode {
	@Expose
	private Target [] targets;
	
	public ProjectionNode() {
		super();
	}

	public ProjectionNode(Target [] targets) {		
		super(ExprType.PROJECTION);
		this.targets = targets;
	}
	
	public Target [] getTargetList() {
	  return this.targets;
	}
	
	public boolean isAll() {
	  return targets == null;
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("\"Projection\": {\"targets\": [");
	  
	  for (int i = 0; i < targets.length; i++) {
	    sb.append("\"").append(targets[i]).append("\"");
	    if( i < targets.length - 1) {
	      sb.append(",");
	    }
	  }
	  sb.append("],");
	  sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",");
	  sb.append("\n  \"in schema\": ").append(getInputSchema());    
	  sb.append("}");
	  return sb.toString()+"\n"
	      + getSubNode().toString();
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalNode.class);
	}
}
