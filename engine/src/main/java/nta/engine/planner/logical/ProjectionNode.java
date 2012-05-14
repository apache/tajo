package nta.engine.planner.logical;

import java.util.Arrays;

import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock.Target;

import com.google.gson.annotations.Expose;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class ProjectionNode extends UnaryNode {
  /**
   * the targets are always filled even if the query is 'select *'
   */
  @Expose	private Target [] targets;
  @Expose private boolean distinct = false;

  /**
   * This method is for gson.
   */
	private ProjectionNode() {
		super();
	}

	public ProjectionNode(Target [] targets) {		
		super(ExprType.PROJECTION);
		this.targets = targets;
	}
	
	public Target [] getTargetList() {
	  return this.targets;
	}
	
	public void setSubNode(LogicalNode subNode) {
	  super.setSubNode(subNode);
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("\"Projection\": {");
    if (distinct) {
      sb.append("\"distinct\": true, ");
    }
    sb.append("\"targets\": [");
	  
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
	
	@Override
  public boolean equals(Object obj) {
	  if (obj instanceof ProjectionNode) {
	    ProjectionNode other = (ProjectionNode) obj;
	    
	    boolean b1 = super.equals(other);
	    boolean b2 = Arrays.equals(targets, other.targets);
	    boolean b3 = subExpr.equals(other.subExpr);
	    
	    return b1 && b2 && b3;
	  } else {
	    return false;
	  }
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  ProjectionNode projNode = (ProjectionNode) super.clone();
	  projNode.targets = targets.clone();
	  
	  return projNode;
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalNode.class);
	}
}
