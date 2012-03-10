package nta.engine.planner.logical;

import java.util.Arrays;

import com.google.gson.annotations.Expose;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
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
	
	public void setSubNode(LogicalNode subNode) {
	  super.setSubNode(subNode);
	  Schema projected = new Schema();
	  for(Target t : targets) {
      DataType type = t.getEvalTree().getValueType();
      String name = t.getEvalTree().getName();
      projected.addColumn(name,type);
    }
	  setOutputSchema(projected);
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
