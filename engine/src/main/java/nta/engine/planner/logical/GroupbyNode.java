package nta.engine.planner.logical;

import java.util.Arrays;

import nta.catalog.Column;
import nta.engine.exec.eval.EvalNode;
import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.utils.TUtil;

import com.google.gson.annotations.Expose;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class GroupbyNode extends UnaryNode implements Cloneable {
	@Expose
	private Column [] columns;
	@Expose
	private EvalNode havingCondition = null;
	@Expose
	private Target [] targets;
	
	public GroupbyNode() {
		super();
	}
	
	public GroupbyNode(final Column [] groupingColumns) {
		super(ExprType.GROUP_BY);
		this.columns = groupingColumns;
	}
	
	public GroupbyNode(final Column [] columns, 
	    final EvalNode havingCondition) {
    this(columns);
    this.havingCondition = havingCondition;
  }
	
	public final Column [] getGroupingColumns() {
	  return this.columns;
	}
	
	public final boolean hasHavingCondition() {
	  return this.havingCondition != null;
	}
	
	public final EvalNode getHavingCondition() {
	  return this.havingCondition;
	}
	
	public final void setHavingCondition(final EvalNode evalTree) {
	  this.havingCondition = evalTree;
	}
	
  public boolean hasTargetList() {
    return this.targets != null;
  }

  public Target[] getTargetList() {
    return this.targets;
  }

  public void setTargetList(Target[] targets) {
    this.targets = targets;
  }
  
  public void setSubNode(LogicalNode subNode) {
    super.setSubNode(subNode);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("\"GroupBy\": {\"fields\":[");
    for (int i=0; i < columns.length; i++) {
      sb.append("\"").append(columns[i]).append("\"");
      if(i < columns.length - 1)
        sb.append(",");
    }
    
    if(hasHavingCondition()) {
      sb.append("], \"having qual\": \""+havingCondition+"\"");
    }
    if(hasTargetList()) {
      sb.append(", \"target\": [");
      for (int i = 0; i < targets.length; i++) {
        sb.append("\"").append(targets[i]).append("\"");
        if( i < targets.length - 1) {
          sb.append(",");
        }
      }
      sb.append("],");
    }
    sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInputSchema());
    sb.append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GroupbyNode) {
      GroupbyNode other = (GroupbyNode) obj;
      return super.equals(other) 
          && Arrays.equals(columns, other.columns)
          && TUtil.checkEquals(havingCondition, other.havingCondition)
          && TUtil.checkEquals(targets, other.targets)
          && getSubNode().equals(other.getSubNode());
    } else {
      return false;  
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    GroupbyNode grp = (GroupbyNode) super.clone();
    if (columns != null) {
      grp.columns = new Column[columns.length];
      for (int i = 0; i < columns.length; i++) {
        grp.columns[i] = (Column) columns[i].clone();
      }
    }
    grp.havingCondition = (EvalNode) (havingCondition != null 
        ? havingCondition.clone() : null);    
    if (targets != null) {
      grp.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        grp.targets[i] = (Target) targets[i].clone();
      }
    }

    return grp;
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
