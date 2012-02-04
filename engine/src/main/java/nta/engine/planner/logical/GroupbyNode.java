package nta.engine.planner.logical;

import nta.catalog.Column;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryBlock.Target;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class GroupbyNode extends UnaryNode {
	private final Column [] columns;
	private EvalNode havingCondition = null;
	private Target [] targets;
	
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
  
  public String toString() {
    StringBuilder sb = new StringBuilder("\"GroupBy\": {\"fields\":[");
    for (int i=0; i < columns.length; i++) {
      sb.append("\"").append(columns[i]).append("\"");
      if(i < columns.length - 1)
        sb.append(",");
    }
    sb.append("],");
    if(hasHavingCondition()) {
      sb.append("\"having qual\": \""+havingCondition+"\"");
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
}
