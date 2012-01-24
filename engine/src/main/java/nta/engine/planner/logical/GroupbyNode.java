package nta.engine.planner.logical;

import nta.engine.exec.eval.EvalNode;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class GroupbyNode extends UnaryNode {
	private final EvalNode [] columns;
	private EvalNode havingCondition = null;
	
	public GroupbyNode(final EvalNode [] groupingColumns) {
		super(ExprType.GROUP_BY);
		this.columns = groupingColumns;
	}
	
	public GroupbyNode(final EvalNode [] columns, 
	    final EvalNode havingCondition) {
    this(columns);
    this.havingCondition = havingCondition;
  }
	
	public final EvalNode [] getGroupingColumns() {
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
    sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInputSchema());
    sb.append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
}
