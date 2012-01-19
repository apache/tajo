package nta.engine.planner.logical;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.exec.eval.EvalNode;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class GroupbyNode extends UnaryNode {
	private final Column [] columns;
	private EvalNode havingCondition = null;
	
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

  @Override
  public Schema getOutputSchema() {
    // TODO - to be completed
    return getSubNode().getOutputSchema();
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("Group By: fields");
    for (int i=0; i < columns.length; i++) {
      sb.append(columns[i]);
      if(i < columns.length - 1)
        sb.append(", ");
    }
    
    if(hasHavingCondition()) {
      sb.append(" with Having "+havingCondition);
    }
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
}
