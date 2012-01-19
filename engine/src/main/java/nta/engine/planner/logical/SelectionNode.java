package nta.engine.planner.logical;

import nta.catalog.Schema;
import nta.engine.exec.eval.EvalNode;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class SelectionNode extends UnaryNode {

	private EvalNode qual;
	
	public SelectionNode(EvalNode qual) {
		super(ExprType.SELECTION);
		setQual(qual);
	}

	public EvalNode getQual() {
		return this.qual;
	}

	public void setQual(EvalNode qual) {
		this.qual = qual;
	}

  @Override
  public Schema getOutputSchema() {
    return getSubNode().getOutputSchema();
  }
  
  public String toString() {
    return "Selection ("+qual.toString()+")\n"
        + getSubNode().toString();
  }
}
