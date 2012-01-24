package nta.engine.planner.logical;

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
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Selection\": {\"qual\": \"").append(qual.toString()).append("\",");
    sb.append("\n  \"out schema\": ").append(getOutputSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInputSchema()).append("}");
    
    return sb.toString()+"\n"
    + getSubNode().toString();
  }
}
