package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.json.GsonCreator;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class SelectionNode extends UnaryNode implements Cloneable {

	@Expose
	private EvalNode qual;
	
	public SelectionNode() {
		super();
	}
	
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
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema()).append("}");
    
    return sb.toString()+"\n"
    + getSubNode().toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionNode) {
      SelectionNode other = (SelectionNode) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SelectionNode selNode = (SelectionNode) super.clone();
    selNode.qual = (EvalNode) this.qual.clone();
    
    return selNode;
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
