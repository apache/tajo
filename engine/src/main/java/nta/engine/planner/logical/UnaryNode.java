/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;


/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryNode extends LogicalNode implements Cloneable {
	@Expose
	LogicalNode subExpr;
	
	public UnaryNode() {
		super();
	}
	
	/**
	 * @param type
	 */
	public UnaryNode(ExprType type) {
		super(type);
	}
	
	public void setSubNode(LogicalNode subNode) {
		this.subExpr = subNode;
	}
	
	public LogicalNode getSubNode() {
		return this.subExpr;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  UnaryNode unary = (UnaryNode) super.clone();
	  unary.subExpr = (LogicalNode) (subExpr == null ? null : subExpr.clone());
	  
	  return unary;
	}
	
	public void accept(LogicalNodeVisitor visitor) {
	  subExpr.accept(visitor);	  
	  visitor.visit(this);
	}
}
