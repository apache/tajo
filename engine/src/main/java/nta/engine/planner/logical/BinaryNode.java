/**
 * 
 */
package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class BinaryNode extends LogicalNode implements Cloneable {
	@Expose
	LogicalNode outer = null;
	@Expose
	LogicalNode inner = null;
	
	public BinaryNode() {
		super();
	}
	
	/**
	 * @param opType
	 */
	public BinaryNode(ExprType opType) {
		super(opType);
	}
	
	public LogicalNode getRightSubNode() {
		return this.outer;
	}
	
	public void setOuter(LogicalNode op) {
		this.outer = op;
	}

	public LogicalNode getLeftSubNode() {
		return this.inner;
	}

	public void setInner(LogicalNode op) {
		this.inner = op;
	}
		 
/*  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BinaryNode) {
      BinaryNode other = (BinaryNode) obj;
      return super.equals(other);
    } else {
      return false;
    }
  } */ 
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  BinaryNode binNode = (BinaryNode) super.clone();
	  binNode.outer = (LogicalNode) outer.clone();
	  binNode.inner = (LogicalNode) inner.clone();
	  
	  return binNode;
	}
}
