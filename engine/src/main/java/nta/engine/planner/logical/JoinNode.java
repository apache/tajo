/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.json.GsonCreator;


/**
 * @author Hyunsik Choi
 *
 */
public class JoinNode extends BinaryNode implements Cloneable {

  /**
   * @param exprType
   */
  public JoinNode(LogicalNode left, LogicalNode right) {
    super(ExprType.JOIN);
    setOuter(left);
    setInner(right);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      return super.equals(other)
          && outer.equals(other.outer)
          && inner.equals(other.inner);
    } else {
      return false;
    }      
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    
    return join;
  }
  
  public String toString() {
    return "Join: \n"
        +getRightSubNode().toString()+" and "+getLeftSubNode();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
