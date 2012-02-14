/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.json.GsonCreator;

/**
 * @author hyunsik
 *
 */
public class LogicalRootNode extends UnaryNode implements Cloneable {

  public LogicalRootNode() {
    super(ExprType.ROOT);
  }
  
  public String toString() {
    return getSubNode().toString();  
  }
  
  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LogicalRootNode) {
      LogicalRootNode other = (LogicalRootNode) obj;
      boolean b1 = super.equals(other);
      boolean b2 = subExpr.equals(other.subExpr);
      
      return b1 && b2;
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
