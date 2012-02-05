/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.json.GsonCreator;


/**
 * @author Hyunsik Choi
 *
 */
public class JoinNode extends BinaryNode {

  /**
   * @param exprType
   */
  public JoinNode(LogicalNode left, LogicalNode right) {
    super(ExprType.JOIN);
    setOuter(left);
    setInner(right);
  }
  
  public String toString() {
    return "Join: \n"
    		+getRightSubNode().toString()+" and "+getLeftSubNode();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
