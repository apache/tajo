/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.json.GsonCreator;

/**
 * @author hyunsik
 *
 */
public class LogicalRootNode extends UnaryNode {

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
}
