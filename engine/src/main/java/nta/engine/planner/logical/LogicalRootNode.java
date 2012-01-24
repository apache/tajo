/**
 * 
 */
package nta.engine.planner.logical;

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
}
