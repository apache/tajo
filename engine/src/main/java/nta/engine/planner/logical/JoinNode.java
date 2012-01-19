/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Schema;

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

  /* (non-Javadoc)
   * @see nta.engina.planner.logical.LogicalNode#getSchema()
   */
  @Override
  public Schema getOutputSchema() {
    return inner.getOutputSchema();
  }
  
  public String toString() {
    return "Join: \n"
    		+getOuter().toString()+" and "+getInner();
  }
}
