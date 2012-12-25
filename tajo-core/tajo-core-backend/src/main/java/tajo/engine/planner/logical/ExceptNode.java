/**
 * 
 */
package tajo.engine.planner.logical;

import tajo.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class ExceptNode extends BinaryNode {

  public ExceptNode() {
    super(ExprType.EXCEPT);
  }

  public ExceptNode(LogicalNode outer, LogicalNode inner) {
    this();
    setOuter(outer);
    setInner(inner);
  }

  public String toString() {
    return getOuterNode().toString() + "\n EXCEPT \n" + getInnerNode().toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
