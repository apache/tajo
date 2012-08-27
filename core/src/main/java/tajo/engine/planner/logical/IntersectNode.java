/**
 * 
 */
package tajo.engine.planner.logical;

import tajo.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class IntersectNode extends BinaryNode {

  public IntersectNode() {
    super(ExprType.INTERSECT);
  }

  public IntersectNode(LogicalNode outer, LogicalNode inner) {
    this();
    setOuter(outer);
    setInner(inner);
  }

  public String toString() {
    return getOuterNode().toString() + "\n INTERSECT \n" + getInnerNode().toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
