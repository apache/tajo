/**
 * 
 */
package tajo.engine.planner.logical;

import tajo.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class UnionNode extends BinaryNode {

  public UnionNode() {
    super(ExprType.UNION);
  }

  public UnionNode(LogicalNode outer, LogicalNode inner) {
    this();
    setOuter(outer);
    setInner(inner);
  }

  public String toString() {
    return getOuterNode().toString() + "\n UNION \n" + getInnerNode().toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
