/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.json.GsonCreator;

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
    return "\"Union\": " + "\n\"out schema: " + getOutputSchema()
        + "\n\"in schema: " + getInputSchema() + "\n"
        + getOuterNode().toString() + " and " + getInnerNode().toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
