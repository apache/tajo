/**
 * 
 */
package nta.engine.planner.logical;

import nta.engine.exec.eval.EvalNode;
import nta.engine.json.GsonCreator;
import nta.engine.planner.JoinType;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 * 
 */
public class JoinNode extends BinaryNode implements Cloneable {
  @Expose private JoinType joinType;
  @Expose private EvalNode joinQual;

  public JoinNode(JoinType joinType, LogicalNode left) {
    super(ExprType.JOIN);
    this.joinType = joinType;
    setOuter(left);
  }

  public JoinNode(JoinType joinType, LogicalNode left, LogicalNode right) {
    super(ExprType.JOIN);
    this.joinType = joinType;
    setOuter(left);
    setInner(right);
  }

  public JoinType getJoinType() {
    return this.joinType;
  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      return super.equals(other) && outer.equals(other.outer)
          && inner.equals(other.inner);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    join.joinType = this.joinType;
    join.joinQual = this.joinQual == null ? null : (EvalNode) this.joinQual.clone();
    return join;
  }

  public String toString() {
    return "\"Join\": \"joinType\": \"" + joinType +"\""
        + (joinQual != null ? ", \"qual\": " + joinQual : "")
        + "\n\"out schema: " + getOutputSchema() 
        + "\n\"in schema: " + getInputSchema()
    		+ "\n" + getOuterNode().toString() + " and " + getInnerNode();
  }

  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
