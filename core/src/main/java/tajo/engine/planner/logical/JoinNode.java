/**
 * 
 */
package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.JoinType;

/**
 * @author Hyunsik Choi
 * 
 */
public class JoinNode extends BinaryNode implements Cloneable {
  @Expose private JoinType joinType;
  @Expose private EvalNode joinQual;
  @Expose private QueryBlock.Target[] targets;

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

  public boolean hasTargetList() {
    return this.targets != null;
  }

  public QueryBlock.Target[] getTargets() {
    return this.targets;
  }

  public void setTargetList(QueryBlock.Target[] targets) {
    this.targets = targets;
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
