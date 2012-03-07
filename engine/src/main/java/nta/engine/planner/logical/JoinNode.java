/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.SchemaUtil;
import nta.engine.exec.eval.EvalNode;
import nta.engine.json.GsonCreator;
import nta.engine.planner.JoinType;

/**
 * @author Hyunsik Choi
 * 
 */
public class JoinNode extends BinaryNode implements Cloneable {
  private JoinType joinType;
  private EvalNode joinQual;

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

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  public void setOuter(LogicalNode outer) {
    super.setOuter(outer);
    if (inner == null) {
      this.setInputSchema(outer.getOutputSchema());
      this.setOutputSchema(outer.getOutputSchema());
    } else {
      if (joinType == JoinType.NATURAL) {
        refreshNaturalJoin();
      } else {
        Schema merged = SchemaUtil.merge(outer.getOutputSchema(),
            inner.getOutputSchema());
        this.setInputSchema(merged);
        this.setOutputSchema(merged);
      }
    }
  }

  public void setInner(LogicalNode inner) {
    super.setInner(inner);
    if (outer == null) {
      this.setInputSchema(inner.getOutputSchema());
      this.setOutputSchema(inner.getOutputSchema());
    } else {
      if (joinType == JoinType.NATURAL) {
        refreshNaturalJoin();
      } else {
        Schema merged = SchemaUtil.merge(outer.getOutputSchema(),
            inner.getOutputSchema());
        this.setInputSchema(merged);
        this.setOutputSchema(merged);
      }
    }
  }

  private void refreshNaturalJoin() {
    Schema joinSchema = new Schema();
    Schema commons = SchemaUtil.getCommons(outer.getOutputSchema(),
        inner.getOutputSchema());
    joinSchema.addColumns(commons);
    for (Column c : outer.getOutputSchema().getColumns()) {
      for (Column common : commons.getColumns()) {
        if (!common.getColumnName().equals(c.getColumnName())) {
          joinSchema.addColumn(c);
        }
      }
    }

    for (Column c : inner.getOutputSchema().getColumns()) {
      for (Column common : commons.getColumns()) {
        if (!common.getColumnName().equals(c.getColumnName())) {
          joinSchema.addColumn(c);
        }
      }
    }
    this.setInputSchema(joinSchema);
    this.setOutputSchema(joinSchema);
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

    return join;
  }

  public String toString() {
    return "\"Join\": "
        + "\n\"out schema: " + getOutputSchema() 
        + "\n\"in schema: " + getInputSchema()
    		+ "\n" + getOuterNode().toString() + " and " + getInnerNode();
  }

  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
