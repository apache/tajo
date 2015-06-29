package org.apache.tajo.plan.expr;

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.logical.TableSubQueryNode;
import org.apache.tajo.storage.Tuple;

import java.util.Set;

public class SubqueryEval extends ValueSetEval {

  private TableSubQueryNode subQueryNode;

  public SubqueryEval(TableSubQueryNode subQueryNode) {
    super(EvalType.SUBQUERY);
    this.subQueryNode = subQueryNode;
  }

  @Override
  public DataType getValueType() {
    return subQueryNode.getOutSchema().getColumn(0).getDataType();
  }

  @Override
  public String getName() {
    return "SUBQUERY";
  }

  @Override
  public EvalNode bind(@Nullable EvalContext evalContext, Schema schema) {
    throw new UnsupportedException("Cannot call bind()");
  }

  @Override
  public Datum eval(Tuple tuple) {
    throw new UnsupportedException("Cannot call eval()");
  }

  public TableSubQueryNode getSubQueryNode() {
    return subQueryNode;
  }

  @Override
  public int hashCode() {
    return subQueryNode.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SubqueryEval) {
      SubqueryEval other = (SubqueryEval) o;
      return this.subQueryNode.equals(other.subQueryNode);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SubqueryEval clone = (SubqueryEval) super.clone();
    clone.subQueryNode = (TableSubQueryNode) this.subQueryNode.clone();
    return subQueryNode;
  }

  @Override
  public String toString() {
    return subQueryNode.toString();
  }

  @Override
  public Set<Datum> getValues() {
    throw new UnsupportedException("Cannot call getValues()");
  }
}
