package org.apache.tajo.plan.expr;

import org.apache.tajo.datum.Datum;

import java.util.Set;

public abstract class ValueSetEval extends EvalNode implements Cloneable {

  public ValueSetEval(EvalType evalType) {
    super(evalType);
  }

  public abstract Set<Datum> getValues();

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public EvalNode getChild(int idx) {
    return null;
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
