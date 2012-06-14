/**
 * 
 */
package nta.engine.exec.eval;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NotEval extends EvalNode implements Cloneable {
  @Expose private EvalNode subExpr;

  public NotEval(EvalNode subExpr) {
    super(Type.NOT);
    Preconditions.checkArgument(
        subExpr instanceof BinaryEval || subExpr instanceof NotEval);
    this.subExpr = subExpr;
  }

  @Override
  public DataType getValueType() {
    return DataType.BOOLEAN;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public void eval(Schema schema, Tuple tuple, Datum... args) {
    subExpr.eval(schema, tuple);
  }

  @Override
  public Datum terminate() {
    return DatumFactory.createBool(!subExpr.terminate().asBool());
  }

  @Override
  public String toString() {
    return "NOT " + subExpr.toString();
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    if (subExpr instanceof NotEval) {
      ((NotEval)subExpr).subExpr.preOrder(visitor);
    } else {
      subExpr.leftExpr.preOrder(visitor);
      subExpr.rightExpr.preOrder(visitor);
    }
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {    
    if (subExpr instanceof NotEval) {
      ((NotEval)subExpr).subExpr.preOrder(visitor);
    } else {
      subExpr.leftExpr.preOrder(visitor);
      subExpr.rightExpr.preOrder(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    NotEval eval = (NotEval) super.clone();
    eval.subExpr = (EvalNode) this.subExpr.clone();
    return eval;
  }
}
