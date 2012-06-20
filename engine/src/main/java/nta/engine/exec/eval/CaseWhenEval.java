package nta.engine.exec.eval;

import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import nta.datum.BoolDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class CaseWhenEval extends EvalNode {
  @Expose private List<WhenClause> whens = Lists.newArrayList();
  @Expose private EvalNode elseResult;

  public CaseWhenEval() {
    super(Type.CASE);
  }

  public void addWhen(EvalNode condition, EvalNode result) {
    whens.add(new WhenClause(condition, result));
  }

  public void setElseResult(EvalNode elseResult) {
    this.elseResult = elseResult;
  }

  @Override
  public EvalContext newContext() {
    return new CaseContext(whens, elseResult != null ? elseResult.newContext() : null);
  }

  @Override
  public CatalogProtos.DataType getValueType() {
    return whens.get(0).getResultExpr().getValueType();
  }

  @Override
  public String getName() {
    return "?";
  }

  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    CaseContext caseCtx = (CaseContext) ctx;
    for (int i = 0; i < whens.size(); i++) {
      whens.get(i).eval(caseCtx.contexts[i], schema, tuple);
    }

    if (elseResult != null) { // without else clause
      elseResult.eval(caseCtx.elseCtx, schema, tuple);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    CaseContext caseCtx = (CaseContext) ctx;
    for (int i = 0; i < whens.size(); i++) {
      if (whens.get(i).terminate(caseCtx.contexts[i]).asBool()) {
        return whens.get(i).getThenResult(caseCtx.contexts[i]);
      }
    }
    if (elseResult != null) { // without else clause
      return elseResult.terminate(caseCtx.elseCtx);
    } else {
      return DatumFactory.createNullDatum();
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("CASE\n");
    for (WhenClause when : whens) {
     sb.append(when).append("\n");
    }

    sb.append("ELSE ").append(elseResult).append(" END\n");

    return sb.toString();
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    for (WhenClause when : whens) {
      when.preOrder(visitor);
    }
    if (elseResult != null) { // without else clause
      elseResult.preOrder(visitor);
    }
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    for (WhenClause when : whens) {
      when.postOrder(visitor);
    }
    if (elseResult != null) { // without else clause
      elseResult.postOrder(visitor);
    }
    visitor.visit(this);
  }

  public static class WhenClause extends EvalNode {
    @Expose private EvalNode condition;
    @Expose private EvalNode result;

    public WhenClause(EvalNode condition, EvalNode result) {
      super(Type.WHEN);
      this.condition = condition;
      this.result = result;
    }

    @Override
    public EvalContext newContext() {
      return new WhenContext(condition.newContext(), result.newContext());
    }

    @Override
    public CatalogProtos.DataType getValueType() {
      return CatalogProtos.DataType.BOOLEAN;
    }

    @Override
    public String getName() {
      return "when?";
    }

    public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
      condition.eval(((WhenContext) ctx).condCtx, schema, tuple);
    }

    @Override
    public Datum terminate(EvalContext ctx) {
      return (BoolDatum) condition.terminate(((WhenContext) ctx).condCtx);
    }

    public EvalNode getConditionExpr() {
      return this.condition;
    }

    public EvalNode getResultExpr() {
      return this.result;
    }

    public String toString() {
      return "WHEN " + condition + " THEN " + result;
    }

    private class WhenContext implements EvalContext {
      EvalContext condCtx;
      EvalContext resultCtx;

      public WhenContext(EvalContext condCtx, EvalContext resultCtx) {
        this.condCtx = condCtx;
        this.resultCtx = resultCtx;
      }
    }

    public Datum getThenResult(EvalContext ctx) {
      return result.terminate(((WhenContext) ctx).resultCtx);
    }

    @Override
    public void preOrder(EvalNodeVisitor visitor) {
      visitor.visit(this);
      condition.preOrder(visitor);
      result.preOrder(visitor);
    }

    @Override
    public void postOrder(EvalNodeVisitor visitor) {
      condition.postOrder(visitor);
      result.postOrder(visitor);
      visitor.visit(this);
    }
  }

  private class CaseContext implements EvalContext {
    EvalContext [] contexts;
    EvalContext elseCtx;

    CaseContext(List<WhenClause> whens, EvalContext elseCtx) {
      contexts = new EvalContext[whens.size()];
      for (int i = 0; i < whens.size(); i++) {
        contexts[i] = whens.get(i).newContext();
      }
      this.elseCtx = elseCtx;
    }
  }
}
