package nta.engine.exec.eval;

import com.google.gson.annotations.Expose;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class AggFuncCallEval extends FuncEval implements Cloneable {
  @Expose protected AggFunction instance;
  private Tuple params;

  public AggFuncCallEval(FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs) {
    super(Type.AGG_FUNCTION, desc, givenArgs);
    this.instance = instance;
  }

  @Override
  public EvalContext newContext() {
    AggFunctionCtx newCtx = new AggFunctionCtx(argEvals, instance.newContext());

    return newCtx;
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    AggFunctionCtx localCtx = (AggFunctionCtx) ctx;
    if (params == null) {
      this.params = new VTuple(argEvals.length);
    }

    if (argEvals != null) {
      params.clear();

      for (int i = 0; i < argEvals.length; i++) {
        argEvals[i].eval(localCtx.argCtxs[i], schema, tuple);
        params.put(i, argEvals[i].terminate(localCtx.argCtxs[i]));
      }
    }

    instance.eval(localCtx.funcCtx, params);
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return instance.terminate(((AggFunctionCtx)ctx).funcCtx);
  }

  public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, EvalNode.class);
  }

  public Object clone() throws CloneNotSupportedException {
    AggFuncCallEval agg = (AggFuncCallEval) super.clone();
    return agg;
  }

  protected class AggFunctionCtx extends FuncCallCtx {
    FunctionContext funcCtx;

    AggFunctionCtx(EvalNode [] argEvals, FunctionContext funcCtx) {
      super(argEvals);
      this.funcCtx = funcCtx;
    }
  }
}
