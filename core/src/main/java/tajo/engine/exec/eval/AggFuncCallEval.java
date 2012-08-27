package tajo.engine.exec.eval;

import com.google.gson.annotations.Expose;
import tajo.catalog.FunctionDesc;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.engine.function.AggFunction;
import tajo.engine.function.FunctionContext;
import tajo.engine.json.GsonCreator;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

public class AggFuncCallEval extends FuncEval implements Cloneable {
  @Expose protected AggFunction instance;
  @Expose boolean firstPhase = false;
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

    if (firstPhase) {
      instance.eval(localCtx.funcCtx, params);
    } else {
      instance.merge(localCtx.funcCtx, params);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    if (firstPhase) {
      return instance.getPartialResult(((AggFunctionCtx)ctx).funcCtx);
    } else {
      return instance.terminate(((AggFunctionCtx)ctx).funcCtx);
    }
  }

  @Override
  public DataType [] getValueType() {
    if (firstPhase) {
      return instance.getPartialResultType();
    } else {
      return funcDesc.getReturnType();
    }
  }

  public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, EvalNode.class);
  }

  public Object clone() throws CloneNotSupportedException {
    AggFuncCallEval agg = (AggFuncCallEval) super.clone();
    return agg;
  }

  public void setFirstPhase() {
    this.firstPhase = true;
  }

  protected class AggFunctionCtx extends FuncCallCtx {
    FunctionContext funcCtx;

    AggFunctionCtx(EvalNode [] argEvals, FunctionContext funcCtx) {
      super(argEvals);
      this.funcCtx = funcCtx;
    }
  }
}
