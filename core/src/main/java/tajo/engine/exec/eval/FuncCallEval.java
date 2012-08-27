package tajo.engine.exec.eval;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.FunctionDesc;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.function.GeneralFunction;
import tajo.engine.json.GsonCreator;
import tajo.engine.utils.TUtil;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class FuncCallEval extends FuncEval {
	@Expose protected GeneralFunction instance;
  private Tuple tuple;
  private Tuple params = null;
  private Schema schema;

	public FuncCallEval(FunctionDesc desc, GeneralFunction instance, EvalNode [] givenArgs) {
		super(Type.FUNCTION, desc, givenArgs);
		this.instance = instance;
  }

  /* (non-Javadoc)
    * @see nta.query.executor.eval.Expr#evalVal(Tuple)
    */
	@Override
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    this.schema = schema;
    this.tuple = tuple;
	}

  @Override
  public Datum terminate(EvalContext ctx) {
    FuncCallCtx localCtx = (FuncCallCtx) ctx;
    if (this.params == null) {
      params = new VTuple(argEvals.length);
    }

    if(argEvals != null) {
      params.clear();
      for(int i=0;i < argEvals.length; i++) {
        argEvals[i].eval(localCtx.argCtxs[i], schema, tuple);
        params.put(i, argEvals[i].terminate(localCtx.argCtxs[i]));
      }
    }
    return instance.eval(params);
  }

  @Override
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FuncCallEval) {
      FuncCallEval other = (FuncCallEval) obj;
      return super.equals(other) &&
          TUtil.checkEquals(instance, other.instance);
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(funcDesc, instance);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncCallEval eval = (FuncCallEval) super.clone();
    eval.instance = (GeneralFunction) instance.clone();
    return eval;
  }
}