package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.engine.json.GsonCreator;
import nta.engine.utils.TUtil;
import nta.storage.Tuple;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class FuncCallEval extends FuncEval {
	@Expose protected Function instance;
  private Tuple tuple;
  private Tuple params = null;
  private Schema schema;

	public FuncCallEval(FunctionDesc desc, Function instance, EvalNode [] givenArgs) {
		super(Type.FUNCTION, desc, givenArgs);
		this.instance = instance;
  }

  @Override
  public void init() {
  }

  /* (non-Javadoc)
    * @see nta.query.executor.eval.Expr#evalVal(nta.storage.Tuple)
    */
	@Override
	public void eval(Schema schema, Tuple tuple, Datum...args) {
    this.schema = schema;
    this.tuple = tuple;
	}

  @Override
  public Datum terminate() {
    if (this.params == null) {
      params = new VTuple(givenArgs.length);
    }

    if(givenArgs != null) {
      params.clear();
      for(int i=0;i < givenArgs.length; i++) {
        givenArgs[i].eval(schema, tuple);
        params.put(i, givenArgs[i].terminate());
      }
    }
    instance.eval(params);
    return instance.terminate();
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
	  return Objects.hashCode(desc, instance);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncCallEval eval = (FuncCallEval) super.clone();
    eval.instance = (Function) instance.clone();
    return eval;
  }
}