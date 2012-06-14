package nta.engine.exec.eval;

import com.google.gson.annotations.Expose;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class AggFuncCallEval extends FuncEval {
  @Expose protected Function instance;
  private Schema schema;
  private Tuple params;
  private Tuple tuple;
  private Datum [] args;

  public AggFuncCallEval(FunctionDesc desc, Function instance, EvalNode[] givenArgs) {
    super(Type.AGG_FUNCTION, desc, givenArgs);
    this.instance = instance;
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Schema schema, Tuple tuple, Datum... args) {
    this.schema = schema;
    this.tuple = tuple;
    this.args = args;
  }

  @Override
  public Datum terminate() {
    if (params == null) {
      this.params = new VTuple(givenArgs.length + 1);
    }

    if (givenArgs != null) {
      params.clear();

      for (int i = 0; i < givenArgs.length; i++) {
        givenArgs[i].eval(schema, tuple);
        params.put(i, givenArgs[i].terminate());
      }
    }

    // TODO - should consider multiple variables
    if(args.length > 0)
      params.put(params.size()-1, args[0]);

    instance.eval(params);
    return instance.terminate();
  }

  public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, EvalNode.class);
  }
}
