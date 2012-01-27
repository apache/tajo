package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

public class AggFuncCallEval extends FuncCallEval {
  public AggFuncCallEval(FunctionDesc desc, Function instance, EvalNode[] givenArgs) {
    super(desc, instance, givenArgs);
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple, Datum... args) {
    Datum[] data = null;

    if (givenArgs != null) {
      data = new Datum[givenArgs.length+1];

      for (int i = 0; i < givenArgs.length; i++) {
        data[i] = givenArgs[i].eval(schema, tuple);
      }
    } else {
      data = new Datum[1];
    }
    
    data[data.length-1] = args[0];

    return instance.invoke(data);
  }
}
