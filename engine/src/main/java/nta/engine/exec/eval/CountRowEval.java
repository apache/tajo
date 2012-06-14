/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class CountRowEval extends FuncCallEval {

  /**
   * @param desc
   * @param instance
   * @param givenArgs
   */
  public CountRowEval(FunctionDesc desc, Function instance,
      EvalNode[] givenArgs) {
    super(desc, instance, givenArgs);
  }
  
  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple, Datum... args) {
    Tuple v = new VTuple(1);
    if (args.length == 1)
      v.put(0, args[0]);
    instance.eval(v);
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return instance.terminate();
  }
}
