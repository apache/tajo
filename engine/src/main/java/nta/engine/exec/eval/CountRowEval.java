/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.AggFunction;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
public class CountRowEval extends AggFuncCallEval {

  /**
   * @param desc
   * @param instance
   * @param givenArgs
   */
  public CountRowEval(FunctionDesc desc, AggFunction instance,
      EvalNode[] givenArgs) {
    super(desc, instance, givenArgs);
  }
  
  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    Tuple v = new VTuple(1);
    instance.eval(((AggFunctionCtx)ctx).funcCtx, v);
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    return instance.terminate(((AggFunctionCtx)ctx).funcCtx);
  }
}
