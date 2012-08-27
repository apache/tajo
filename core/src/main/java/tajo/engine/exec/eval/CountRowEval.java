/**
 * 
 */
package tajo.engine.exec.eval;

import tajo.catalog.FunctionDesc;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.function.AggFunction;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

/**
 * @author Hyunsik Choi
 *
 */
@Deprecated
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
