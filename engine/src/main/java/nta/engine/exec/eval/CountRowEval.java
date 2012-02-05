/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.storage.Tuple;

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
  public Datum eval(Schema schema, Tuple tuple, Datum... args) {
    return instance.invoke(args);
  }
}
