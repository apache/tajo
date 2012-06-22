/**
 * 
 */
package nta.engine;

import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;
import org.mortbay.log.Log;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;

/**
 * @author hyunsik
 * 
 */
public class SlowFunc extends AggFunction {
  private Datum param;

  public SlowFunc() {
    super(new Column[] { new Column("name", DataType.STRING) });
  }

  @Override
  public FunctionContext newContext() {
    return null;
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    param = params.get(0);
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return null;
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.STRING};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    try {
      Thread.sleep(1000);
      Log.info("Sleepy... z...z...z");
    } catch (InterruptedException ie) {
    }
    return param;
  }
}
