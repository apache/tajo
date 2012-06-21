package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class NewMaxFloat extends AggFunction<Datum> {
  public NewMaxFloat() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.FLOAT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MaxContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MaxContext maxCtx = (MaxContext) ctx;
    maxCtx.max = Math.max(maxCtx.max, params.get(0).asFloat());
  }

  @Override
  public Tuple getPartialResult(FunctionContext ctx) {
    Tuple part = new VTuple(1);
    part.put(0, DatumFactory.createFloat(((MaxContext)ctx).max));
    return part;
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createFloat(((MaxContext)ctx).max);
  }

  private class MaxContext implements FunctionContext {
    float max;
  }
}
