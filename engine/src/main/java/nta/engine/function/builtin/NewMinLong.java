package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class NewMinLong extends AggFunction<Datum> {

  public NewMinLong() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.LONG)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MinContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MinContext minCtx = (MinContext)ctx;
    minCtx.min = Math.min(minCtx.min, params.get(0).asLong());
  }

  @Override
  public Tuple getPartialResult(FunctionContext ctx) {
    Tuple part = new VTuple(1);
    part.put(0, DatumFactory.createLong(((MinContext)ctx).min));
    return part;
  }

  @Override
  public LongDatum terminate(FunctionContext ctx) {
    return DatumFactory.createLong(((MinContext)ctx).min);
  }

  private class MinContext implements FunctionContext {
    long min = Long.MAX_VALUE;
  }
}
