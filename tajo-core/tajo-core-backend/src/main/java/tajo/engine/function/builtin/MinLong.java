package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.LongDatum;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class MinLong extends AggFunction<Datum> {

  public MinLong() {
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
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createLong(((MinContext)ctx).min);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.LONG};
  }

  @Override
  public LongDatum terminate(FunctionContext ctx) {
    return DatumFactory.createLong(((MinContext)ctx).min);
  }

  private class MinContext implements FunctionContext {
    long min = Long.MAX_VALUE;
  }
}
