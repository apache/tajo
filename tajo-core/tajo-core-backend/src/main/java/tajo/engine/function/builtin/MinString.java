package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.StringDatum;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class MinString extends AggFunction<Datum> {

  public MinString() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.STRING)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MinContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MinContext minCtx = (MinContext) ctx;
    if (minCtx.min == null) {
      minCtx.min = params.get(0).asChars();
    } else if (params.get(0).asChars().compareTo(minCtx.min) < 0) {
      minCtx.min = params.get(0).asChars();
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createString(((MinContext)ctx).min);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.STRING};
  }

  @Override
  public StringDatum terminate(FunctionContext ctx) {
    return DatumFactory.createString(((MinContext)ctx).min);
  }

  private class MinContext implements FunctionContext {
    String min;
  }
}
