package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.function.AggFunction;
import tajo.engine.function.FunctionContext;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NewMaxInt extends AggFunction<Datum> {

  public NewMaxInt() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.LONG)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MaxContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MaxContext maxCtx = (MaxContext) ctx;
    maxCtx.max = Math.max(maxCtx.max, params.get(0).asInt());
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createInt(((MaxContext)ctx).max);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.INT};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt(((MaxContext)ctx).max);
  }

  private class MaxContext implements FunctionContext {
    int max;
  }
}
