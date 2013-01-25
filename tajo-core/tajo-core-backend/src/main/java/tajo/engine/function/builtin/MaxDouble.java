package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.DoubleDatum;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class MaxDouble extends AggFunction<DoubleDatum> {

  public MaxDouble() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.DOUBLE)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MaxContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MaxContext maxCtx = (MaxContext) ctx;
    maxCtx.max = Math.max(maxCtx.max, params.get(0).asDouble());
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createDouble(((MaxContext)ctx).max);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.DOUBLE};
  }

  @Override
  public DoubleDatum terminate(FunctionContext ctx) {
    return DatumFactory.createDouble(((MaxContext)ctx).max);
  }

  private class MaxContext implements FunctionContext {
    double max;
  }
}
