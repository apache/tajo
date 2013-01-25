package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class MinDouble extends AggFunction<Datum> {

  public MinDouble() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.DOUBLE)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MinContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MinContext minCtx = (MinContext) ctx;
    minCtx.min = Math.min(minCtx.min, params.get(0).asDouble());
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createDouble(((MinContext)ctx).min);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.DOUBLE};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createDouble(((MinContext)ctx).min);
  }

  private class MinContext implements FunctionContext {
    double min = Double.MAX_VALUE;
  }
}
