package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.FloatDatum;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class MinFloat extends AggFunction<FloatDatum> {

  public MinFloat() {
    super(new Column[] {
        new Column("val", DataType.FLOAT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MinContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MinContext minCtx = (MinContext) ctx;
    minCtx.min = Math.min(minCtx.min, params.get(0).asFloat());
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createFloat(((MinContext) ctx).min);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.FLOAT};
  }

  @Override
  public FloatDatum terminate(FunctionContext ctx) {
    return DatumFactory.createFloat(((MinContext) ctx).min);
  }

  private class MinContext implements FunctionContext {
    float min = Float.MAX_VALUE;
  }
}
