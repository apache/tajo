package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.FloatDatum;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NewMinFloat extends AggFunction<FloatDatum> {

  public NewMinFloat() {
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
