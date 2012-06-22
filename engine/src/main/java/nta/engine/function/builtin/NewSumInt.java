package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NewSumInt extends AggFunction<Datum> {

  public NewSumInt() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.INT)
    });
  }

  @Override
  public SumIntContext newContext() {
    return new SumIntContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    SumIntContext sumCtx = (SumIntContext) ctx;
    sumCtx.sum += params.get(0).asLong();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createInt(((SumIntContext)ctx).sum);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.INT};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt(((SumIntContext)ctx).sum);
  }

  private class SumIntContext implements FunctionContext {
    int sum;
  }
}
