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
