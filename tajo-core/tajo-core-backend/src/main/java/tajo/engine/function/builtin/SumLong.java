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
public class SumLong extends AggFunction<Datum> {

  public SumLong() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.LONG)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new SumContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((SumContext)ctx).sum += params.get(0).asLong();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createLong(((SumContext)ctx).sum);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.LONG};
  }

  @Override
  public LongDatum terminate(FunctionContext ctx) {
    return DatumFactory.createLong(((SumContext)ctx).sum);
  }

  private class SumContext implements FunctionContext {
    long sum;
  }
}
