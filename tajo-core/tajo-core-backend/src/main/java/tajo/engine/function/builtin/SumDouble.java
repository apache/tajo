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
public class SumDouble extends AggFunction<Datum> {

  public SumDouble() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.DOUBLE)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new SumContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((SumContext)ctx).sum += params.get(0).asDouble();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createDouble(((SumContext)ctx).sum);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.DOUBLE};
  }

  @Override
  public DoubleDatum terminate(FunctionContext ctx) {
    return DatumFactory.createDouble(((SumContext)ctx).sum);
  }

  private class SumContext implements FunctionContext {
    double sum;
  }
}
