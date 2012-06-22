package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DoubleDatum;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NewSumDouble extends AggFunction<Datum> {

  public NewSumDouble() {
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
