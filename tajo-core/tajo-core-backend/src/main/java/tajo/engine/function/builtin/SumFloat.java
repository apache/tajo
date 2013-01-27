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
public class SumFloat extends AggFunction<Datum> {
  public SumFloat() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.FLOAT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new SumContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((SumContext)ctx).sum += params.get(0).asFloat();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createFloat(((SumContext)ctx).sum);
  }

  @Override
  public CatalogProtos.DataType[] getPartialResultType() {
    return new CatalogProtos.DataType[] {DataType.FLOAT};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createFloat(((SumContext)ctx).sum);
  }

  private class SumContext implements FunctionContext {
    private float sum;
  }
}
