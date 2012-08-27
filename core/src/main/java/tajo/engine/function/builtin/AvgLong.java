package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.DoubleDatum;
import tajo.engine.function.AggFunction;
import tajo.engine.function.FunctionContext;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class AvgLong extends AggFunction<DoubleDatum> {

  public AvgLong() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.DOUBLE)
    });
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += params.get(0).asLong();
    avgCtx.count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += part.get(0).asLong();
    avgCtx.count += part.get(1).asLong();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum part = new ArrayDatum(2);
    part.put(0, DatumFactory.createLong(avgCtx.sum));
    part.put(1, DatumFactory.createLong(avgCtx.count));

    return part;
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.LONG, DataType.LONG};
  }

  @Override
  public DoubleDatum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createDouble(avgCtx.sum / avgCtx.count);
  }

  private class AvgContext implements FunctionContext {
    long sum;
    long count;
  }
}
