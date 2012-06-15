package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class AvgDouble extends AggFunction {
  public AvgDouble() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.DOUBLE)
    });
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  public void init() {
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += params.get(0).asDouble();
    avgCtx.count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += part.get(0).asDouble();
    avgCtx.count += part.get(1).asLong();
  }

  @Override
  public Tuple getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    Tuple part = new VTuple(2);
    part.put(0, DatumFactory.createDouble(avgCtx.sum));
    part.put(1, DatumFactory.createLong(avgCtx.count));

    return part;
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createDouble(avgCtx.sum / avgCtx.count);
  }

  private class AvgContext implements FunctionContext {
    double sum;
    long count;
  }
}
