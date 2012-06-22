package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.ArrayDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

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
    ArrayDatum array = (ArrayDatum) part.get(0);
    avgCtx.sum += array.get(0).asDouble();
    avgCtx.count += array.get(1).asLong();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum part = new ArrayDatum(2);
    part.put(0, DatumFactory.createDouble(avgCtx.sum));
    part.put(1, DatumFactory.createLong(avgCtx.count));

    return part;
  }

  @Override
  public CatalogProtos.DataType[] getPartialResultType() {
    return new DataType[] {DataType.DOUBLE,DataType.LONG};
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
