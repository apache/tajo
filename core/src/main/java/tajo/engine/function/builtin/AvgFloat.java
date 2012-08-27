package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.FloatDatum;
import tajo.engine.function.AggFunction;
import tajo.engine.function.FunctionContext;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class AvgFloat extends AggFunction<FloatDatum> {

  public AvgFloat() {
    super(new Column[] {
        new Column("val", DataType.DOUBLE)
    });
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += params.get(0).asFloat();
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
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.DOUBLE, DataType.LONG};
  }

  @Override
  public FloatDatum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createFloat((float) (avgCtx.sum / avgCtx.count));
  }

  private class AvgContext implements FunctionContext {
    double sum;
    long count;
  }
}
