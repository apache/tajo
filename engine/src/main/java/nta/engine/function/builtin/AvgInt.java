package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.*;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class AvgInt extends AggFunction<FloatDatum> {

  public AvgInt() {
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
    avgCtx.sum += params.get(0).asInt();
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
  public FloatDatum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createFloat(avgCtx.sum / avgCtx.count);
  }

  private class AvgContext implements FunctionContext {
    long sum;
    long count;
  }
}
