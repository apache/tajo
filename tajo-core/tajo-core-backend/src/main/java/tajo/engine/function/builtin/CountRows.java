package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.LongDatum;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class CountRows extends AggFunction<Datum> {

  public CountRows() {
    super(NoArgs);
  }

  protected CountRows(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new CountRowContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((CountRowContext) ctx).count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    ((CountRowContext) ctx).count += part.get(0).asLong();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createLong(((CountRowContext) ctx).count);
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.LONG};
  }

  @Override
  public LongDatum terminate(FunctionContext ctx) {
    return DatumFactory.createLong(((CountRowContext) ctx).count);
  }

  protected class CountRowContext implements FunctionContext {
    long count;
  }
}
