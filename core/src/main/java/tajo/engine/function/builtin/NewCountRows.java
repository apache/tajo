package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.LongDatum;
import tajo.engine.function.AggFunction;
import tajo.engine.function.FunctionContext;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class NewCountRows extends AggFunction<Datum> {

  public NewCountRows() {
    super(NoArgs);
  }

  protected NewCountRows(Column[] columns) {
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
