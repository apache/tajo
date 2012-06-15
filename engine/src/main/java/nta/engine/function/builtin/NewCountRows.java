package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.engine.function.AggFunction;
import nta.engine.function.FunctionContext;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class NewCountRows extends AggFunction<Datum> {

  public NewCountRows() {
    super(NoArgs);
  }

  protected NewCountRows(Column [] columns) {
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
  public Tuple getPartialResult(FunctionContext ctx) {
    Tuple tuple = new VTuple(1);
    tuple.put(0, DatumFactory.createLong(((CountRowContext) ctx).count));
    return tuple;
  }

  @Override
  public LongDatum terminate(FunctionContext ctx) {
    return DatumFactory.createLong(((CountRowContext) ctx).count);
  }

  protected class CountRowContext implements FunctionContext {
    long count;
  }
}
