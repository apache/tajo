package nta.engine.function;

import nta.catalog.Column;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class CountRows extends GeneralFunction<LongDatum> {
  private long count;
  public CountRows() {
    super(new Column[] {});
  }


  @Override
  public void init() {
  }

  @Override
  public void eval(final Tuple params) {
    count++;
  }

  @Override
  public LongDatum terminate() {
    return DatumFactory.createLong(count);
  }
}
