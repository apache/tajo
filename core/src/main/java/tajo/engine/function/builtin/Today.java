package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.LongDatum;
import tajo.engine.function.GeneralFunction;
import tajo.storage.Tuple;

/**
 * @author jihoon
 */
public class Today extends GeneralFunction<LongDatum> {

  public Today() {
    super(new Column[] {});
  }

  @Override
  public Datum eval(Tuple params) {
    return DatumFactory.createLong(System.currentTimeMillis());
  }
}
