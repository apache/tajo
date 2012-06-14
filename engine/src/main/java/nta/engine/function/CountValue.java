package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DatumType;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class CountValue extends GeneralFunction<LongDatum> {
  private long count;
  
  public CountValue() {
    super(new Column[] { new Column("arg1", DataType.ANY)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(final Tuple params) {
    if (!params.contains(1)) {
      if (params.get(0).type() != DatumType.NULL) {
        count = 0;
      } else {
        count = 1;
      }
    }

    if (params.get(0).type() != DatumType.NULL) {
      count++;
    }
  }

  @Override
  public LongDatum terminate() {
    return DatumFactory.createLong(count);
  }
}
