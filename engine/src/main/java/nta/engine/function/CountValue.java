package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DatumType;

/**
 * @author Hyunsik Choi
 */
public final class CountValue extends Function {
  private Datum one = DatumFactory.createLong(1);
  
  public CountValue() {
    super(new Column[] { new Column("arg1", DataType.ANY)});
  }

  @Override
  public Datum invoke(final Datum... datums) {
    if (datums.length == 1) {
      if (datums[0].type() != DatumType.NULL) {
        return DatumFactory.createLong(1);
      } else {
        return DatumFactory.createLong(0);
      }
    }

    if (datums[0].type() != DatumType.NULL) {
      return datums[1].plus(one);
    } else {
      return datums[1];
    }
  }

  @Override
  public DataType getResType() {
    return DataType.LONG;
  }
}
