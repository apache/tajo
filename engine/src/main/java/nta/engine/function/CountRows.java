package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

/**
 * @author Hyunsik Choi
 */
public final class CountRows extends Function {
  private static final Datum one = DatumFactory.createLong(1);
  
  public CountRows() {
    super(new Column[] {});
  }

  @Override
  public Datum invoke(final Datum... datums) {
    if(datums.length == 0)
      return one;
    else
      return datums[0].plus(one);
  }

  @Override
  public DataType getResType() {
    return DataType.LONG;
  }
}
