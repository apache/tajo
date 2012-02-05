package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

/**
 * @author Hyunsik Choi
 */
public final class MinLong extends Function {
  public MinLong() {
    super(new Column[] { new Column("arg1", DataType.LONG)});
  }

  @Override
  public Datum invoke(final Datum... datums) {
    if (datums.length == 1) {
      return datums[0];
    }
    return DatumFactory
        .createLong(Math.min(datums[0].asLong(), datums[1].asLong()));
  }

  @Override
  public DataType getResType() {
    return DataType.LONG;
  }
}
