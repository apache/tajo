package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

/**
 * @author Hyunsik Choi
 */
public final class MaxDouble extends Function {
  public MaxDouble() {
    super(new Column[] { new Column("arg1", DataType.DOUBLE)});
  }

  @Override
  public Datum invoke(final Datum... datums) {
    if (datums.length == 1) {
      return datums[0];
    }
    return DatumFactory
        .createDouble(Math.max(datums[0].asDouble(), datums[1].asDouble()));
  }

  @Override
  public DataType getResType() {
    return DataType.DOUBLE;
  }
}
