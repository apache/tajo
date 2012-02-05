package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

/**
 * @author Hyunsik Choi
 */
public final class MinFloat extends Function {
  public MinFloat() {
    super(new Column[] { new Column("arg1", DataType.FLOAT)});
  }

  @Override
  public Datum invoke(final Datum... datums) {
    if (datums.length == 1) {
      return datums[0];
    }
    return DatumFactory
        .createFloat(Math.min(datums[0].asFloat(), datums[1].asFloat()));
  }

  @Override
  public DataType getResType() {
    return DataType.FLOAT;
  }
}
