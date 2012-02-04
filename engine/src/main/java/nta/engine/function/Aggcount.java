package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

public class Aggcount extends Function {

  public Aggcount() {
    super(new Column[] { new Column("arg1", DataType.INT) });
  }

  @Override
  public Datum invoke(Datum... data) {
    if (data.length == 1) {
      return DatumFactory.createInt(1);
    } else {
      return data[0].plus(DatumFactory.createInt(1));
    }
  }

  @Override
  public DataType getResType() {
    return DataType.INT;
  }
}