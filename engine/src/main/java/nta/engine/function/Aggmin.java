package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;

public class Aggmin extends Function {

  public Aggmin() {
    super(new Column[] { new Column("arg1", DataType.INT) });
  }

  @Override
  public Datum invoke(Datum... data) {
    if (data.length == 1) {
      return data[0];
    } else if (data[0].greaterThan(data[1]).asBool()) {
      return data[1];
    } else {
      return data[0];
    }
  }

  @Override
  public DataType getResType() {
    return DataType.INT;
  }
}