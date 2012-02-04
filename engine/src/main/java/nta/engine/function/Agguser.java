
package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

public class Agguser extends Function {

  public Agguser() {
    super(new Column[] { new Column("arg1", DataType.INT) });
  }

  @Override
  public Datum invoke(Datum... data) {
    return DatumFactory.createInt(0);
  }

  @Override
  public DataType getResType() {
    return DataType.INT;
  }
}