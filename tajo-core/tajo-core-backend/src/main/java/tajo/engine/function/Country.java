package tajo.engine.function;

import tajo.catalog.Column;
import tajo.catalog.function.GeneralFunction;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.StringDatum;
import tajo.storage.Tuple;
import tajo.util.GeoUtil;

public class Country extends GeneralFunction<StringDatum> {

  public Country() {
    super(new Column[] {new Column("addr", DataType.STRING)});
  }

  @Override
  public Datum eval(Tuple params) {
    return new StringDatum(GeoUtil.getCountryCode(params.get(0).asChars()));
  }
}
