package tajo.engine.function;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.BoolDatum;
import tajo.datum.Datum;
import tajo.storage.Tuple;
import tajo.util.GeoUtil;

public class InCountry extends GeneralFunction<BoolDatum> {

  public InCountry() {
    super(new Column[] {new Column("addr", DataType.STRING),
        new Column("code", DataType.STRING)});
  }

  @Override
  public Datum eval(Tuple params) {
    String addr = params.get(0).asChars();
    String otherCode = params.get(1).asChars();
    String thisCode = GeoUtil.getCountryCode(addr);

    return new BoolDatum(thisCode.equals(otherCode));
  }
}
