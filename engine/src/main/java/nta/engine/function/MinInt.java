package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.IntDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MinInt extends GeneralFunction<IntDatum> {
  private Datum minVal;
  private Datum curVal;

  public MinInt() {
    super(new Column[] { new Column("arg1", DataType.INT)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.get(0);
    minVal = params.get(1);
  }

  @Override
  public IntDatum terminate() {
    if (minVal == null) {
      return (IntDatum) curVal;
    }
    return DatumFactory
        .createInt(Math.min(minVal.asInt(), curVal.asInt()));
  }
}
