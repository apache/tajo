package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MinLong extends GeneralFunction<LongDatum> {
  private Long minVal;
  private Long curVal;

  public MinLong() {
    super(new Column[] { new Column("arg1", DataType.LONG)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.get(0).asLong();
    minVal = params.get(1).asLong();
  }

  @Override
  public LongDatum terminate() {
    if (minVal == null) {
      return DatumFactory.createLong(curVal);
    }
    return DatumFactory
        .createLong(Math.min(minVal, curVal));
  }
}
