package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MaxLong extends GeneralFunction<LongDatum> {
  private Long maxVal;
  private Long curVal;

  public MaxLong() {
    super(new Column[] { new Column("arg1", DataType.LONG)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.get(0).asLong();
    maxVal = params.get(1).asLong();
  }

  @Override
  public LongDatum terminate() {
    if (maxVal == null) {
      return DatumFactory.createLong(curVal);
    }
    return DatumFactory
        .createLong(Math.max(maxVal, curVal));
  }
}
