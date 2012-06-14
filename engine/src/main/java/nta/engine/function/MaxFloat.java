package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MaxFloat extends GeneralFunction {
  private Long maxVal;
  private Long curVal;

  public MaxFloat() {
    super(new Column[] { new Column("arg1", DataType.FLOAT)});
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