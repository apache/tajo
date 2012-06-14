package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.DoubleDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MaxDouble extends GeneralFunction<DoubleDatum> {
  private Double maxVal;
  private Double curVal;

  public MaxDouble() {
    super(new Column[] { new Column("arg1", DataType.DOUBLE)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(final Tuple params) {
    curVal = params.get(0).asDouble();
    maxVal = params.get(1).asDouble();
  }

  @Override
  public DoubleDatum terminate() {
    if (maxVal == null) {
      return DatumFactory.createDouble(curVal);
    }
    return DatumFactory
        .createDouble(Math.max(maxVal, curVal));
  }
}
