package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.DoubleDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MinDouble extends GeneralFunction {
  private Double minVal;
  private Double curVal;

  public MinDouble() {
    super(new Column[] { new Column("arg1", DataType.DOUBLE)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(final Tuple params) {
    curVal = params.get(0).asDouble();
    minVal = params.get(1).asDouble();
  }

  @Override
  public DoubleDatum terminate() {
    if (minVal == null) {
      return DatumFactory.createDouble(curVal);
    }
    return DatumFactory
        .createDouble(Math.min(minVal, curVal));
  }
}
