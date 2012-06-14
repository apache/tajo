package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;
import nta.datum.FloatDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class MinFloat extends GeneralFunction<FloatDatum> {
  private Long minVal;
  private Long curVal;

  public MinFloat() {
    super(new Column[] { new Column("arg1", DataType.FLOAT)});
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
  public FloatDatum terminate() {
    if (minVal == null) {
      return DatumFactory.createFloat(curVal);
    }
    return DatumFactory
        .createFloat(Math.min(minVal, curVal));
  }
}
