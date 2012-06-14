package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.FloatDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class SumFloat extends GeneralFunction<FloatDatum> {
  private FloatDatum curVal;
  private FloatDatum sumVal;

  public SumFloat() {
    super(new Column[] { new Column("arg1", DataType.FLOAT)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.getFloat(0);
    sumVal = params.getFloat(1);
  }

  @Override
  public FloatDatum terminate() {
    if (sumVal == null) {
      return curVal;
    } else {
      return (FloatDatum) sumVal.plus(curVal);
    }
  }
}
