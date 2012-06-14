package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DoubleDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class SumDouble extends GeneralFunction<DoubleDatum> {
  private DoubleDatum curVal;
  private DoubleDatum sumVal;

  public SumDouble() {
    super(new Column[] { new Column("arg1", DataType.DOUBLE)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.getDouble(0);
    sumVal = params.getDouble(1);
  }

  @Override
  public DoubleDatum terminate() {
    if (sumVal == null) {
      return curVal;
    } else {
      return (DoubleDatum) sumVal.plus(curVal);
    }
  }
}
