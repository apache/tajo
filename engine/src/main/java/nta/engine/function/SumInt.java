package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.IntDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class SumInt extends GeneralFunction<IntDatum> {
  private IntDatum curVal;
  private IntDatum sumVal;

  public SumInt() {
    super(new Column[] { new Column("arg1", DataType.INT)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.getInt(0);
    sumVal = params.getInt(1);
  }

  @Override
  public IntDatum terminate() {
    if (sumVal == null) {
      return curVal;
    } else {
      return (IntDatum) curVal.plus(sumVal);
    }
  }
}
