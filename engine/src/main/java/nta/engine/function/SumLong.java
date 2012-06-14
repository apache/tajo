/**
 * 
 */
package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.LongDatum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class SumLong extends GeneralFunction {
  private LongDatum curVal;
  private LongDatum sumVal;

  public SumLong() {
    super(new Column[] { new Column("arg1", DataType.LONG)});
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    curVal = params.getLong(0);
    sumVal = params.getLong(1);
  }

  @Override
  public LongDatum terminate() {
    if (sumVal == null) {
      return curVal;
    } else {
      return (LongDatum) sumVal.plus(curVal);
    }
  }
}
