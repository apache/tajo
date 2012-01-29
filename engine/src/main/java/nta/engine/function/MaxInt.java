/**
 * 
 */
package nta.engine.function;

import nta.catalog.ColumnBase;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;

/**
 * @author hyunsik
 *
 */
public class MaxInt extends Function {

  /**
   * @param definedArgs
   */
  public MaxInt() {
    super(new ColumnBase[] { new ColumnBase("arg1", DataType.INT)});
  }

  /* (non-Javadoc)
   * @see nta.engine.function.Function#invoke(nta.datum.Datum[])
   */
  @Override
  public Datum invoke(Datum... datums) {
    if (datums.length == 1) {
      return datums[0];
    }
    return DatumFactory
        .createInt(Math.max(datums[0].asInt(), datums[1].asInt()));
  }

  /* (non-Javadoc)
   * @see nta.engine.function.Function#getResType()
   */
  @Override
  public DataType getResType() {
    return DataType.INT;
  }
}
