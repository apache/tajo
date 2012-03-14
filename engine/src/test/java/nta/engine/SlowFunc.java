/**
 * 
 */
package nta.engine;

import org.mortbay.log.Log;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;

/**
 * @author hyunsik
 * 
 */
public class SlowFunc extends Function {
  public SlowFunc() {
    super(new Column[] { new Column("name", DataType.STRING) });
  }

  @Override
  public Datum invoke(Datum... datums) {
    try {
      Thread.sleep(1000);
      Log.info("Sleepy... z...z...z");
    } catch (InterruptedException ie) {
      
    }
    return datums[0];
  }

  @Override
  public DataType getResType() {
    return DataType.STRING;
  }
}
