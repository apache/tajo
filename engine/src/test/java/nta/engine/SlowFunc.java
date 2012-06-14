/**
 * 
 */
package nta.engine;

import nta.storage.Tuple;
import org.mortbay.log.Log;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.GeneralFunction;

/**
 * @author hyunsik
 * 
 */
public class SlowFunc extends GeneralFunction {
  private Datum param;

  public SlowFunc() {
    super(new Column[] { new Column("name", DataType.STRING) });
  }

  @Override
  public void init() {
  }

  @Override
  public void eval(Tuple params) {
    param = params.get(0);
  }

  @Override
  public Datum terminate() {
    try {
      Thread.sleep(1000);
      Log.info("Sleepy... z...z...z");
    } catch (InterruptedException ie) {

    }
    return param;
  }
}
