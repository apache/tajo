package nta.engine.function.builtin;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.IntDatum;
import nta.engine.function.GeneralFunction;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

import java.util.Random;

/**
 * @author Hyunsik Choi
 */
public class RandomInt extends GeneralFunction<Datum> {
  private Random random;

  public RandomInt() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.INT)
    });
    random = new Random(System.nanoTime());
  }

  @Override
  public Datum eval(Tuple params) {
    return DatumFactory.createInt(random.nextInt(params.get(0).asInt()));
  }
}
