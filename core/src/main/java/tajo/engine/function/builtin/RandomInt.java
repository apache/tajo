package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.function.GeneralFunction;
import tajo.storage.Tuple;

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
