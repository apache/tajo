package nta.engine.function;

import com.google.gson.Gson;
import nta.catalog.Column;
import nta.datum.Datum;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class AggFunction<T extends Datum> extends Function<T> {
  public AggFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public abstract void init();

  @Override
  public abstract void eval(Tuple params);

  public void merge(Tuple...parts) {
    for (Tuple part : parts) {
      eval(part);
    }
  }
  public abstract Tuple getPartialResult();
  public abstract T terminate();

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, AggFunction.class);
  }
}
