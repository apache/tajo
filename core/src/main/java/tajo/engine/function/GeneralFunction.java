package tajo.engine.function;

import com.google.gson.Gson;
import tajo.catalog.Column;
import tajo.datum.Datum;
import tajo.engine.json.GsonCreator;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class GeneralFunction<T extends Datum> extends Function<T> {
  public GeneralFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  public abstract Datum eval(Tuple params);

	public enum Type {
	  AGG,
	  GENERAL
	}

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, GeneralFunction.class);
  }
}