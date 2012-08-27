package tajo.engine.function;

import com.google.gson.Gson;
import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos;
import tajo.datum.Datum;
import tajo.engine.json.GsonCreator;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class AggFunction<T extends Datum> extends Function<T> {

  public AggFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  public abstract FunctionContext newContext();

  public abstract void eval(FunctionContext ctx, Tuple params);

  public void merge(FunctionContext ctx, Tuple part) {
    eval(ctx, part);
  }

  public abstract Datum getPartialResult(FunctionContext ctx);

  public abstract CatalogProtos.DataType [] getPartialResultType();

  public abstract T terminate(FunctionContext ctx);

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, AggFunction.class);
  }
}
