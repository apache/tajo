/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.engine.function.Function;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class FunctionAdapter implements JsonDeserializer<Function>, JsonSerializer<Function> {

  @Override
  public JsonElement serialize(Function src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    String className = src.getClass().getCanonicalName();
    jsonObj.addProperty("className", className);
    JsonElement jsonElem = context.serialize(src);
    jsonObj.add("property", jsonElem);
    return jsonObj;
  }

  @Override
  public Function deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String className = jsonObject.get("className").getAsJsonPrimitive().getAsString();
    
    Class clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new JsonParseException(e);
    }
    return context.deserialize(jsonObject.get("property"), clazz);
  }
}
