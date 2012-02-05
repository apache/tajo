/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import nta.engine.function.Function;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

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
    jsonObj.addProperty("classname", className);
    JsonElement jsonElem = context.serialize(src);
    jsonObj.add("property", jsonElem);
    return jsonObj;
  }

  @Override
  public Function deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String className = jsonObject.get("classname").getAsJsonPrimitive().getAsString();
    
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
