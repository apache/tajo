/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.engine.planner.logical.LogicalNode;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class LogicalNodeAdapter implements JsonSerializer<LogicalNode>, JsonDeserializer<LogicalNode> {

	@Override
	public LogicalNode deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		String className = jsonObject.get("classname").getAsJsonPrimitive().getAsString();
		
		Class clazz = null;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new JsonParseException(e);
		}
		return ctx.deserialize(jsonObject.get("property"), clazz);
	}

	@Override
	public JsonElement serialize(LogicalNode src, Type typeOfSrc,
			JsonSerializationContext context) {
		JsonObject jsonObj = new JsonObject();
		String className = src.getClass().getCanonicalName();
		jsonObj.addProperty("classname", className);
		JsonElement jsonElem = context.serialize(src);
		jsonObj.add("property", jsonElem);
		return jsonObj;
	}

}
