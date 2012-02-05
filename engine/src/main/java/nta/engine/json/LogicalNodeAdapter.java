/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import nta.engine.planner.logical.LogicalNode;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

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
