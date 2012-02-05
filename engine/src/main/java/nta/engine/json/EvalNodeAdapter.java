/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import nta.engine.exec.eval.EvalNode;

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
public class EvalNodeAdapter implements JsonSerializer<EvalNode>, JsonDeserializer<EvalNode> {

	@Override
	public EvalNode deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		JsonObject jsonObj = json.getAsJsonObject();
		String className = jsonObj.get("type").getAsString();
		JsonElement elem = jsonObj.get("properties");
		
		try {
			return ctx.deserialize(elem, Class.forName(className));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public JsonElement serialize(EvalNode evalNode, Type type,
			JsonSerializationContext ctx) {
		JsonObject json = new JsonObject();
		json.add("type", new JsonPrimitive(evalNode.getClass().getName()));
		json.add("properties", ctx.serialize(evalNode, evalNode.getClass()));
		return json;
	}

}
