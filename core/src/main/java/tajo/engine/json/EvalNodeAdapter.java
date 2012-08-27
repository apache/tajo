/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.engine.exec.eval.EvalNode;

import java.lang.reflect.Type;

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
