/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.ipc.protocolrecords.Fragment;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class TableDescAdapter implements JsonSerializer<TableDesc>, JsonDeserializer<TableDesc> {

	@Override
	public TableDesc deserialize(JsonElement json, Type type,
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
	public JsonElement serialize(TableDesc src, Type typeOfSrc,
			JsonSerializationContext context) {
		JsonObject jsonObj = new JsonObject();
		String className = src.getClass().getCanonicalName();
		jsonObj.addProperty("classname", className);

		if (src.getClass().getSimpleName().equals("TableDescImpl")) {
			((TableDescImpl)src).initFromProto();
		} else if (src.getClass().getSimpleName().equals("Fragment")) {
			((Fragment)src).initFromProto();
		}
		JsonElement jsonElem = context.serialize(src);
		jsonObj.add("property", jsonElem);
		return jsonObj;
	}

}
