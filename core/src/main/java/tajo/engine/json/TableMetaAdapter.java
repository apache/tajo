/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.catalog.TableMeta;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class TableMetaAdapter implements JsonSerializer<TableMeta>, JsonDeserializer<TableMeta> {

	@Override
	public TableMeta deserialize(JsonElement json, Type typeOfT,
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

	@Override
	public JsonElement serialize(TableMeta src, Type typeOfSrc,
			JsonSerializationContext context) {
		src.initFromProto();
		JsonObject jsonObj = new JsonObject();
		String className = src.getClass().getCanonicalName();
		jsonObj.addProperty("classname", className);
		JsonElement jsonElem = context.serialize(src);
		jsonObj.add("property", jsonElem);
		return jsonObj;
	}

}
