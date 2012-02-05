/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import org.apache.hadoop.fs.Path;

import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.ipc.protocolrecords.Fragment;

import com.google.gson.Gson;
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
