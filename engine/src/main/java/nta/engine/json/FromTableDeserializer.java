/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock.FromTable;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * @author jihoon
 *
 */
public class FromTableDeserializer implements JsonDeserializer<FromTable> {

	@Override
	public FromTable deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		Gson gson = GsonCreator.getInstance();
		JsonObject fromTableObj = json.getAsJsonObject();
		boolean isFragment = fromTableObj.get("isFragment").getAsBoolean();
		TableDesc desc = null;
		if (isFragment) {
			desc = gson.fromJson(fromTableObj.get("desc"), Fragment.class);
		} else {
			desc = gson.fromJson(fromTableObj.get("desc"), TableDescImpl.class);
		}
		
		return new FromTable(desc, fromTableObj.get("alias").getAsString());
	}

}
