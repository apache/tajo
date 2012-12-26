/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.engine.parser.QueryBlock.FromTable;
import tajo.storage.Fragment;

import java.lang.reflect.Type;

public class FromTableDeserializer implements JsonDeserializer<FromTable> {

	@Override
	public FromTable deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		Gson gson = GsonCreator.getInstance();
		JsonObject fromTableObj = json.getAsJsonObject();
		boolean isFragment = fromTableObj.get("isFragment").getAsBoolean();
		TableDesc desc;
		if (isFragment) {
			desc = gson.fromJson(fromTableObj.get("desc"), Fragment.class);
		} else {
			desc = gson.fromJson(fromTableObj.get("desc"), TableDescImpl.class);
		}
		
		return new FromTable(desc, fromTableObj.get("alias").getAsString());
	}

}
