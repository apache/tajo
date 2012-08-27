/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.QueryBlock.FromTable;

import java.lang.reflect.Type;

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
