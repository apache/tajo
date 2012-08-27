/**
 * 
 */
package tajo.engine.json;

import com.google.gson.*;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.ipc.protocolrecords.Fragment;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class FragmentDeserializer implements JsonDeserializer<Fragment> {

	@Override
	public Fragment deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		Gson gson = GsonCreator.getInstance();
		JsonObject fragObj = json.getAsJsonObject();
		JsonObject metaObj = fragObj.get("meta").getAsJsonObject();
		TableMetaImpl meta = new TableMetaImpl(
		    gson.fromJson(metaObj.get("schema"), Schema.class), 
				gson.fromJson(metaObj.get("storeType"), StoreType.class), 
				gson.fromJson(metaObj.get("options"), Options.class));
		Fragment fragment = new Fragment(fragObj.get("tabletId").getAsString(), 
				gson.fromJson(fragObj.get("path"), Path.class), 
				meta, 
				fragObj.get("startOffset").getAsLong(), 
				fragObj.get("length").getAsLong());
		return fragment;
	}

}
