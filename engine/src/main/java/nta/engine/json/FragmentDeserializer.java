/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import org.apache.hadoop.fs.Path;

import nta.catalog.Options;
import nta.catalog.Schema;
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
		TableMetaImpl meta = new TableMetaImpl(gson.fromJson(metaObj.get("schema"), Schema.class), 
				gson.fromJson(metaObj.get("storeType"), StoreType.class));
		meta.setOptions(gson.fromJson(metaObj.get("options"), Options.class));
		Fragment fragment = new Fragment(fragObj.get("tabletId").getAsString(), 
				gson.fromJson(fragObj.get("path"), Path.class), 
				meta, 
				fragObj.get("startOffset").getAsLong(), 
				fragObj.get("length").getAsLong());
		return fragment;
	}

}
