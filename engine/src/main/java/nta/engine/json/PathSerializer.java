/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import org.apache.hadoop.fs.Path;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * @author jihoon
 *
 */
public class PathSerializer implements JsonSerializer<Path> {

	@Override
	public JsonElement serialize(Path arg0, Type arg1,
			JsonSerializationContext arg2) {
		return new JsonPrimitive(arg0.toString());
	}

}
