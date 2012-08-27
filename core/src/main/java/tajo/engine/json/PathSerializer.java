/**
 * 
 */
package tajo.engine.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Type;

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
