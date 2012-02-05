/**
 * 
 */
package nta.engine.json;

import java.lang.reflect.Type;

import org.apache.hadoop.fs.Path;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * @author jihoon
 *
 */
public class PathDeserializer implements JsonDeserializer<Path> {

	@Override
	public Path deserialize(JsonElement arg0, Type arg1,
			JsonDeserializationContext arg2) throws JsonParseException {
		return new Path(arg0.getAsJsonPrimitive().getAsString());
	}

}
