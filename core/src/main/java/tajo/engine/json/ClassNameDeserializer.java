/**
 * 
 */
package tajo.engine.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class ClassNameDeserializer implements JsonDeserializer<Class> {

	@Override
	public Class deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		try {
			return Class.forName(json.getAsString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

}
