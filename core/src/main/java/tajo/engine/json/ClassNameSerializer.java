/**
 * 
 */
package tajo.engine.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class ClassNameSerializer implements JsonSerializer<Class> {

	@Override
	public JsonElement serialize(Class clazz, Type type,
			JsonSerializationContext ctx) {
		return new JsonPrimitive(clazz.getName());
	}

}
