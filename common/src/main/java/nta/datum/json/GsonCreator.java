/**
 * 
 */
package nta.datum.json;

import nta.datum.Datum;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author jihoon
 *
 */
public class GsonCreator {

	private static GsonBuilder builder;
	private static Gson gson;
	
	private static void init() {
		if (builder == null) {
			builder = new GsonBuilder().excludeFieldsWithoutExposeAnnotation();
			builder.registerTypeAdapter(Datum.class, new DatumAdapter());
		} 
		if (gson == null ) {
			gson = builder.create();
		}
	}

	public static Gson getInstance() {
		init();
		return gson;
	}
}
