/**
 * 
 */
package tajo.engine.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.fs.Path;
import tajo.catalog.TableDesc;
import tajo.catalog.TableMeta;
import tajo.datum.Datum;
import tajo.datum.json.DatumAdapter;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.function.AggFunction;
import tajo.engine.function.Function;
import tajo.engine.function.GeneralFunction;
import tajo.engine.planner.logical.LogicalNode;

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
			builder.registerTypeAdapter(Path.class, new PathSerializer());
			builder.registerTypeAdapter(Path.class, new PathDeserializer());
			builder.registerTypeAdapter(TableDesc.class, new TableDescAdapter());
			builder.registerTypeAdapter(Class.class, new ClassNameSerializer());
			builder.registerTypeAdapter(Class.class, new ClassNameDeserializer());
			builder.registerTypeAdapter(LogicalNode.class, new LogicalNodeAdapter());
			builder.registerTypeAdapter(EvalNode.class, new EvalNodeAdapter());
			builder.registerTypeAdapter(TableMeta.class, new TableMetaAdapter());
			builder.registerTypeAdapter(Datum.class, new DatumTypeAdapter());
			builder.registerTypeAdapter(Function.class, new FunctionAdapter());
      builder.registerTypeAdapter(GeneralFunction.class, new FunctionAdapter());
      builder.registerTypeAdapter(AggFunction.class, new FunctionAdapter());
			builder.registerTypeAdapter(Datum.class, new DatumAdapter());
		}
	}

	public static Gson getInstance() {
	  init();
	  if (gson == null ) {
	    gson = builder.create();
	  }
	  return gson;
	}

	public static Gson getPrettyInstance() {
	  init();
	  if (gson == null ) {
	    gson = builder.setPrettyPrinting().create();
	  }
	  return gson;
	}
}
