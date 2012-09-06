/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import tajo.engine.eval.EvalNode;
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
