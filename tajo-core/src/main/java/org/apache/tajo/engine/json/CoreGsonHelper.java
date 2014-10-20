/**
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

package org.apache.tajo.engine.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.function.Function;
import org.apache.tajo.catalog.json.FunctionAdapter;
import org.apache.tajo.catalog.json.TableMetaAdapter;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.json.*;
import org.apache.tajo.util.TUtil;

import java.lang.reflect.Type;
import java.util.Map;

public class CoreGsonHelper {
  private static Gson gson;
  private static Gson gsonPretty;

  private CoreGsonHelper() {
  }
	
	private static Map<Type, GsonSerDerAdapter> registerAdapters() {
    Map<Type, GsonSerDerAdapter> adapters = TUtil.newHashMap();
    adapters.put(Path.class, new PathSerializer());
    adapters.put(Class.class, new ClassNameSerializer());
    adapters.put(LogicalNode.class, new LogicalNodeAdapter());
    adapters.put(EvalNode.class, new EvalNodeAdapter());
    adapters.put(TableMeta.class, new TableMetaAdapter());
    adapters.put(Function.class, new FunctionAdapter());
    adapters.put(GeneralFunction.class, new FunctionAdapter());
    adapters.put(AggFunction.class, new FunctionAdapter());
    adapters.put(Datum.class, new DatumAdapter());
    adapters.put(DataType.class, new DataTypeAdapter());

    return adapters;
	}

	public static Gson getInstance() {
	  if (gson == null ) {
      GsonHelper helper = new GsonHelper(registerAdapters());
      gson = helper.getGson();
	  }
	  return gson;
	}

  public static Gson getPrettyInstance() {
    if (gsonPretty == null) {
      GsonBuilder prettyBuilder = new GsonBuilder()
          .setPrettyPrinting()
          .excludeFieldsWithoutExposeAnnotation();
      GsonHelper.registerAdapters(prettyBuilder, registerAdapters());
      gsonPretty = prettyBuilder.create();
    }

    return gsonPretty;
  }

  public static String toJson(GsonObject object, Class<? extends GsonObject> clazz) {
    return getInstance().toJson(object, clazz);
  }

  public static <T extends GsonObject> T fromJson(String json, Class<T> clazz) {
    return getInstance().fromJson(json, clazz);
  }
}
