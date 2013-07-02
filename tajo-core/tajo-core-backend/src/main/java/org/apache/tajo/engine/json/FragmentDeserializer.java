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

/**
 * 
 */
package org.apache.tajo.engine.json;

import com.google.gson.*;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMetaImpl;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.storage.Fragment;

import java.lang.reflect.Type;

public class FragmentDeserializer implements JsonDeserializer<Fragment> {

	@Override
	public Fragment deserialize(JsonElement json, Type type,
			JsonDeserializationContext ctx) throws JsonParseException {
		Gson gson = GsonCreator.getInstance();
		JsonObject fragObj = json.getAsJsonObject();
		JsonObject metaObj = fragObj.get("meta").getAsJsonObject();
		TableMetaImpl meta = new TableMetaImpl(
		    gson.fromJson(metaObj.get("schema"), Schema.class), 
				gson.fromJson(metaObj.get("storeType"), StoreType.class), 
				gson.fromJson(metaObj.get("options"), Options.class));
		Fragment fragment = new Fragment(fragObj.get("tabletId").getAsString(), 
				gson.fromJson(fragObj.get("path"), Path.class), 
				meta, 
				fragObj.get("startOffset").getAsLong(), 
				fragObj.get("length").getAsLong(), null);
		return fragment;
	}

}
