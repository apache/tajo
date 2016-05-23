/*
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

package org.apache.tajo.catalog.json;

import com.google.gson.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaLegacy;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.function.Function;
import org.apache.tajo.json.CommonGsonHelper;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class SchemaAdapter implements GsonSerDerAdapter<Schema> {

  @Override
  public JsonElement serialize(Schema src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("version", src instanceof SchemaLegacy ? "1" : "2");
    JsonElement jsonElem = context.serialize(src);
    jsonObj.add("body", jsonElem);
    return jsonObj;
  }

  @Override
  public Schema deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    int version = CommonGsonHelper.getOrDie(jsonObject, "version").getAsJsonPrimitive().getAsInt();
    
    if (version == 1) {
      return context.deserialize(CommonGsonHelper.getOrDie(jsonObject, "body"), SchemaLegacy.class);
    } else {
      throw new TajoInternalError("Schema version 2 is not supported yet");
    }
  }
}
