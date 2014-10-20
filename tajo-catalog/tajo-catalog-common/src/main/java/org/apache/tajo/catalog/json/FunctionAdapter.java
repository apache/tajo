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

package org.apache.tajo.catalog.json;

import com.google.gson.*;
import org.apache.tajo.function.Function;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class FunctionAdapter implements GsonSerDerAdapter<Function> {

  @Override
  public JsonElement serialize(Function src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    String className = src.getClass().getName();
    jsonObj.addProperty("class", className);
    JsonElement jsonElem = context.serialize(src);
    jsonObj.add("body", jsonElem);
    return jsonObj;
  }

  @Override
  public Function deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String className = jsonObject.get("class").getAsJsonPrimitive().getAsString();
    
    Class clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new JsonParseException(e);
    }
    return context.deserialize(jsonObject.get("body"), clazz);
  }
}
