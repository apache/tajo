/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.json;

import com.google.gson.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;

import java.lang.reflect.Type;


public class DataTypeAdapter implements GsonSerDerAdapter<DataType> {

  @Override
  public DataType deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {

    JsonObject obj = (JsonObject) json;
    DataType.Builder builder = DataType.newBuilder();
    TajoDataTypes.Type type = TajoDataTypes.Type.valueOf(CommonGsonHelper.getOrDie(obj, "type").getAsString());
    builder.setType(type);

    JsonElement len = obj.get("len");
    if (len != null) {
      builder.setLength(len.getAsInt());
    }
    JsonElement code = obj.get("code");
    if (code != null) {
      builder.setCode(code.getAsString());
    }
    return builder.build();
  }

  @Override
  public JsonElement serialize(DataType src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("type", src.getType().name());
    if (src.hasLength()) {
      json.addProperty("len", src.getLength());
    }
    if (src.hasCode()) {
      json.addProperty("code", src.getCode());
    }

    return json;
  }
}
