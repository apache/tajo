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
package org.apache.tajo.plan.serder;

import com.google.gson.*;
import org.apache.tajo.json.CommonGsonHelper;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class LogicalNodeAdapter implements GsonSerDerAdapter<LogicalNode> {

  @Override
  public LogicalNode deserialize(JsonElement src, Type type,
                                 JsonDeserializationContext ctx) throws JsonParseException {
    JsonObject jsonObject = src.getAsJsonObject();
    String nodeName = CommonGsonHelper.getOrDie(jsonObject, "type").getAsString();
    Class clazz = NodeType.valueOf(nodeName).getBaseClass();
    return ctx.deserialize(jsonObject.get("body"), clazz);
  }

  @Override
  public JsonElement serialize(LogicalNode src, Type typeOfSrc,
                               JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("type", src.getType().name());
    json.add("body", context.serialize(src));
    return json;
  }
}
