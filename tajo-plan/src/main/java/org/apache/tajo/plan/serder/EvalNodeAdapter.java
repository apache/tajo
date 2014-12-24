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
import org.apache.tajo.json.GsonSerDerAdapter;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalType;

import java.lang.reflect.Type;

public class EvalNodeAdapter implements GsonSerDerAdapter<EvalNode> {

  @Override
  public EvalNode deserialize(JsonElement json, Type type,
                              JsonDeserializationContext ctx) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String nodeName = CommonGsonHelper.getOrDie(jsonObject, "type").getAsString();
    Class clazz = EvalType.valueOf(nodeName).getBaseClass();
    return ctx.deserialize(jsonObject.get("body"), clazz);
  }

  @Override
  public JsonElement serialize(EvalNode evalNode, Type type,
                               JsonSerializationContext ctx) {
    JsonObject json = new JsonObject();
    json.addProperty("type", evalNode.getType().name());
    json.add("body", ctx.serialize(evalNode));
    return json;
  }
}
