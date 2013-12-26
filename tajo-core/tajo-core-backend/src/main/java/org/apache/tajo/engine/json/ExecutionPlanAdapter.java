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

import com.google.common.base.Preconditions;
import com.google.gson.*;
import org.apache.tajo.engine.planner.global.ExecutionPlan;
import org.apache.tajo.engine.planner.global.ExecutionPlan.ExecutionPlanJsonHelper;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class ExecutionPlanAdapter implements GsonSerDerAdapter<ExecutionPlan> {

  @Override
  public ExecutionPlan deserialize(JsonElement jsonElement, Type type,
                                   JsonDeserializationContext context) throws JsonParseException {
    JsonObject json = jsonElement.getAsJsonObject();
    String typeName = json.get("type").getAsJsonPrimitive().getAsString();
    Preconditions.checkState(typeName.equals("ExecutionPlan"));
    ExecutionPlanJsonHelper helper = context.deserialize(json.get("body"), ExecutionPlanJsonHelper.class);
    return helper.toExecutionPlan();
  }

  @Override
  public JsonElement serialize(ExecutionPlan src, Type type, JsonSerializationContext context) {
    ExecutionPlanJsonHelper helper = new ExecutionPlanJsonHelper(src);
    JsonObject json = new JsonObject();
    json.addProperty("type", "ExecutionPlan");
    json.add("body", context.serialize(helper, ExecutionPlanJsonHelper.class));
    return json;
  }
}
