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

package org.apache.tajo.engine.json;

import com.google.gson.*;
import org.apache.tajo.engine.planner.global.InputContext;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class SourceContextAdapter implements GsonSerDerAdapter<InputContext> {

  @Override
  public InputContext deserialize(JsonElement jsonElement, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    JsonObject json = jsonElement.getAsJsonObject();
    ScanNode[] scanNodes = context.deserialize(json.get("body"), ScanNode[].class);
    InputContext srcContext = new InputContext();
    for (ScanNode scan : scanNodes) {
      srcContext.addScanNode(scan);
    }
    return srcContext;
  }

  @Override
  public JsonElement serialize(InputContext src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.add("body", context.serialize(src.getScanNodes(), ScanNode[].class));
    return json;
  }
}
