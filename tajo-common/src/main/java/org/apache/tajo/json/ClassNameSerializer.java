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
package org.apache.tajo.json;

import com.google.gson.*;
import org.apache.tajo.util.ClassUtil;

import java.lang.reflect.Type;

public class ClassNameSerializer implements GsonSerDerAdapter<Class> {

	@Override
	public JsonElement serialize(Class clazz, Type type,
			JsonSerializationContext ctx) {
		return new JsonPrimitive(clazz.getName());
	}

  @Override
  public Class deserialize(JsonElement json, Type type,
                           JsonDeserializationContext ctx) throws JsonParseException {
    try {
      return ClassUtil.forName(json.getAsString());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }
}
