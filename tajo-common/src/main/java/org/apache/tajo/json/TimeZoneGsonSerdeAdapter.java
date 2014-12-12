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

/**
 * 
 */
package org.apache.tajo.json;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.TimeZone;

public class TimeZoneGsonSerdeAdapter implements GsonSerDerAdapter<TimeZone> {

	@Override
	public JsonElement serialize(TimeZone object, Type arg1, JsonSerializationContext arg2) {
		return new JsonPrimitive(object.getID());
	}

  @Override
  public TimeZone deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext context) throws JsonParseException {
    return TimeZone.getTimeZone(arg0.getAsJsonPrimitive().getAsString());
  }
}
