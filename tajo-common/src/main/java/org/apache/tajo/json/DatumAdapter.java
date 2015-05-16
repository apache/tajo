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

package org.apache.tajo.json;

import com.google.gson.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;

import java.lang.reflect.Type;

public class DatumAdapter implements GsonSerDerAdapter<Datum> {

	@Override
	public Datum deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		String typeName = CommonGsonHelper.getOrDie(jsonObject, "type").getAsString();
    TajoDataTypes.Type type = TajoDataTypes.Type.valueOf(typeName);
    switch (type) {
    case DATE:
      return new DateDatum(CommonGsonHelper.getOrDie(jsonObject, "value").getAsInt());
    case TIME:
      return new TimeDatum(CommonGsonHelper.getOrDie(jsonObject, "value").getAsLong());
    case TIMESTAMP:
      return new TimestampDatum(CommonGsonHelper.getOrDie(jsonObject, "value").getAsLong());
    case INTERVAL:
      String[] values = CommonGsonHelper.getOrDie(jsonObject, "value").getAsString().split(",");
      return new IntervalDatum(Integer.parseInt(values[0]), Long.parseLong(values[1]));
    case ANY:
      return new AnyDatum(deserialize(CommonGsonHelper.getOrDie(jsonObject, "actual"), typeOfT, context));
    default:
      return context.deserialize(CommonGsonHelper.getOrDie(jsonObject, "body"),
          DatumFactory.getDatumClass(TajoDataTypes.Type.valueOf(typeName)));
    }
	}

	@Override
	public JsonElement serialize(Datum src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject jsonObj = new JsonObject();
		jsonObj.addProperty("type", src.type().name());
    switch (src.type()) {
    case DATE:
      jsonObj.addProperty("value", src.asInt4());
      break;
    case TIME:
      jsonObj.addProperty("value", src.asInt8());
      break;
    case TIMESTAMP:
      jsonObj.addProperty("value", src.asInt8());
      break;
    case INTERVAL:
      IntervalDatum interval = (IntervalDatum)src;
      jsonObj.addProperty("value", interval.getMonths() + "," + interval.getMilliSeconds());
      break;
    case ANY:
      jsonObj.add("actual", serialize(((AnyDatum) src).getActual(), typeOfSrc, context));
      break;
    default:
      jsonObj.add("body", context.serialize(src));
    }

		return jsonObj;
	}
}
