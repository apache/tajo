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

package org.apache.tajo.storage.json;


import net.minidev.json.JSONObject;
import org.apache.commons.net.util.Base64;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.NestedPathUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.text.TextLineSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.TimeZone;

public class JsonLineSerializer extends TextLineSerializer {
  // Full Path -> Type
  private final Map<String, Type> types;
  private final String [] projectedPaths;

  private final boolean hasTimezone;
  private final TimeZone timezone;

  public JsonLineSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);

    projectedPaths = SchemaUtil.convertColumnsToPaths(schema.getAllColumns(), true);
    types = SchemaUtil.buildTypeMap(schema.getAllColumns(), projectedPaths);

    hasTimezone = meta.containsOption(StorageConstants.TIMEZONE);
    timezone = TimeZone.getTimeZone(meta.getOption(StorageConstants.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE));
  }

  @Override
  public void init() {
  }

  private void putValue(JSONObject json,
                        String fullPath,
                        String [] pathElements,
                        int depth,
                        int fieldIndex,
                        Tuple input) throws IOException {
    String fieldName = pathElements[depth];

    if (input.isBlankOrNull(fieldIndex)) {
      return;
    }

    switch (types.get(fullPath)) {

    case BOOLEAN:
      json.put(fieldName, input.getBool(fieldIndex));
      break;

    case INT1:
    case INT2:
      json.put(fieldName, input.getInt2(fieldIndex));
      break;

    case INT4:
      json.put(fieldName, input.getInt4(fieldIndex));
      break;

    case INT8:
      json.put(fieldName, input.getInt8(fieldIndex));
      break;

    case FLOAT4:
      json.put(fieldName, input.getFloat4(fieldIndex));
      break;

    case FLOAT8:
      json.put(fieldName, input.getFloat8(fieldIndex));
      break;


    case TEXT:
    case VARCHAR:
      json.put(fieldName, input.getText(fieldIndex));
      break;

    case CHAR:
    case INET4:
    case DATE:
    case INTERVAL:
      json.put(fieldName, input.asDatum(fieldIndex).asChars());
      break;

    case TIMESTAMP:
      if (hasTimezone) {
        json.put(fieldName, TimestampDatum.asChars(input.getTimeDate(fieldIndex), timezone, false));
      } else {
        json.put(fieldName, input.asDatum(fieldIndex).asChars());
      }
      break;
    case TIME:
      if (hasTimezone) {
        json.put(fieldName, TimeDatum.asChars(input.getTimeDate(fieldIndex), timezone, false));
      } else {
        json.put(fieldName, input.asDatum(fieldIndex).asChars());
      }
      break;

    case BIT:
    case BINARY:
    case BLOB:
    case VARBINARY:
      json.put(fieldName,  Base64.encodeBase64String(input.getBytes(fieldIndex)));
      break;

    case NULL_TYPE:
      break;

    case RECORD:
      JSONObject record = json.containsKey(fieldName) ? (JSONObject) json.get(fieldName) : new JSONObject();
      json.put(fieldName, record);
      putValue(record, fullPath + "/" + pathElements[depth + 1], pathElements, depth + 1, fieldIndex, input);
      break;

    default:
      throw new TajoRuntimeException(
          new NotImplementedException("" + types.get(fullPath).name() + " for json"));
    }
  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {
    JSONObject jsonObject = new JSONObject();

    for (int i = 0; i < projectedPaths.length; i++) {
      String [] paths = projectedPaths[i].split(NestedPathUtil.PATH_DELIMITER);
      putValue(jsonObject, paths[0], paths, 0, i, input);
    }

    String jsonStr = jsonObject.toJSONString();
    byte [] jsonBytes = jsonStr.getBytes(TextDatum.DEFAULT_CHARSET);
    out.write(jsonBytes);
    return jsonBytes.length;
  }

  @Override
  public void release() {
  }
}
