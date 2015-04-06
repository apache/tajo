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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.common.exception.NotImplementedException;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.text.TextLineSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class JsonLineSerializer extends TextLineSerializer {
  private static ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();

  private Type [] types;
  private String [] simpleNames;
  private int columnNum;


  public JsonLineSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {
    types = SchemaUtil.toTypes(schema);
    simpleNames = SchemaUtil.toSimpleNames(schema);
    columnNum = schema.size();
  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {
    JSONObject jsonObject = new JSONObject();

    for (int i = 0; i < columnNum; i++) {
      if (input.isNull(i)) {
        continue;
      }

      String fieldName = simpleNames[i];
      Type type = types[i];

      switch (type) {

      case BOOLEAN:
        jsonObject.put(fieldName, input.getBool(i));
        break;

      case INT1:
      case INT2:
        jsonObject.put(fieldName, input.getInt2(i));
        break;

      case INT4:
        jsonObject.put(fieldName, input.getInt4(i));
        break;

      case INT8:
        jsonObject.put(fieldName, input.getInt8(i));
        break;

      case FLOAT4:
        jsonObject.put(fieldName, input.getFloat4(i));
        break;

      case FLOAT8:
        jsonObject.put(fieldName, input.getFloat8(i));
        break;

      case CHAR:
      case TEXT:
      case VARCHAR:
      case INET4:
      case TIMESTAMP:
      case DATE:
      case TIME:
      case INTERVAL:
        jsonObject.put(fieldName, input.getText(i));
        break;

      case BIT:
      case BINARY:
      case BLOB:
      case VARBINARY:
        jsonObject.put(fieldName,  Base64.encodeBase64String(input.getBytes(i)));
        break;

      case NULL_TYPE:
        break;

      default:
        throw new NotImplementedException(types[i].name() + " is not supported.");
      }
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
