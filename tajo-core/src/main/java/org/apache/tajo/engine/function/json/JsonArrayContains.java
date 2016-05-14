/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function.json;

import net.minidev.json.JSONArray;
import net.minidev.json.parser.ParseException;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.AnyDatum;
import org.apache.tajo.datum.BooleanDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "json_array_contains",
    description = "Determine if the given value exists in the JSON array",
    detail = "Determine if the given value exists in the JSON array.",
    example = "> SELECT json_array_contains('[100, 200, 300]', 100);\n" +
        "t",
    returnType = Type.BOOLEAN,
    paramTypes = {@ParamTypes(paramTypes = {Type.TEXT, Type.ANY})}
)
public class JsonArrayContains extends ScalarJsonFunction {
  private boolean isLong;
  private boolean isDouble;
  private boolean isFirst = true;

  public JsonArrayContains() {
    super(new Column[]{
        new Column("json_array", TajoDataTypes.Type.TEXT),
        new Column("index", TajoDataTypes.Type.ANY)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    try {
      Object parsed = parser.parse(params.getBytes(0));
      if (parsed instanceof JSONArray) {
        JSONArray array = (JSONArray) parsed;
        Datum actualParam = ((AnyDatum)params.asDatum(1)).getActual();

        if (isFirst) {
          isFirst = false;
          if (actualParam.kind() == TajoDataTypes.Type.INT1 ||
              actualParam.kind() == TajoDataTypes.Type.INT2 ||
              actualParam.kind() == TajoDataTypes.Type.INT4 ||
              actualParam.kind() == TajoDataTypes.Type.INT8) {
            isLong = true;
          } else if (actualParam.kind() == TajoDataTypes.Type.FLOAT4 ||
              actualParam.kind() == TajoDataTypes.Type.FLOAT8) {
            isDouble = true;
          }
        }

        for (Object eachElem : array) {
          if (isLong && eachElem instanceof Long) {
            if (actualParam.asInt8() == (Long)eachElem) {
              return BooleanDatum.TRUE;
            }
          } else if (isDouble && eachElem instanceof Double) {
            if (actualParam.asFloat8() == (Double)eachElem) {
              return BooleanDatum.TRUE;
            }
          } else {
            if (actualParam.asChars().equals(eachElem.toString())) {
              return BooleanDatum.TRUE;
            }
          }
        }
        return BooleanDatum.FALSE;
      } else {
        return NullDatum.get();
      }
    } catch (ParseException e) {
      return NullDatum.get();
    }
  }
}
