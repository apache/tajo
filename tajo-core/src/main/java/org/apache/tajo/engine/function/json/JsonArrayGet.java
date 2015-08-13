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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "json_array_get",
    description = "Returns the element at the specified index into the JSON array",
    detail = "Returns the element at the specified index into the JSON array.\n " +
        "This function returns an element indexed from the end of an array with a negative index,\n " +
        "and null if the element at the specified index doesn’t exist.",
    example = "> SELECT json_array_get('[100, 200, 300]', 1);\n" +
        "200",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4})}
)
public class JsonArrayGet extends ScalarJsonFunction {
  private int realIndex = Integer.MAX_VALUE;

  public JsonArrayGet() {
    super(new Column[]{
        new Column("json_array", TajoDataTypes.Type.TEXT),
        new Column("index", TajoDataTypes.Type.INT4)
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
        if (realIndex == Integer.MAX_VALUE) {
          int givenIndex = params.getInt4(1);
          // Zero and positive given index points out the element from the left side,
          // while negative given index points out the element from the right side.
          realIndex = givenIndex < 0 ? array.size() + givenIndex : givenIndex;
        }

        if (realIndex >= array.size() || realIndex < 0) {
          return NullDatum.get();
        } else {
          return DatumFactory.createText(array.get(realIndex).toString());
        }
      } else {
        return NullDatum.get();
      }
    } catch (ParseException e) {
      return NullDatum.get();
    }
  }
}
