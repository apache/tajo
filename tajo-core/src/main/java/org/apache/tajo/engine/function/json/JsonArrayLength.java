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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "json_array_length",
    description = "Returns the length of json array",
    detail = "Returns the length of json array.",
    example = "> SELECT json_array_length('[100, 200, 300]');\n" +
        "3",
    returnType = Type.INT8,
    paramTypes = {@ParamTypes(paramTypes = {Type.TEXT})}
)
public class JsonArrayLength extends ScalarJsonFunction {

  public JsonArrayLength() {
    super(new Column[]{
        new Column("string", TajoDataTypes.Type.TEXT),
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }
    try {
      Object parsed = parser.parse(params.getBytes(0));
      if (parsed instanceof JSONArray) {
        JSONArray array = (JSONArray) parsed;
        return DatumFactory.createInt8(array.size());
      } else {
        return NullDatum.get();
      }
    } catch (ParseException e) {
      return NullDatum.get();
    }
  }
}
