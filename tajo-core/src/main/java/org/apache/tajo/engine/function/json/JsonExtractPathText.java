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

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * json_extract_path_text(string, string) -
 * Extracts JSON string from a JSON string based on json path specified,
 * and returns JSON string pointed to by xPath.
 *
 *
 * Returns null if either argument is null.
 *
 * Example:
 * SELECT json_extract_path_text('{"sample" : {"name" : "tajo"}}','$.sample.name') FROM src LIMIT 1;\n"
 * -> result: 'tajo'
 */
@Description(
    functionName = "json_extract_path_text",
    description = "Returns JSON string pointed to by xPath",
    detail = "Extracts JSON string from a JSON string based on json path specified,\n"
        + "and returns JSON string pointed to by xPath.",
    example = "> SELECT json_extract_path_text('{\"sample\" : {\"name\" : \"tajo\"}}','$.sample.name');\n"
        + "tajo",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class JsonExtractPathText extends GeneralFunction {
  private JSONParser parser;
  private JsonPath jsonPath;

  public JsonExtractPathText() {
    super(new Column[]{
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("string", TajoDataTypes.Type.TEXT),
    });
    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    // default is JsonSmartMappingProvider
    try {

      JSONObject object = (JSONObject) parser.parse(params.getBytes(0));
      if (jsonPath == null) {
        jsonPath = JsonPath.compile(params.getText(1));
      }
      return DatumFactory.createText(jsonPath.read(object).toString());
    } catch (Exception e) {
      return NullDatum.get();
    }
  }
}
