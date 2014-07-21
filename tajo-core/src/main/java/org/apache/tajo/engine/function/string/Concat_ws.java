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

package org.apache.tajo.engine.function.string;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text concat_ws(sep text, str "any" [, str "any" [, ...] ])
 */
@Description(
    functionName = "concat_ws",
    description = "Concatenate all but first arguments with separators.",
    detail = "The first parameter is used as a separator. NULL arguments are ignored.",
    example = "> concat_ws(',', 'abcde', 2);\n"
        + "abcde,2",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT_ARRAY})}
)
public class Concat_ws extends GeneralFunction {
  @Expose private boolean hasMoreCharacters;

  public Concat_ws() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("text", TajoDataTypes.Type.TEXT),
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum sepDatum = params.get(0);

    if(sepDatum instanceof NullDatum) return NullDatum.get();

    String seperator = ((TextDatum)sepDatum).asChars();

    String opSperator = "";
    StringBuilder result = new StringBuilder();

    int paramSize = params.size();
    for(int i = 1; i < paramSize; i++) {
      Datum datum = params.get(i);
      if(datum instanceof NullDatum) {
        continue;
      } else {
        result.append(opSperator).append(datum.asChars());
      }
      opSperator = seperator;
    }
    return DatumFactory.createText(result.toString());
  }
}
