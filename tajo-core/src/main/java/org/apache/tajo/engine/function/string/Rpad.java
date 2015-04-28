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
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;


/**
 * Function definition
 *
 * text rpad(string text, length int [, fill text])
 */
@Description(
  functionName = "rpad",
  description = "Fill up the string to length length by appending the characters fill (a space by default)",
  detail = "If the string is already longer than length then it is truncated.",
  example = "> SELECT rpad('hi', 5, 'xy');\n"
      + "hixyx",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4}),
                @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4, TajoDataTypes.Type.TEXT})}
)
public class Rpad extends GeneralFunction {
  @Expose private boolean hasFillCharacters;

  public Rpad() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("length", TajoDataTypes.Type.INT4),
        new Column("fill_text", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] paramTypes) {
    if (paramTypes.length == 3) {
      hasFillCharacters = true;
    }
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    String fillText;
    if (hasFillCharacters) {
      fillText = params.getText(2);
    } else {
      fillText = " ";
    }

    String input = params.getText(0);
    int expected = params.getInt4(1);

    int templen = expected - params.size(0);

    if (templen <= 0) {
      return DatumFactory.createText(input.substring(0, expected));
    }

    return DatumFactory.createText(StringUtils.rightPad(input, expected, fillText));
  }
}
