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
 * text btrim(string text [, characters text])
 */
@Description(
  functionName = "trim",
  synonyms = {"btrim"},
  description = " Remove the longest string consisting only of "
          + " characters in characters (a space by default) "
          + "from the start and end of string.",
  example = "> SELECT trim('xyxtrimyyx', 'xy');\n"
          + "trim",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT}),
          @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,TajoDataTypes.Type.TEXT})}
)
public class BTrim extends GeneralFunction {
  @Expose private boolean hasTrimCharacters;

  public BTrim() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] paramTypes) {
    if (paramTypes.length == 2) {
      hasTrimCharacters = true;
    }
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    String input = params.getText(0);
    if (!hasTrimCharacters) {
      return DatumFactory.createText(StringUtils.strip(input, null));
    } else {
      return DatumFactory.createText(StringUtils.strip(input, params.getText(1)));
    }
  }
}
