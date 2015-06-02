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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text substr(string text, from int4 [, length int4])
 */
@Description(
  functionName = "substr",
  description = "Extract substring.",
  example = "> SELECT substr('alphabet', 3, 2);\n"
          + "ph",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {
    @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,TajoDataTypes.Type.INT4}),
    @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,TajoDataTypes.Type.INT4,
                              TajoDataTypes.Type.INT4})
  } 
)
public class Substr extends GeneralFunction {
  public Substr() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("from", TajoDataTypes.Type.INT4),
        new Column("length", TajoDataTypes.Type.INT4)    //optional
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }
    if (params.size() > 2 && params.isBlankOrNull(2)) {
      return NullDatum.get();
    }

    String value = params.getText(0);
    int start = params.getInt4(1) - 1;

    int from = Math.max(0, start);
    int length = params.size() > 2 ? params.getInt4(2) : -1;

    int to = value.length();
    if (length >= 0) {
      to = Math.min(start + length, to);
    }

    if (from >= to) {
      return DatumFactory.createText("");
    }

    return DatumFactory.createText(value.substring(from, to));
  }
}
