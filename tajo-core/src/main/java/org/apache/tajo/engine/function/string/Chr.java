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
 * char chr(value int4)
 */
@Description(
  functionName = "chr",
  description = "Character with the given code.",
  detail = "For UTF8 the argument is treated as a Unicode code point. "
    + "For other multibyte encodings the argument must designate an ASCII character.",
  example = "> SELECT chr(65);\n"
          + "A",
  returnType = TajoDataTypes.Type.CHAR,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class Chr extends GeneralFunction {
  public Chr() {
    super(new Column[]{
            new Column("n", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    int value = params.getInt4(0);
    if (value <= 0 || value > 65525) {
        return NullDatum.get();
    } else {
        return DatumFactory.createText(String.valueOf((char)value));
    }
  }
}
