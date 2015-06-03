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
 * int4 ascii(string text)
 */
@Description(
  functionName = "ascii",
  description = "ASCII code of the first character of the argument.",
  detail = "For UTF8 returns the Unicode code point of the character.\n"
        + "For other multibyte encodings, the argument must be an ASCII character.",
  example = "> SELECT ascii('x');\n"
          + "120",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT})}
)
public class Ascii extends GeneralFunction {


  public Ascii() {
    super(new Column[]{
            new Column("char", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    byte[] bytes = params.getBytes(0);
    if (bytes.length > 0) {
      return DatumFactory.createInt4(bytes[0]);
    } else {
      return DatumFactory.createInt4(0);
    }
  }
}
