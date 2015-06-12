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

import org.apache.commons.codec.binary.Hex;
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
 * text to_hex(text)
 * text to_hex(int)
 */
@Description(
  functionName = "to_hex",
  description = "Convert the argument to hexadecimal",
  example = "SELECT to_hex(15);\n"
          + "F",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4}),
          @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8})}
)
public class ToHex extends GeneralFunction {

  public ToHex() {
    super(new Column[] {
        new Column("n", TajoDataTypes.Type.INT8)
    });
  }

  public String trimZero(String hexString) {
    int len = hexString.length();
    for (int i = 0; i < len; i++) {
        if (hexString.charAt(i) != '0') {
            return hexString.substring(i);
        }
    }

    return hexString;
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    String ret = new String(Hex.encodeHex(params.getBytes(0)));
    return DatumFactory.createText(trimZero(ret));
  }
}
