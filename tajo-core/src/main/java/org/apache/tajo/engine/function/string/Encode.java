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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
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
 * bytearray encode(data bytea, format text)
 */
@Description(
  functionName = "encode",
  description = "Encode binary data into a textual representation.",
  detail = "Supported formats are: base64, hex, escape. escape converts zero bytes and "
        + "high-bit-set bytes to octal sequences (\\nnn) and doubles backslashes.",
  example = "> SELECT encode(E'123\\\\000\\\\001', 'base64');\n"
          + "MTIzAAE=",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class Encode extends GeneralFunction {
  public Encode() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("format", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    String encodedBase64Text="";
    String encodedHexString="";

    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    String input = params.getText(0);
    String formatType = params.getText(1).toLowerCase();
    if(formatType.equals("base64")) {
      try {
        // Base64
        encodedBase64Text = new String(Base64.encodeBase64(StringEscapeUtils.unescapeJava(input).getBytes()));
      }
      catch (Exception e) {
        return NullDatum.get();
      }

      return DatumFactory.createText(encodedBase64Text);
    }
    else if(formatType.equals("hex")) {
      try {
        // Hex
        encodedHexString = HexStringConverter.getInstance().encodeHex(StringEscapeUtils.unescapeJava(input));
      }
      catch (Exception e) {
        return NullDatum.get();
      }

      return DatumFactory.createText(encodedHexString);
    }
    else
      return NullDatum.get();
  }
}
