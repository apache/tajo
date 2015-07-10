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

import java.nio.ByteBuffer;

/**
 * Function definition
 *
 * int strposb(string text, substring text))
 */
@Description(
  functionName = "strposb",
  description = "Binary location of specified substring.",
  example = "> SELECT strpos('tajo', 'aj');\n"
      + "2",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,
          TajoDataTypes.Type.TEXT})}
)
public class StrPosb extends GeneralFunction {
  public StrPosb() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("substring", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    String value = params.getText(0);
    String substring = params.getText(1);
    if (substring.length() == 0) {
      return DatumFactory.createInt4(1);
    }

    return DatumFactory.createInt4(findText(value, substring) + 1);
  }

  /**
   * finds the location of specified substring.
   * @param value
   * @param substring
   * @return
   */
  public static int findText(String value, String substring) {
    //This method is copied from Hive's GenericUDFUtils.findText()
    int start = 0;
    byte[] valueBytes = value.getBytes();
    byte[] substrBytes = substring.getBytes();

    ByteBuffer src = ByteBuffer.wrap(valueBytes, 0, valueBytes.length);
    ByteBuffer tgt = ByteBuffer.wrap(substrBytes, 0, substrBytes.length);
    byte b = tgt.get();
    src.position(start);

    while (src.hasRemaining()) {
      if (b == src.get()) { // matching first byte
        src.mark(); // save position in loop
        tgt.mark(); // save position in target
        boolean found = true;
        int pos = src.position() - 1;
        while (tgt.hasRemaining()) {
          if (!src.hasRemaining()) { // src expired first
            tgt.reset();
            src.reset();
            found = false;
            break;
          }
          if (!(tgt.get() == src.get())) {
            tgt.reset();
            src.reset();
            found = false;
            break; // no match
          }
        }
        if (found) {
          return pos;
        }
      }
    }
    return -1; // not found
  }
}
