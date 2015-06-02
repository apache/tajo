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
 * find_in_set(text,str_array) - Returns the first occurrence of str in str_array where str_array
 * is a comma-delimited string.
 *
 * Returns null if either argument is null.
 * Returns 0 if the first argument has any commas.
 *
 * Example:
 * SELECT find_in_set('cr','crt,c,cr,c,def') FROM src LIMIT 1;\n"
 * -> result: 3
 */
@Description(
  functionName = "find_in_set",
  description = "Returns the first occurrence of str in str_array where str_array is a comma-delimited string",
  detail = "Returns null if either argument is null.\n"
      + "Returns 0 if the first argument has any commas.",
  example = "> SELECT find_in_set('cr','crt,c,cr,c,def');\n"
          + "3",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class FindInSet extends GeneralFunction {
  public FindInSet() {
    super(new Column[]{
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("str_array", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    byte[] searchBytes = params.getBytes(0);

    //  Returns 0 if the first argument has any commas.
    for (byte searchByte : searchBytes) {
      if (searchByte == ',') {
        return DatumFactory.createInt4(0);
      }
    }

    byte[] arrayData = params.getBytes(1);
    int findingLength = searchBytes.length;

    int posInTextArray = 0;
    int curLengthOfCandidate = 0;
    boolean matching = true;

    for (byte abyte : arrayData) {

      if (abyte == ',') {
        posInTextArray++;
        if (matching && curLengthOfCandidate == findingLength) {
          return DatumFactory.createInt4(posInTextArray);
        } else {
          matching = true;
          curLengthOfCandidate = 0;
        }
      } else {
        if (curLengthOfCandidate + 1 <= findingLength) {
          if (!matching || searchBytes[curLengthOfCandidate] != abyte) {
            matching = false;
          }
        } else {
          matching = false;
        }
        curLengthOfCandidate++;
      }

    }

    if (matching && curLengthOfCandidate == findingLength) {
      posInTextArray++;
      return DatumFactory.createInt4(posInTextArray);
    } else {
      return DatumFactory.createInt4(0);
    }

  }
}
