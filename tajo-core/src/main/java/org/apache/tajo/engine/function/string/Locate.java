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

import org.apache.commons.lang.StringUtils;
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
 * Function definition.
 * 
 * INT4 locate(string TEXT, substr TEXT, [, pos INT4])
 */
@Description(
  functionName = "locate",
  description = "Location of specified substring",
  example = "> SELECT locate('high', 'ig')\n"
          + "2",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4})
  }
)
public class Locate extends GeneralFunction {
  public Locate() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("substr", TajoDataTypes.Type.TEXT),
        new Column("pos", TajoDataTypes.Type.INT4)
    });
  }
  
  /**
   * Returns the position of the first occurance of substr in string after position pos (using one-based index).
   * 
   * if substr is empty string, it always matches except 
   * pos is greater than string length + 1.(mysql locate() function spec.)
   * At any not matched case, it returns 0.
   */
  private int locate(String str, String substr, int pos) {
    if (substr.length() == 0) {
      if (pos <= (str.length() + 1)) {
        return pos;
      }
      else {
        return 0;
      }
    }
    int idx = StringUtils.indexOf(str, substr, pos - 1);
    if (idx == -1) {
      return 0;
    }
    return idx + 1;
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    int pos = 1;  // one-based index
    if (params.size() > 2) {
      pos = params.getInt4(2);
      if (pos < 0) {
        return DatumFactory.createInt4(0);  // negative value is not acceptable.
      }
      if (pos == 0) {
        pos = 1;  // one-based index
      }
    }
    
    String str = params.getText(0);
    String substr = params.getText(1);
    
    return DatumFactory.createInt4(locate(str, substr, pos));
  }
}
