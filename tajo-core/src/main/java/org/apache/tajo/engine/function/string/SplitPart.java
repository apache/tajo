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
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text split_part(string text, delimiter text, field int)
 */
@Description(
  functionName = "split_part",
  description = "Split string on delimiter and return the given field",
  example = "> SELECT split_part('abc~@~def~@~ghi', '~@~', 2);\n"
          + "def",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,
          TajoDataTypes.Type.TEXT,TajoDataTypes.Type.INT4})}
)
public class SplitPart extends GeneralFunction {
  public SplitPart() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("delimiter", TajoDataTypes.Type.TEXT),
        new Column("field", TajoDataTypes.Type.INT4),
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(2)) {
      return NullDatum.get();
    }

    String [] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(params.getText(0), params.getText(1), -1);
    int idx = params.getInt4(2) - 1;
    if (split.length > idx) {
      return DatumFactory.createText(split[idx]);
    } else {
      // If part is larger than the number of string portions, it will returns NULL.
      return NullDatum.get();
    }
  }
}
